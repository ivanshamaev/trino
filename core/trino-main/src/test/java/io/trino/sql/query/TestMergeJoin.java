/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.sql.query;

import io.trino.Session;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.PREFER_MERGE_JOIN;
import static io.trino.testing.assertions.Assert.assertEqualsIgnoreOrder;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end tests for the forced merge join: the planner marks eligible INNER/LEFT
 * equi-joins with {@code prefer_merge_join=FORCE}, adds canonical sorts on both
 * sides, and the local execution planner builds the merge join pipelines. Results
 * must be identical to the standard (hash) evaluation of the same query.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.CONCURRENT)
public class TestMergeJoin
{
    private final QueryAssertions assertions = new QueryAssertions();

    private final Session mergeJoinSession = Session.builder(TEST_SESSION)
            .setSystemProperty(PREFER_MERGE_JOIN, "FORCE")
            // small test queries would otherwise choose a REPLICATED (broadcast) distribution,
            // for which the merge join is not eligible
            .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "PARTITIONED")
            .build();

    @AfterAll
    public void teardown()
    {
        assertions.close();
    }

    @Test
    public void testInnerJoinWithSpillEnabled()
    {
        Session spilling = Session.builder(mergeJoinSession).setSystemProperty("spill_enabled", "true").build();
        assertThat(assertions.query(spilling, "SELECT l.k, r.v FROM (VALUES 1, 2, 2) l(k) JOIN (VALUES (1, 10), (2, 20)) r(k, v) ON l.k = r.k"))
                .matches("VALUES (1, 10), (2, 20), (2, 20)");
    }

    @Test
    public void testInnerJoin()
    {
        assertThat(assertions.query(mergeJoinSession, "SELECT l.k, l.a, r.k, r.b FROM (VALUES (1, 10), (2, 20), (2, 21), (4, 40), (NULL, 50)) l(k, a) JOIN (VALUES (1, 100), (2, 200), (3, 300), (NULL, 400)) r(k, b) ON l.k = r.k"))
                .matches("VALUES (1, 10, 1, 100), (2, 20, 2, 200), (2, 21, 2, 200)");
    }

    @Test
    public void testLeftJoin()
    {
        assertThat(assertions.query(mergeJoinSession, "SELECT l.k, l.a, r.b FROM (VALUES (1, 10), (2, 20), (3, 30), (NULL, 40)) l(k, a) LEFT JOIN (VALUES (1, 100), (3, 300)) r(k, b) ON l.k = r.k"))
                .matches("VALUES (1, 10, 100), (2, 20, CAST(NULL AS integer)), (3, 30, 300), (NULL, 40, CAST(NULL AS integer))");
    }

    @Test
    public void testVarcharKey()
    {
        assertThat(assertions.query(mergeJoinSession, "SELECT l.k, r.b FROM (VALUES ('a', 1), ('c', 2), (NULL, 3)) l(k, a) JOIN (VALUES ('a', 10), ('b', 20), ('c', 30)) r(k, b) ON l.k = r.k"))
                .matches("VALUES ('a', 10), ('c', 30)");
    }

    @Test
    public void testJoinWithResidualFilter()
    {
        assertThat(assertions.query(mergeJoinSession, "SELECT l.k, r.b FROM (VALUES (1, 1), (2, 2)) l(k, a) JOIN (VALUES (1, 10), (1, 11), (2, 20)) r(k, b) ON l.k = r.k AND r.b > 10"))
                .matches("VALUES (1, 11), (2, 20)");
    }

    @Test
    public void testJoinWithDuplicateKeys()
    {
        // many-to-many: 3 x 2 matches on key 7
        assertThat(assertions.query(mergeJoinSession,
                "SELECT l.k, r.b FROM "
                        + "(VALUES (7, 'a'), (7, 'b'), (7, 'c'), (8, 'd')) l(k, s) "
                        + "JOIN (VALUES (7, 1), (7, 2)) r(k, b) ON l.k = r.k"))
                .matches("VALUES (7, 1), (7, 1), (7, 1), (7, 2), (7, 2), (7, 2)");
    }

    @Test
    public void testExplainShowsMergeJoinAlgorithm()
    {
        String explain = assertions.execute(mergeJoinSession, "EXPLAIN SELECT l.k, r.b FROM (VALUES (1, 10), (2, 20), (2, 21), (4, 40), (NULL, 50)) l(k, a) JOIN (VALUES (1, 100), (2, 200), (3, 300), (NULL, 400)) r(k, b) ON l.k = r.k")
                .getOnlyColumnAsSet()
                .toString();
        assertThat(explain).contains("Algorithm: MERGE");
    }

    @Test
    public void testOffByDefaultProducesSameResults()
    {
        // same query with merge join disabled must return identical results
        String query = "SELECT l.k, r.b FROM (VALUES (1, 10), (2, 20)) l(k, a) JOIN (VALUES (1, 100), (2, 200)) r(k, b) ON l.k = r.k";
        assertThat(assertions.query(query))
                .matches("VALUES (1, 100), (2, 200)");
        assertThat(assertions.query(mergeJoinSession, query))
                .matches("VALUES (1, 100), (2, 200)");
    }

    @Test
    public void testRowKeyWithNestedNullUsesSqlEquality()
    {
        String query = "SELECT count(*) FROM (SELECT ROW(1, CAST(NULL AS integer)) AS k) l "
                + "JOIN (SELECT ROW(1, CAST(NULL AS integer)) AS k) r ON l.k = r.k";
        assertThat(assertions.query(mergeJoinSession, query))
                .matches("VALUES BIGINT '0'");
        assertThat(assertions.query(query))
                .matches("VALUES BIGINT '0'");
    }

    @Test
    public void testArrayKeyWithNestedNullUsesSqlEquality()
    {
        String query = "SELECT count(*) FROM (SELECT ARRAY[CAST(NULL AS integer)] AS k) l "
                + "JOIN (SELECT ARRAY[CAST(NULL AS integer)] AS k) r ON l.k = r.k";
        assertThat(assertions.query(mergeJoinSession, query))
                .matches("VALUES BIGINT '0'");
        assertThat(assertions.query(query))
                .matches("VALUES BIGINT '0'");
    }

    @Test
    public void testLargeFullJoinMatchesHashJoin()
    {
        String query =
                """
                WITH
                  l AS (SELECT IF(n % 19 = 0, CAST(NULL AS bigint), n % 211) k, n v FROM UNNEST(sequence(1, 5000)) t(n)),
                  r AS (SELECT IF(n % 23 = 0, CAST(NULL AS bigint), n % 211) k, n v FROM UNNEST(sequence(1, 4000)) t(n))
                SELECT l.k, l.v, r.v
                FROM l FULL JOIN r ON l.k = r.k AND r.v % 7 = 0
                """;
        Session hashJoinSession = Session.builder(mergeJoinSession)
                .setSystemProperty(PREFER_MERGE_JOIN, "OFF")
                .build();

        assertEqualsIgnoreOrder(
                assertions.execute(mergeJoinSession, query).getMaterializedRows(),
                assertions.execute(hashJoinSession, query).getMaterializedRows());
    }
}
