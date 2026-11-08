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

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.testing.assertions.Assert.assertEqualsIgnoreOrder;
import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestMergeJoinAggregationPushdown
{
    private final QueryAssertions assertions = new QueryAssertions();

    @AfterAll
    public void close()
    {
        assertions.close();
    }

    @Test
    public void testPostSelectionAggregationOrdering()
    {
        check(false);
    }

    @Test
    public void testProjectionPreservesSpillability()
    {
        check(true);
    }

    private void check(boolean spilling)
    {
        Session forced = Session.builder(TEST_SESSION)
                .setSystemProperty("prefer_merge_join", "FORCE")
                .setSystemProperty("join_distribution_type", "PARTITIONED")
                .setSystemProperty("join_reordering_strategy", "NONE")
                .setSystemProperty("spill_enabled", Boolean.toString(spilling))
                .setSystemProperty("push_partial_aggregation_through_join", "true")
                .build();
        Session hash = Session.builder(forced).setSystemProperty("prefer_merge_join", "OFF").build();
        String fact = IntStream.range(0, 640).mapToObj(i -> "(" + i % 16 + ", BIGINT '" + i + "')").collect(Collectors.joining(","));
        String dimension = IntStream.range(0, 16).mapToObj(i -> "(" + i + "," + i % 2 + ")").collect(Collectors.joining(","));
        for (String value : new String[] {"l.v", "l.v + 1"}) {
            String sql = "SELECT r.k, r.g, sum(" + value + ") FROM (VALUES " + fact + ") l(k,v) JOIN (VALUES " + dimension + ") r(k,g) ON l.k=r.k GROUP BY r.k, r.g";
            String plan = (String) assertions.execute(forced, "EXPLAIN " + sql).getOnlyValue();
            var expected = assertions.execute(hash, sql).getMaterializedRows();
            Session noPushdown = Session.builder(forced).setSystemProperty("push_partial_aggregation_through_join", "false").build();
            var control = assertions.execute(noPushdown, sql).getMaterializedRows();
            assertEqualsIgnoreOrder(control, expected);
            assertEqualsIgnoreOrder(assertions.execute(forced, sql).getMaterializedRows(), expected);
            assertThat(plan).contains("Algorithm: MERGE");
        }
    }
}
