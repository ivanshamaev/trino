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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;

import static io.trino.testing.assertions.Assert.assertEqualsIgnoreOrder;
import static org.assertj.core.api.Assertions.assertThat;

public class TestMergeJoinAggregationExchange
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder().setWorkerCount(2).setInitialTables(ImmutableList.of()).build();
    }

    @Test
    public void testOrderedExchangeAfterAggregation()
    {
        try (TestTable fact = new TestTable(
                getQueryRunner()::execute,
                "audit_exchange_fact",
                "WITH (sorted_by = ARRAY['k']) AS SELECT CAST(n % 32 AS BIGINT) k, CAST(n % 7 AS BIGINT) v FROM UNNEST(sequence(1, 6400)) t(n)");
                TestTable dimension = new TestTable(
                        getQueryRunner()::execute,
                        "audit_exchange_dim",
                        "WITH (sorted_by = ARRAY['k']) AS SELECT CAST(n AS BIGINT) k, CAST(n % 3 AS BIGINT) g, CAST(n % 2 AS BIGINT) cutoff FROM UNNEST(sequence(0, 31)) t(n)")) {
            assertUpdate("ANALYZE " + fact.getName());
            assertUpdate("ANALYZE " + dimension.getName());
            Session forced = Session.builder(getSession())
                    .setSystemProperty("prefer_merge_join", "FORCE")
                    .setSystemProperty("join_distribution_type", "PARTITIONED")
                    .setSystemProperty("join_reordering_strategy", "NONE")
                    .setSystemProperty("push_partial_aggregation_through_join", "true").build();
            Session hash = Session.builder(forced).setSystemProperty("prefer_merge_join", "OFF").build();
            Session noPushdown = Session.builder(forced).setSystemProperty("push_partial_aggregation_through_join", "false").build();
            String sql = "SELECT l.k, l.g, l.cutoff, sum(r.v) FROM " + dimension.getName() + " l JOIN " + fact.getName()
                    + " r ON l.k=r.k AND l.cutoff < r.v + 1 GROUP BY l.k, l.g, l.cutoff";
            var expected = computeActual(hash, sql).getMaterializedRows();
            assertEqualsIgnoreOrder(computeActual(noPushdown, sql).getMaterializedRows(), expected);
            assertThat((String) computeActual(forced, "EXPLAIN " + sql).getOnlyValue()).contains("Algorithm: MERGE", "RemoteMerge");
            for (int run = 0; run < 5; run++) {
                var actual = computeActual(forced, sql).getMaterializedRows();
                assertEqualsIgnoreOrder(actual, expected);
            }
        }
    }
}
