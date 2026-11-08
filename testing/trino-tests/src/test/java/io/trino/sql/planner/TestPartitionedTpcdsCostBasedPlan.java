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

package io.trino.sql.planner;

import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.tpcds.Table;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * This class tests cost-based optimization rules. It contains unmodified TPC-DS queries.
 * This class is using Iceberg connector un-partitioned TPC-DS tables.
 */
public class TestPartitionedTpcdsCostBasedPlan
        extends BaseCostBasedPlanTest
{
    private static final Logger LOG = Logger.get(TestPartitionedTpcdsCostBasedPlan.class);
    private static final Set<String> MEMORY_PRESSURE_QUERIES = Set.of("q04.sql", "q11.sql", "q74.sql");

    protected TestPartitionedTpcdsCostBasedPlan()
    {
        super("tpcds_sf1000_parquet_part", true);
    }

    @Override
    protected List<String> getTableNames()
    {
        return Table.getBaseTables().stream()
                .filter(table -> table != Table.DBGEN_VERSION)
                .map(Table::getName)
                .collect(toImmutableList());
    }

    @Override
    protected String getTableResourceDirectory()
    {
        return "iceberg/tpcds/sf1000/partitioned/";
    }

    @Override
    protected String getTableTargetDirectory()
    {
        return "iceberg-tpcds-sf1000-parquet-part/";
    }

    @Override
    protected List<String> getQueryResourcePaths()
    {
        return TPCDS_SQL_FILES;
    }

    @ParameterizedTest
    @MethodSource("getQueryResourcePaths")
    public void testCostBasedMergeTpcdsPlans(String queryResourcePath)
    {
        Session hash = Session.builder(getPlanTester().getDefaultSession())
                .setSystemProperty("prefer_merge_join", "OFF")
                .setSystemProperty("spill_enabled", "true")
                // Pin the planning budget instead of deriving it from the test JVM heap.
                .setSystemProperty("query_max_memory_per_node", "16GB")
                .build();
        Session costBased = Session.builder(hash).setSystemProperty("prefer_merge_join", "COST_BASED").build();
        String query = readQuery(queryResourcePath);
        String actual = generateQueryPlan(query, costBased, plan -> {
            for (var node : searchFrom(plan.getRoot()).where(node -> node instanceof JoinNode join && join.getAlgorithm().isPresent()).findAll()) {
                JoinNode join = (JoinNode) node;
                PlanNodeStatsEstimate build = plan.getStatsAndCosts().getStats().getOrDefault(join.getRight().getId(), PlanNodeStatsEstimate.unknown());
                LOG.info("TPCDS merge choice query=%s buildRows=%s buildBytes=%s buildNDV=%s",
                        queryResourcePath,
                        build.getOutputRowCount(),
                        build.getOutputSizeInBytes(join.getRight().getOutputSymbols()),
                        join.getCriteria().stream().map(clause -> build.getSymbolStatistics(clause.getRight()).getDistinctValuesCount()).toList());
            }
        });
        if (MEMORY_PRESSURE_QUERIES.contains(Path.of(queryResourcePath).getFileName().toString())) {
            assertThat(actual).isEqualTo(read(getQueryPlanResourcePath(queryResourcePath).replace(".plan.txt", ".merge.plan.txt")));
        }
        else {
            assertThat(actual).isEqualTo(generateQueryPlan(query, hash));
        }
    }

    static void main()
    {
        new TestPartitionedTpcdsCostBasedPlan().generate();
    }
}
