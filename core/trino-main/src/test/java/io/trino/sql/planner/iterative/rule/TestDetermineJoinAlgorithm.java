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
package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import io.trino.cost.CostComparator;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.cost.SymbolStatsEstimate;
import io.trino.cost.TaskCountEstimator;
import io.trino.spi.type.DoubleType;
import io.trino.sql.ir.ComparisonOperator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.assertions.MatchContext;
import io.trino.sql.planner.assertions.MatchResult;
import io.trino.sql.planner.assertions.Matcher;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.iterative.rule.test.RuleBuilder;
import io.trino.sql.planner.iterative.rule.test.RuleTester;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.JoinNode.EquiJoinClause;
import io.trino.sql.planner.plan.JoinNode.JoinAlgorithm;
import io.trino.sql.planner.plan.JoinType;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.UnionNode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.SystemSessionProperties.PREFER_MERGE_JOIN;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.ir.TestingIr.comparison;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aliasToIndex;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.JoinNode.DistributionType.PARTITIONED;
import static io.trino.sql.planner.plan.JoinNode.DistributionType.REPLICATED;
import static io.trino.sql.planner.plan.JoinType.FULL;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.sql.planner.plan.JoinType.LEFT;
import static io.trino.sql.planner.plan.JoinType.RIGHT;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestDetermineJoinAlgorithm
{
    private RuleTester tester;

    @BeforeAll
    public void setUp()
    {
        tester = RuleTester.builder().build();
    }

    @AfterAll
    public void tearDown()
    {
        tester.close();
        tester = null;
    }

    @Test
    public void testCostBasedKeepsHashForUnknownStatistics()
    {
        assertRule()
                .setSystemProperty(PREFER_MERGE_JOIN, "COST_BASED")
                .setSystemProperty("spill_enabled", "true")
                .on(p -> p.join(
                        INNER,
                        PARTITIONED,
                        p.values(p.symbol("L1", BIGINT)),
                        p.values(p.symbol("R1", BIGINT)),
                        equi(p)))
                .doesNotFire();
    }

    @Test
    public void testCostBasedSelectsBoundedFullJoinUnderMemoryPressure()
    {
        costBasedRule(1)
                .setSystemProperty("spill_enabled", "true")
                .setSystemProperty("query_max_memory_per_node", "1MB")
                .on(p -> p.join(
                        FULL,
                        PARTITIONED,
                        p.values(new PlanNodeId("probe"), p.symbol("L1", BIGINT)),
                        p.values(new PlanNodeId("build"), p.symbol("R1", BIGINT)),
                        equi(p)))
                .matches(mergeJoinAlgorithm());
    }

    @Test
    public void testCostBasedAccountsForWorkerCount()
    {
        costBasedRule(100)
                .setSystemProperty("spill_enabled", "true")
                .setSystemProperty("query_max_memory_per_node", "10MB")
                .on(p -> p.join(
                        FULL,
                        PARTITIONED,
                        p.values(new PlanNodeId("probe"), p.symbol("L1", BIGINT)),
                        p.values(new PlanNodeId("build"), p.symbol("R1", BIGINT)),
                        equi(p)))
                .doesNotFire();
    }

    @Test
    public void testCostBasedRequiresSpillForBoundedMemory()
    {
        costBasedRule(1)
                .setSystemProperty("spill_enabled", "false")
                .setSystemProperty("query_max_memory_per_node", "1MB")
                .on(p -> p.join(
                        FULL,
                        PARTITIONED,
                        p.values(new PlanNodeId("probe"), p.symbol("L1", BIGINT)),
                        p.values(new PlanNodeId("build"), p.symbol("R1", BIGINT)),
                        equi(p)))
                .doesNotFire();
    }

    @Test
    public void testCostBasedKeepsHashWhenUnsortedBuildFits()
    {
        costBasedRule(1)
                .setSystemProperty("spill_enabled", "true")
                .setSystemProperty("query_max_memory_per_node", "1GB")
                .on(p -> p.join(
                        INNER,
                        PARTITIONED,
                        p.values(new PlanNodeId("probe"), p.symbol("L1", BIGINT)),
                        p.values(new PlanNodeId("build"), p.symbol("R1", BIGINT)),
                        equi(p)))
                .doesNotFire();
    }

    @Test
    public void testCostBasedKeepsHashWithoutKeyCardinality()
    {
        costBasedRule(1)
                .setSystemProperty("spill_enabled", "true")
                .setSystemProperty("query_max_memory_per_node", "1MB")
                .overrideStats("build", PlanNodeStatsEstimate.builder().setOutputRowCount(1_000_000).build())
                .on(p -> p.join(
                        FULL,
                        PARTITIONED,
                        p.values(new PlanNodeId("probe"), p.symbol("L1", BIGINT)),
                        p.values(new PlanNodeId("build"), p.symbol("R1", BIGINT)),
                        equi(p)))
                .doesNotFire();
    }

    @Test
    public void testCostBasedHandlesUnionBeforeExchanges()
    {
        costBasedRule(1)
                .setSystemProperty("spill_enabled", "true")
                .setSystemProperty("query_max_memory_per_node", "1GB")
                .on(p -> p.join(
                        INNER,
                        PARTITIONED,
                        new UnionNode(
                                new PlanNodeId("probe"),
                                ImmutableList.of(p.values(p.symbol("branch_a", BIGINT)), p.values(p.symbol("branch_b", BIGINT))),
                                ImmutableListMultimap.of(p.symbol("L1", BIGINT), p.symbol("branch_a", BIGINT), p.symbol("L1", BIGINT), p.symbol("branch_b", BIGINT)),
                                ImmutableList.of(p.symbol("L1", BIGINT))),
                        p.values(new PlanNodeId("build"), p.symbol("R1", BIGINT)),
                        equi(p)))
                .doesNotFire();
    }

    @Test
    public void testInvalidCostMargin()
    {
        for (String margin : ImmutableList.of("-0.1", "1", "NaN", "Infinity")) {
            assertThatThrownBy(() -> costBasedRule(1)
                    .setSystemProperty("spill_enabled", "true")
                    .setSystemProperty("query_max_memory_per_node", "1MB")
                    .setSystemProperty("merge_join_cost_margin", margin)
                    .on(p -> p.join(
                            FULL,
                            PARTITIONED,
                            p.values(new PlanNodeId("probe"), p.symbol("L1", BIGINT)),
                            p.values(new PlanNodeId("build"), p.symbol("R1", BIGINT)),
                            equi(p)))
                    .matches(mergeJoinAlgorithm()))
                    .hasMessageContaining("merge_join_cost_margin");
        }
    }

    private RuleBuilder costBasedRule(int workers)
    {
        return tester.assertThat(new DetermineJoinAlgorithm(tester.getPlannerContext(), new CostComparator(75, 10, 15), new TaskCountEstimator(() -> workers)))
                .setSystemProperty(PREFER_MERGE_JOIN, "COST_BASED")
                .overrideStats("probe", statistics("L1"))
                .overrideStats("build", statistics("R1"));
    }

    private static PlanNodeStatsEstimate statistics(String name)
    {
        return PlanNodeStatsEstimate.builder().setOutputRowCount(1_000_000)
                .addSymbolStatistics(new Symbol(BIGINT, name), SymbolStatsEstimate.builder().setDistinctValuesCount(1_000_000).setNullsFraction(0).build())
                .build();
    }

    @Test
    public void testForceFiresRegardlessOfCost()
    {
        // FORCE fires for any eligible join regardless of stats
        assertRule()
                .setSystemProperty(PREFER_MERGE_JOIN, "FORCE")
                .on(p -> p.join(
                        INNER,
                        PARTITIONED,
                        p.values(p.symbol("L1", BIGINT)),
                        p.values(p.symbol("R1", BIGINT)),
                        equi(p)))
                .matches(mergeJoinWithNotNullFilters());
    }

    @Test
    public void testDoesNotFireWhenDisabled()
    {
        assertRule()
                .setSystemProperty(PREFER_MERGE_JOIN, "OFF")
                .on(p -> p.join(
                        INNER,
                        PARTITIONED,
                        p.values(p.symbol("L1", BIGINT)),
                        p.values(p.symbol("R1", BIGINT)),
                        equi(p)))
                .doesNotFire();
    }

    @Test
    public void testSelectsMergeForEligibleInnerJoin()
    {
        assertRule()
                .setSystemProperty(PREFER_MERGE_JOIN, "PREFER")
                .on(p -> p.join(
                        INNER,
                        PARTITIONED,
                        p.values(p.symbol("L1", BIGINT)),
                        p.values(p.symbol("R1", BIGINT)),
                        equi(p)))
                .matches(mergeJoinWithNotNullFilters());
    }

    @Test
    public void testSelectsMergeForEligibleLeftJoin()
    {
        assertRule()
                .setSystemProperty(PREFER_MERGE_JOIN, "FORCE")
                .on(p -> p.join(
                        LEFT,
                        PARTITIONED,
                        p.values(p.symbol("L1", BIGINT)),
                        p.values(p.symbol("R1", BIGINT)),
                        equi(p)))
                .matches(mergeJoinAlgorithm());
    }

    @Test
    public void testSelectsMergeForEligibleRightOrFullJoin()
    {
        for (JoinType type : ImmutableList.of(RIGHT, FULL)) {
            assertRule()
                    .setSystemProperty(PREFER_MERGE_JOIN, "FORCE")
                    .on(p -> p.join(
                            type,
                            PARTITIONED,
                            p.values(p.symbol("L1", BIGINT)),
                            p.values(p.symbol("R1", BIGINT)),
                            equi(p)))
                    .matches(mergeJoinAlgorithm());
        }
    }

    @Test
    public void testDoesNotFireForReplicatedDistribution()
    {
        assertRule()
                .setSystemProperty(PREFER_MERGE_JOIN, "FORCE")
                .on(p -> p.join(
                        INNER,
                        REPLICATED,
                        p.values(p.symbol("L1", BIGINT)),
                        p.values(p.symbol("R1", BIGINT)),
                        equi(p)))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForDoubleJoinKey()
    {
        // DOUBLE keys are excluded until NaN handling is validated (compare(NaN, NaN) == 0 while NaN = NaN is false)
        assertRule()
                .setSystemProperty(PREFER_MERGE_JOIN, "FORCE")
                .on(p -> p.join(
                        INNER,
                        PARTITIONED,
                        p.values(p.symbol("L1", DoubleType.DOUBLE)),
                        p.values(p.symbol("R1", DoubleType.DOUBLE)),
                        new EquiJoinClause(p.symbol("L1", DoubleType.DOUBLE), p.symbol("R1", DoubleType.DOUBLE))))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForNonEquiJoin()
    {
        assertRule()
                .setSystemProperty(PREFER_MERGE_JOIN, "FORCE")
                .on(p -> p.join(
                        INNER,
                        p.values(p.symbol("L1", BIGINT)),
                        p.values(p.symbol("R1", BIGINT)),
                        comparison(ComparisonOperator.LESS_THAN, p.symbol("L1", BIGINT).toSymbolReference(), p.symbol("R1", BIGINT).toSymbolReference())))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenAlgorithmAlreadySet()
    {
        assertRule()
                .setSystemProperty(PREFER_MERGE_JOIN, "FORCE")
                .on(p -> p.join(
                        INNER,
                        PARTITIONED,
                        p.values(p.symbol("L1", BIGINT)),
                        p.values(p.symbol("R1", BIGINT)),
                        equi(p)).withAlgorithm(JoinAlgorithm.MERGE))
                .doesNotFire();
    }

    private RuleBuilder assertRule()
    {
        return tester.assertThat(new DetermineJoinAlgorithm(tester.getPlannerContext()));
    }

    private static EquiJoinClause equi(PlanBuilder p)
    {
        return new EquiJoinClause(p.symbol("L1", BIGINT), p.symbol("R1", BIGINT));
    }

    private static PlanMatchPattern mergeJoinAlgorithm()
    {
        return node(JoinNode.class,
                values(aliasToIndex(ImmutableList.of("L1"))),
                values(aliasToIndex(ImmutableList.of("R1"))))
                .with(new Matcher()
                {
                    @Override
                    public boolean shapeMatches(PlanNode planNode)
                    {
                        return planNode instanceof JoinNode joinNode && joinNode.getAlgorithm().orElse(null) == JoinAlgorithm.MERGE;
                    }

                    @Override
                    public MatchResult detailMatches(PlanNode planNode, MatchContext context)
                    {
                        return MatchResult.match();
                    }
                });
    }

    private static PlanMatchPattern mergeJoinWithNotNullFilters()
    {
        return node(JoinNode.class,
                node(FilterNode.class, values(aliasToIndex(ImmutableList.of("L1")))),
                node(FilterNode.class, values(aliasToIndex(ImmutableList.of("R1")))))
                .with(new Matcher()
                {
                    @Override
                    public boolean shapeMatches(PlanNode planNode)
                    {
                        return planNode instanceof JoinNode joinNode && joinNode.getAlgorithm().orElse(null) == JoinAlgorithm.MERGE;
                    }

                    @Override
                    public MatchResult detailMatches(PlanNode planNode, MatchContext context)
                    {
                        return MatchResult.match();
                    }
                });
    }
}
