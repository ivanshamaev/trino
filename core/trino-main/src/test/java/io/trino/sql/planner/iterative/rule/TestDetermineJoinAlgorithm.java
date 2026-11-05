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
/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License");
 * you may obtain a copy of the License at
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
import io.trino.spi.type.DoubleType;
import io.trino.sql.ir.ComparisonOperator;
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
