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
import com.google.common.collect.ImmutableMap;
import io.trino.matching.Captures;
import io.trino.sql.ir.Logical;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.ValuesNode;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static org.assertj.core.api.Assertions.assertThat;

public class TestMergeJoinExpressionRewrite
{
    @Test
    public void testPayloadProjectionPreservesAlgorithmAndEquiKeys()
    {
        Symbol left = new Symbol(BIGINT, "left");
        Symbol right = new Symbol(BIGINT, "right");
        Symbol payload = new Symbol(BIGINT, "payload");
        JoinNode join = new JoinNode(
                new PlanNodeId("join"),
                INNER,
                new ValuesNode(new PlanNodeId("left"), ImmutableList.of(left)),
                new ValuesNode(new PlanNodeId("right"), ImmutableList.of(right)),
                ImmutableList.of(new JoinNode.EquiJoinClause(left, right)),
                ImmutableList.of(left),
                ImmutableList.of(right),
                false,
                Optional.empty(),
                Optional.of(JoinNode.DistributionType.PARTITIONED),
                Optional.empty(),
                Optional.of(JoinNode.JoinAlgorithm.MERGE),
                ImmutableMap.of(),
                Optional.empty());
        ProjectNode projection = new ProjectNode(new PlanNodeId("project"), join, Assignments.of(payload, left.toSymbolReference()));
        JoinNode rewritten = (JoinNode) PushProjectionThroughJoin.pushProjectionThroughJoin(projection, Lookup.noLookup(), new PlanNodeIdAllocator()).orElseThrow();
        assertThat(rewritten.getAlgorithm()).contains(JoinNode.JoinAlgorithm.MERGE);
        assertThat(rewritten.getCriteria()).isEqualTo(join.getCriteria());
        assertThat(((ProjectNode) rewritten.getLeft()).getAssignments().get(left)).isEqualTo(left.toSymbolReference());
        assertThat(((ProjectNode) rewritten.getRight()).getAssignments().get(right)).isEqualTo(right.toSymbolReference());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testFilterRewritePreservesAlgorithm()
    {
        Symbol left = new Symbol(BIGINT, "left");
        Symbol right = new Symbol(BIGINT, "right");
        JoinNode join = new JoinNode(
                new PlanNodeId("join"),
                INNER,
                new ValuesNode(new PlanNodeId("left"), ImmutableList.of(left)),
                new ValuesNode(new PlanNodeId("right"), ImmutableList.of(right)),
                ImmutableList.of(new JoinNode.EquiJoinClause(left, right)),
                ImmutableList.of(left),
                ImmutableList.of(right),
                false,
                Optional.of(new Logical(Logical.Operator.AND, ImmutableList.of(TRUE, TRUE))),
                Optional.of(JoinNode.DistributionType.PARTITIONED),
                Optional.empty(),
                Optional.of(JoinNode.JoinAlgorithm.MERGE),
                ImmutableMap.of(),
                Optional.empty());
        Rule<JoinNode> rule = (Rule<JoinNode>) new ExpressionRewriteRuleSet((_, _) -> TRUE).joinExpressionRewrite();
        JoinNode rewritten = (JoinNode) rule.apply(join, Captures.empty(), null).transformedPlan().orElseThrow();
        assertThat(rewritten.getAlgorithm()).contains(JoinNode.JoinAlgorithm.MERGE);
    }
}
