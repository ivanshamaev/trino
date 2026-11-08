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
package io.trino.sql.planner.sanity;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.execution.warnings.WarningCollector;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.SortNode;
import org.junit.jupiter.api.Test;

import java.util.function.Function;

import static io.trino.spi.connector.SortOrder.ASC_NULLS_LAST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static io.trino.sql.planner.plan.JoinNode.DistributionType.PARTITIONED;
import static io.trino.sql.planner.plan.JoinNode.JoinAlgorithm.MERGE;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestValidateJoinAlgorithms
        extends BasePlanTest
{
    private final PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();

    @Test
    public void testSortedSingleStreamInputs()
    {
        validate(p -> merge(p, true));
    }

    @Test
    public void testUnsortedInputRejected()
    {
        assertThatThrownBy(() -> validate(p -> merge(p, false)))
                .hasMessageContaining("MERGE input is not sorted on canonical join keys");
    }

    @Test
    public void testWrongNullOrderingRejected()
    {
        assertThatThrownBy(() -> validate(p -> {
            JoinNode join = merge(p, true);
            return join.replaceChildren(ImmutableList.of(p.sort(ImmutableList.of(p.symbol("left", BIGINT)), join.getLeft()), join.getRight()));
        })).hasMessageContaining("MERGE input is not sorted on canonical join keys");
    }

    @Test
    public void testUnsupportedKeyRejected()
    {
        assertThatThrownBy(() -> validate(p -> p.join(
                INNER,
                PARTITIONED,
                p.values(p.symbol("left", DOUBLE)),
                p.values(p.symbol("right", DOUBLE)),
                new JoinNode.EquiJoinClause(p.symbol("left", DOUBLE), p.symbol("right", DOUBLE))).withAlgorithm(MERGE)))
                .hasMessageContaining("Ineligible MERGE join");
    }

    @Test
    public void testRetryRejected()
    {
        Session session = Session.builder(getPlanTester().getDefaultSession()).setSystemProperty("retry_policy", "TASK").build();
        assertThatThrownBy(() -> validate(session, p -> merge(p, true))).hasMessageContaining("Ineligible MERGE join");
    }

    @Test
    public void testHashSpillabilityMustBeSet()
    {
        Session session = Session.builder(getPlanTester().getDefaultSession()).setSystemProperty("spill_enabled", "true").build();
        assertThatThrownBy(() -> validate(session, p -> p.join(
                INNER,
                PARTITIONED,
                p.values(p.symbol("left", BIGINT)),
                p.values(p.symbol("right", BIGINT)),
                new JoinNode.EquiJoinClause(p.symbol("left", BIGINT), p.symbol("right", BIGINT)))))
                .hasMessageContaining("Hash join spillability not set after physical planning");
    }

    @Test
    public void testMultipleInputStreamsRejected()
    {
        assertThatThrownBy(() -> validate(p -> {
            JoinNode join = merge(p, true);
            Symbol key = p.symbol("left", BIGINT);
            return join.replaceChildren(ImmutableList.of(
                    p.exchange(e -> e.scope(LOCAL).type(REPARTITION).addSource(join.getLeft()).addInputsSet(key)
                            .fixedHashDistributionPartitioningScheme(ImmutableList.of(key), ImmutableList.of(key))),
                    join.getRight()));
        })).hasMessageContaining("MERGE input requires a single stream");
    }

    @Test
    public void testOrderedExchangeCannotHideUnsortedSource()
    {
        assertThatThrownBy(() -> validate(p -> {
            JoinNode join = merge(p, true);
            Symbol key = p.symbol("left", BIGINT);
            return join.replaceChildren(ImmutableList.of(
                    p.exchange(e -> e.type(REPARTITION).addSource(p.values(key)).addInputsSet(key)
                            .fixedHashDistributionPartitioningScheme(ImmutableList.of(key), ImmutableList.of(key))
                            .orderingScheme(new OrderingScheme(ImmutableList.of(key), ImmutableMap.of(key, ASC_NULLS_LAST)))),
                    join.getRight()));
        })).hasMessageContaining("Ordered exchange source is not sorted");
    }

    @Test
    public void testIncompatibleNodePartitioningRejected()
    {
        assertThatThrownBy(() -> validate(p -> {
            JoinNode join = merge(p, true);
            Symbol key = p.symbol("left", BIGINT);
            Symbol other = p.symbol("other", BIGINT);
            PlanNode source = new SortNode(
                    idAllocator.getNextId(),
                    p.values(key, other),
                    new OrderingScheme(ImmutableList.of(key), ImmutableMap.of(key, ASC_NULLS_LAST)),
                    false);
            return join.replaceChildren(ImmutableList.of(
                    p.exchange(e -> e.type(REPARTITION).addSource(source).addInputsSet(key, other)
                            .fixedHashDistributionPartitioningScheme(ImmutableList.of(key, other), ImmutableList.of(other))
                            .orderingScheme(new OrderingScheme(ImmutableList.of(key), ImmutableMap.of(key, ASC_NULLS_LAST)))),
                    join.getRight()));
        })).hasMessageContaining("MERGE inputs have incompatible partitioning");
    }

    private JoinNode merge(PlanBuilder p, boolean sorted)
    {
        Symbol left = p.symbol("left", BIGINT);
        Symbol right = p.symbol("right", BIGINT);
        PlanNode probe = p.values(left);
        PlanNode build = p.values(right);
        if (sorted) {
            probe = new SortNode(idAllocator.getNextId(), probe, new OrderingScheme(ImmutableList.of(left), ImmutableMap.of(left, ASC_NULLS_LAST)), false);
            build = new SortNode(idAllocator.getNextId(), build, new OrderingScheme(ImmutableList.of(right), ImmutableMap.of(right, ASC_NULLS_LAST)), false);
        }
        return p.join(INNER, PARTITIONED, probe, build, new JoinNode.EquiJoinClause(left, right)).withAlgorithm(MERGE);
    }

    private void validate(Function<PlanBuilder, PlanNode> provider)
    {
        validate(getPlanTester().getDefaultSession(), provider);
    }

    private void validate(Session session, Function<PlanBuilder, PlanNode> provider)
    {
        getPlanTester().inTransaction(session, transaction -> {
            var context = getPlanTester().getPlannerContext();
            PlanBuilder builder = new PlanBuilder(idAllocator, context, transaction);
            new ValidateJoinAlgorithms().validate(provider.apply(builder), transaction, context, WarningCollector.NOOP);
            return null;
        });
    }
}
