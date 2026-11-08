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

import io.trino.Session;
import io.trino.execution.warnings.WarningCollector;
import io.trino.spi.connector.LocalProperty;
import io.trino.spi.connector.SortingProperty;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.planner.optimizations.ActualProperties;
import io.trino.sql.planner.optimizations.LocalProperties;
import io.trino.sql.planner.optimizations.PropertyDerivations;
import io.trino.sql.planner.optimizations.StreamPropertyDerivations;
import io.trino.sql.planner.optimizations.StreamPropertyDerivations.StreamProperties;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.sanity.PlanSanityChecker.Checker;

import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.SystemSessionProperties.isSpillEnabled;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_LAST;
import static io.trino.sql.planner.iterative.rule.DetermineJoinAlgorithm.isEligible;
import static io.trino.sql.planner.optimizations.StreamPropertyDerivations.StreamProperties.StreamDistribution.FIXED;
import static io.trino.sql.planner.plan.JoinNode.JoinAlgorithm.MERGE;

/**
 * Verifies physical join contracts after all plan rewrites. DynamicFiltersChecker validates filter producers/consumers.
 */
public class ValidateJoinAlgorithms
        implements Checker
{
    @Override
    public void validate(PlanNode planNode, Session session, PlannerContext plannerContext, WarningCollector warningCollector)
    {
        planNode.accept(new Visitor(session, plannerContext, new SymbolAllocator(SymbolsExtractor.extractUnique(planNode))), null);
    }

    private static final class Visitor
            extends PlanVisitor<Void, Void>
    {
        private final Session session;
        private final PlannerContext plannerContext;
        private final SymbolAllocator symbolAllocator;
        private final Map<PlanNode, StreamProperties> streams = new IdentityHashMap<>();
        private final Map<PlanNode, ActualProperties> properties = new IdentityHashMap<>();

        private Visitor(Session session, PlannerContext plannerContext, SymbolAllocator symbolAllocator)
        {
            this.session = session;
            this.plannerContext = plannerContext;
            this.symbolAllocator = symbolAllocator;
        }

        @Override
        protected Void visitPlan(PlanNode node, Void context)
        {
            node.getSources().forEach(source -> source.accept(this, context));
            return null;
        }

        @Override
        public Void visitJoin(JoinNode node, Void context)
        {
            visitPlan(node, context);
            if (node.getAlgorithm().filter(MERGE::equals).isPresent()) {
                checkArgument(isEligible(node, session), "Ineligible MERGE join: %s", node.getId());
                List<Symbol> leftKeys = node.getCriteria().stream().map(JoinNode.EquiJoinClause::getLeft).distinct().collect(toImmutableList());
                List<Symbol> rightKeys = node.getCriteria().stream().map(JoinNode.EquiJoinClause::getRight).distinct().collect(toImmutableList());
                validateMergeInput(node.getLeft(), leftKeys);
                validateMergeInput(node.getRight(), rightKeys);
                ActualProperties left = properties(node.getLeft());
                ActualProperties right = properties(node.getRight());
                checkArgument(!left.isNullsAndAnyReplicated() && !right.isNullsAndAnyReplicated(), "MERGE inputs replicate nulls or arbitrary rows");
                if (!left.isSingleNode() || !right.isSingleNode()) {
                    Map<Symbol, Set<Symbol>> mapping = leftKeys.stream().collect(toImmutableMap(
                            symbol -> symbol,
                            symbol -> node.getCriteria().stream().filter(clause -> clause.getLeft().equals(symbol)).map(JoinNode.EquiJoinClause::getRight).collect(toImmutableSet())));
                    checkArgument(left.isNodePartitionedOn(leftKeys, false) && right.isNodePartitionedOn(rightKeys, false)
                                    && left.isCompatibleTablePartitioningWith(right, symbol -> mapping.getOrDefault(symbol, Set.of()), plannerContext.getMetadata(), session),
                            "MERGE inputs have incompatible partitioning: %s",
                            node.getId());
                }
            }
            else if (isSpillEnabled(session) && !node.getCriteria().isEmpty()) {
                checkArgument(node.isSpillable().isPresent(), "Hash join spillability not set after physical planning: %s", node.getId());
                if (node.isSpillable().orElseThrow()) {
                    checkArgument(streams(node.getLeft()).getDistribution() == FIXED, "Spillable hash join requires fixed probe streams: %s", node.getId());
                    StreamProperties build = streams(node.getRight());
                    List<Symbol> keys = node.getCriteria().stream().map(JoinNode.EquiJoinClause::getRight).collect(toImmutableList());
                    checkArgument(build.isSingleStream() || build.isExactlyPartitionedOn(keys), "Hash join build streams do not match join keys: %s", node.getId());
                }
            }
            return null;
        }

        @Override
        public Void visitExchange(ExchangeNode node, Void context)
        {
            visitPlan(node, context);
            node.getOrderingScheme().ifPresent(ordering -> {
                for (int source = 0; source < node.getSources().size(); source++) {
                    List<Symbol> inputs = node.getInputs().get(source);
                    List<LocalProperty<Symbol>> desired = ordering.orderBy().stream()
                            .map(key -> (LocalProperty<Symbol>) new SortingProperty<>(inputs.get(node.getOutputSymbols().indexOf(key)), ordering.ordering(key)))
                            .collect(toImmutableList());
                    checkArgument(LocalProperties.match(streams(node.getSources().get(source)).getLocalProperties(), desired).stream().allMatch(Optional::isEmpty),
                            "Ordered exchange source is not sorted: %s",
                            node.getId());
                }
            });
            return null;
        }

        private void validateMergeInput(PlanNode source, List<Symbol> keys)
        {
            StreamProperties input = streams(source);
            checkArgument(input.isSingleStream(), "MERGE input requires a single stream: %s", source.getId());
            List<LocalProperty<Symbol>> desired = keys.stream().map(key -> (LocalProperty<Symbol>) new SortingProperty<>(key, ASC_NULLS_LAST)).collect(toImmutableList());
            checkArgument(LocalProperties.match(input.getLocalProperties(), desired).stream().allMatch(Optional::isEmpty),
                    "MERGE input is not sorted on canonical join keys: %s",
                    source.getId());
        }

        private StreamProperties streams(PlanNode node)
        {
            StreamProperties result = streams.get(node);
            if (result == null) {
                result = StreamPropertyDerivations.deriveProperties(node, node.getSources().stream().map(this::streams).collect(toImmutableList()), plannerContext, session, symbolAllocator);
                streams.put(node, result);
            }
            return result;
        }

        private ActualProperties properties(PlanNode node)
        {
            ActualProperties result = properties.get(node);
            if (result == null) {
                result = PropertyDerivations.deriveProperties(node, node.getSources().stream().map(this::properties).collect(toImmutableList()), plannerContext, session, symbolAllocator);
                properties.put(node, result);
            }
            return result;
        }
    }
}
