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
import io.trino.Session;
import io.trino.SystemSessionProperties;
import io.trino.cost.CostComparator;
import io.trino.cost.MergeJoinCostModel;
import io.trino.cost.MergeJoinCostModel.Input;
import io.trino.cost.PlanCostEstimate;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.cost.TaskCountEstimator;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.operator.RetryPolicy;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.connector.SortingProperty;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IrExpressions;
import io.trino.sql.ir.IsNull;
import io.trino.sql.planner.OptimizerConfig;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.optimizations.ActualProperties;
import io.trino.sql.planner.optimizations.LocalProperties;
import io.trino.sql.planner.optimizations.PropertyDerivations;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.JoinNode.DistributionType;
import io.trino.sql.planner.plan.JoinNode.EquiJoinClause;
import io.trino.sql.planner.plan.JoinNode.JoinAlgorithm;
import io.trino.sql.planner.plan.JoinType;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.SetOperationNode;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.SystemSessionProperties.getCharVarcharCoercion;
import static io.trino.SystemSessionProperties.getQueryMaxMemoryPerNode;
import static io.trino.SystemSessionProperties.getRetryPolicy;
import static io.trino.SystemSessionProperties.getTaskConcurrency;
import static io.trino.SystemSessionProperties.isColocatedJoinEnabled;
import static io.trino.SystemSessionProperties.isSpillEnabled;
import static io.trino.sql.ir.IrUtils.combineConjuncts;
import static io.trino.sql.planner.plan.Patterns.join;
import static java.lang.Double.isFinite;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;

/**
 * Marks eligible equi-joins with {@link JoinAlgorithm#MERGE} when the session enables
 * merge joins. This rule runs after {@code DetermineJoinDistributionType} (the
 * distribution must already be decided) and before {@code AddExchanges}, so the
 * exchange planner can build sorted streams for both join sides.
 *
 * <p>Eligibility (MUST gates, see ai-docs/smj-research/15-smj-selection-policy.md):
 * <ul>
 *     <li>INNER, LEFT, RIGHT or FULL join with non-empty equi-criteria</li>
 *     <li>PARTITIONED distribution</li>
 *     <li>all key types are orderable; DOUBLE and REAL keys are excluded until
 *     NaN-handling guards are validated (compare(NaN, NaN) == 0 while NaN = NaN is false)</li>
 *     <li>retry policy NONE (merging sources are not retryable)</li>
 *     <li>distributed sort enabled (otherwise the local planner would serialize the
 *     whole input pipeline into a single stream)</li>
 * </ul>
 *
 * <p>PREFER and FORCE select every eligible join. COST_BASED additionally requires
 * known statistics, spill, a resource trigger, and a cost advantage including sorting,
 * exchanges and replay. OFF preserves the default hash join behavior.</p>
 */
public class DetermineJoinAlgorithm
        implements Rule<JoinNode>
{
    private static final Pattern<JoinNode> PATTERN = join()
            .matching(joinNode -> joinNode.getDistributionType().isPresent())
            .matching(joinNode -> joinNode.getAlgorithm().isEmpty());

    private final PlannerContext plannerContext;
    private final CostComparator costComparator;
    private final TaskCountEstimator taskCountEstimator;

    public DetermineJoinAlgorithm(PlannerContext plannerContext)
    {
        this(plannerContext, new CostComparator(new OptimizerConfig()), new TaskCountEstimator(() -> 1));
    }

    public DetermineJoinAlgorithm(PlannerContext plannerContext, CostComparator costComparator, TaskCountEstimator taskCountEstimator)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.costComparator = requireNonNull(costComparator, "costComparator is null");
        this.taskCountEstimator = requireNonNull(taskCountEstimator, "taskCountEstimator is null");
    }

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(JoinNode joinNode, Captures captures, Context context)
    {
        Session session = context.getSession();
        OptimizerConfig.MergeJoinStrategy strategy = SystemSessionProperties.getMergeJoinStrategy(session);
        if (strategy == OptimizerConfig.MergeJoinStrategy.OFF) {
            return Result.empty();
        }
        if (!isEligible(joinNode, session)) {
            return Result.empty();
        }

        if (strategy == OptimizerConfig.MergeJoinStrategy.COST_BASED && !isMergeCheaper(joinNode, context)) {
            return Result.empty();
        }

        // For INNER joins, equi-join keys can never be NULL (NULL = NULL is false), so
        // adding IS NOT NULL filters prunes the NULL tail before the shuffle (C9 risk)
        if (joinNode.getType() == JoinType.INNER) {
            joinNode = addNotNullFilters(joinNode, session, context);
        }

        return Result.ofPlanNode(joinNode.withAlgorithm(JoinAlgorithm.MERGE));
    }

    private boolean isMergeCheaper(JoinNode join, Context context)
    {
        Session session = context.getSession();
        // The bounded build/replay path requires spill. FORCE remains useful for
        // in-memory testing, but automatic selection must not rely on unbounded buffers.
        if (!isSpillEnabled(session) || !join.isSpillable().orElse(true)) {
            return false;
        }
        List<Symbol> probeKeys = join.getCriteria().stream().map(EquiJoinClause::getLeft).distinct().collect(toImmutableList());
        List<Symbol> buildKeys = join.getCriteria().stream().map(EquiJoinClause::getRight).distinct().collect(toImmutableList());
        PlanNodeStatsEstimate probeStats = context.getStatsProvider().getStats(join.getLeft());
        PlanNodeStatsEstimate buildStats = context.getStatsProvider().getStats(join.getRight());
        // Check stats before deriving connector properties (which can require metadata IO).
        Input probe = input(join.getLeft(), probeKeys, probeStats, false);
        Input build = input(join.getRight(), buildKeys, buildStats, false);
        if (!probe.hasKnownStatistics() || !build.hasKnownStatistics()) {
            return false;
        }
        ActualProperties probeProperties = properties(join.getLeft(), context);
        ActualProperties buildProperties = properties(join.getRight(), context);
        probe = input(join.getLeft(), probeKeys, probeStats, sortedPages(probeProperties, probeKeys));
        build = input(join.getRight(), buildKeys, buildStats, sortedPages(buildProperties, buildKeys));
        Map<Symbol, Set<Symbol>> mapping = join.getCriteria().stream()
                .map(EquiJoinClause::getLeft).distinct()
                .collect(toImmutableMap(symbol -> symbol, symbol -> join.getCriteria().stream()
                        .filter(clause -> clause.getLeft().equals(symbol)).map(EquiJoinClause::getRight).collect(toImmutableSet())));
        boolean colocated = isColocatedJoinEnabled(session)
                && !probeProperties.isSingleNode()
                && !buildProperties.isSingleNode()
                && probeProperties.isNodePartitionedOn(probeKeys, false)
                && buildProperties.isNodePartitionedOn(buildKeys, false)
                && probeProperties.isCompatibleTablePartitioningWith(buildProperties, symbol -> mapping.getOrDefault(symbol, Set.of()), plannerContext.getMetadata(), session);
        int tasks = taskCountEstimator.estimateHashedTaskCount(session);
        // Leave half of the per-node allowance for concurrently live upstream/downstream
        // operators. There is no query-wide admission estimate available in this rule.
        double memoryLimit = getQueryMaxMemoryPerNode(session).toBytes();
        double memory = memoryLimit / 2;
        if (memory <= 0) {
            return false;
        }
        MergeJoinCostModel.Estimate estimate = MergeJoinCostModel.estimate(
                probe,
                build,
                tasks,
                memoryLimit,
                getTaskConcurrency(session),
                colocated,
                join.getType() != JoinType.RIGHT && join.getType() != JoinType.FULL);
        boolean ready = colocated && probe.sortedPages() && build.sortedPages();
        if (!ready && estimate.hashMemoryPerTask() <= SystemSessionProperties.getMergeJoinSmallBuildThreshold(session) * memory) {
            return false;
        }
        boolean pressure = estimate.hashMemoryPerTask() > SystemSessionProperties.getMergeJoinMemoryPressureThreshold(session) * memory;
        boolean big = estimate.buildBytesPerTask() > SystemSessionProperties.getMergeJoinBigSideThreshold(session) * memory
                && estimate.probeBytesPerTask() > SystemSessionProperties.getMergeJoinBigSideThreshold(session) * memory;
        if (!ready && !pressure && !big && !(probe.sortedPages() || build.sortedPages())) {
            return false;
        }
        PlanCostEstimate merge = estimate.merge();
        double multiplier = 1 / (1 - SystemSessionProperties.getMergeJoinCostMargin(session));
        PlanCostEstimate withMargin = new PlanCostEstimate(
                merge.getCpuCost() * multiplier,
                merge.getMaxMemory() * multiplier,
                merge.getMaxMemoryWhenOutputting() * multiplier,
                merge.getNetworkCost() * multiplier);
        if (!isFinite(withMargin.getCpuCost()) || !isFinite(withMargin.getMaxMemory()) || !isFinite(withMargin.getNetworkCost())) {
            return false;
        }
        return costComparator.compare(session, withMargin, estimate.hash()) < 0;
    }

    private static Input input(PlanNode node, List<Symbol> keys, PlanNodeStatsEstimate stats, boolean sortedPages)
    {
        double distinct = keys.stream().mapToDouble(symbol -> stats.getSymbolStatistics(symbol).getDistinctValuesCount()).max().orElse(Double.NaN);
        return new Input(stats.getOutputRowCount(), stats.getOutputSizeInBytes(node.getOutputSymbols()), stats.getOutputSizeInBytes(keys), distinct, sortedPages);
    }

    private ActualProperties properties(PlanNode node, Context context)
    {
        PlanNode resolved = context.getLookup().resolve(node);
        if (resolved instanceof SetOperationNode) {
            // These nodes have not been rewritten to exchanges yet. Neither page
            // ordering nor compatible task partitioning follows from their inputs.
            return ActualProperties.builder().global(ActualProperties.Global.arbitraryPartition()).build();
        }
        return PropertyDerivations.deriveProperties(
                resolved,
                resolved.getSources().stream().map(source -> properties(source, context)).collect(toImmutableList()),
                plannerContext,
                context.getSession(),
                context.getSymbolAllocator());
    }

    private static boolean sortedPages(ActualProperties properties, List<Symbol> keys)
    {
        return LocalProperties.match(properties.getLocalProperties(), keys.stream().map(symbol -> new SortingProperty<>(symbol, SortOrder.ASC_NULLS_LAST)).collect(toImmutableList()))
                .stream().allMatch(java.util.Optional::isEmpty);
    }

    private JoinNode addNotNullFilters(JoinNode joinNode, Session session, Context context)
    {
        PlanNode left = joinNode.getLeft();
        PlanNode right = joinNode.getRight();

        List<Expression> leftPredicates = joinNode.getCriteria().stream()
                .map(JoinNode.EquiJoinClause::getLeft)
                .distinct()
                .map(symbol -> isNotNull(symbol, session))
                .collect(toImmutableList());
        List<Expression> rightPredicates = joinNode.getCriteria().stream()
                .map(JoinNode.EquiJoinClause::getRight)
                .distinct()
                .map(symbol -> isNotNull(symbol, session))
                .collect(toImmutableList());

        left = new FilterNode(
                context.getIdAllocator().getNextId(),
                left,
                combineConjuncts(leftPredicates));
        right = new FilterNode(
                context.getIdAllocator().getNextId(),
                right,
                combineConjuncts(rightPredicates));

        return (JoinNode) joinNode.replaceChildren(ImmutableList.of(left, right));
    }

    private Expression isNotNull(Symbol symbol, Session session)
    {
        return IrExpressions.not(
                plannerContext.getMetadata(),
                getCharVarcharCoercion(session),
                new IsNull(symbol.toSymbolReference()));
    }

    private static boolean isEligible(JoinNode joinNode, Session session)
    {
        if (joinNode.getType() != JoinType.INNER
                && joinNode.getType() != JoinType.LEFT
                && joinNode.getType() != JoinType.RIGHT
                && joinNode.getType() != JoinType.FULL) {
            return false;
        }
        if (joinNode.getCriteria().isEmpty()) {
            return false;
        }
        if (joinNode.getDistributionType().orElse(DistributionType.REPLICATED) != DistributionType.PARTITIONED) {
            return false;
        }
        // Dynamic filters are supported: the merge join build side collects them via the
        // DynamicFilterSourceOperator wrapper in LocalExecutionPlanner.createMergeJoin (INNER only)
        if (getRetryPolicy(session) != RetryPolicy.NONE) {
            return false;
        }
        if (!SystemSessionProperties.isDistributedSortEnabled(session)) {
            return false;
        }
        for (EquiJoinClause criterion : joinNode.getCriteria()) {
            if (!isMergeJoinKey(criterion.getLeft()) || !isMergeJoinKey(criterion.getRight())) {
                return false;
            }
        }
        return true;
    }

    private static boolean isMergeJoinKey(Symbol key)
    {
        Type type = key.type();
        // DOUBLE and REAL are orderable but excluded for now: the comparison operator
        // groups NaN keys together while equality says NaN != NaN
        return type.isOrderable()
                && !(type instanceof DoubleType)
                && !(type instanceof RealType)
                // Comparison treats nested nulls as equal, while SQL equality is unknown.
                && !(type instanceof RowType)
                && !(type instanceof ArrayType);
    }
}
