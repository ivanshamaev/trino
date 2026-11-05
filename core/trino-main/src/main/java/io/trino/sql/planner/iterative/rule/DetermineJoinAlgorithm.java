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
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.SystemSessionProperties;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.operator.RetryPolicy;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IrExpressions;
import io.trino.sql.ir.IsNull;
import io.trino.sql.planner.OptimizerConfig;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.JoinNode.DistributionType;
import io.trino.sql.planner.plan.JoinNode.EquiJoinClause;
import io.trino.sql.planner.plan.JoinNode.JoinAlgorithm;
import io.trino.sql.planner.plan.JoinType;
import io.trino.sql.planner.plan.PlanNode;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.SystemSessionProperties.getCharVarcharCoercion;
import static io.trino.SystemSessionProperties.getMergeJoinBigSideThreshold;
import static io.trino.SystemSessionProperties.getMergeJoinMemoryPressureThreshold;
import static io.trino.SystemSessionProperties.getMergeJoinSmallBuildThreshold;
import static io.trino.SystemSessionProperties.getQueryMaxMemoryPerNode;
import static io.trino.SystemSessionProperties.getRetryPolicy;
import static io.trino.sql.ir.IrUtils.combineConjuncts;
import static io.trino.sql.planner.plan.Patterns.join;
import static java.util.Objects.requireNonNull;

/**
 * Marks eligible equi-joins with {@link JoinAlgorithm#MERGE} when the session enables
 * merge joins. This rule runs after {@code DetermineJoinDistributionType} (the
 * distribution must already be decided) and before {@code AddExchanges}, so the
 * exchange planner can build sorted streams for both join sides.
 *
 * <p>Eligibility (MUST gates, see ai-docs/smj-research/15-smj-selection-policy.md):
 * <ul>
 *     <li>INNER or LEFT join with non-empty equi-criteria</li>
 *     <li>PARTITIONED distribution</li>
 *     <li>all key types are orderable; DOUBLE and REAL keys are excluded until
 *     NaN-handling guards are validated (compare(NaN, NaN) == 0 while NaN = NaN is false)</li>
 *     <li>retry policy NONE (merging sources are not retryable)</li>
 *     <li>distributed sort enabled (otherwise the local planner would serialize the
 *     whole input pipeline into a single stream)</li>
 * </ul>
 *
 * <p>The PREFER strategy currently selects merge for every eligible join; resource-based
 * triggers (build side exceeding memory thresholds, sortedness discounts) are planned
 * for the cost-based phase.</p>
 */
public class DetermineJoinAlgorithm
        implements Rule<JoinNode>
{
    private static final Pattern<JoinNode> PATTERN = join()
            .matching(joinNode -> joinNode.getDistributionType().isPresent())
            .matching(joinNode -> joinNode.getAlgorithm().isEmpty());

    private final PlannerContext plannerContext;

    public DetermineJoinAlgorithm(PlannerContext plannerContext)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
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

        // COST_BASED: only select merge when the build side is large enough that hash join
        // would either OOM or spill excessively (W2 trigger); otherwise prefer hash (N1/N4)
        if (strategy == OptimizerConfig.MergeJoinStrategy.COST_BASED
                && !isCostBasedMergePreferred(joinNode, context, session)) {
            return Result.empty();
        }

        // For INNER joins, equi-join keys can never be NULL (NULL = NULL is false), so
        // adding IS NOT NULL filters prunes the NULL tail before the shuffle (C9 risk)
        if (joinNode.getType() == JoinType.INNER) {
            joinNode = addNotNullFilters(joinNode, session, context);
        }

        return Result.ofPlanNode(joinNode.withAlgorithm(JoinAlgorithm.MERGE));
    }

    /**
     * Cost-based selection per the policy in ai-docs/smj-research/15-smj-selection-policy.md
     * §15.4-15.5: select merge when the build side exceeds the memory pressure threshold
     * (W2: hash join would OOM or spill); do not select when the build is small (N1:
     * sorting would add CPU overhead without memory benefit).
     */
    private boolean isCostBasedMergePreferred(JoinNode joinNode, Context context, Session session)
    {
        PlanNodeStatsEstimate buildStats = context.getStatsProvider().getStats(joinNode.getRight());
        PlanNodeStatsEstimate probeStats = context.getStatsProvider().getStats(joinNode.getLeft());

        double buildSizeInBytes = buildStats.getOutputSizeInBytes(joinNode.getRight().getOutputSymbols());
        double probeSizeInBytes = probeStats.getOutputSizeInBytes(joinNode.getLeft().getOutputSymbols());

        // reference: per-task usable memory (simplified; the full model uses
        // query.max-memory-per-node scaled by task concurrency, refined in phase 3)
        DataSize maxMemory = getQueryMaxMemoryPerNode(session);
        long taskMemoryBytes = maxMemory.toBytes();

        double alpha = getMergeJoinMemoryPressureThreshold(session);
        double beta = getMergeJoinSmallBuildThreshold(session);
        double gamma = getMergeJoinBigSideThreshold(session);

        // W2: build side exceeds the memory pressure threshold — hash would OOM/spill
        if (buildSizeInBytes > alpha * taskMemoryBytes) {
            return true;
        }

        // W3: both sides are big — spill is inevitable on both sides
        if (buildSizeInBytes > gamma * taskMemoryBytes && probeSizeInBytes > gamma * taskMemoryBytes) {
            return true;
        }

        // N1: build fits easily — hash join is cheaper (no sort overhead)
        if (buildSizeInBytes < beta * taskMemoryBytes) {
            return false;
        }

        // between beta and alpha: borderline, prefer hash (safe default; merge is an
        // optimization for memory pressure, not a general-purpose replacement)
        return false;
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
                && !(type instanceof RealType);
    }
}
