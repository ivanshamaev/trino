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

import io.trino.Session;
import io.trino.SystemSessionProperties;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.operator.RetryPolicy;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.Type;
import io.trino.sql.planner.OptimizerConfig;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.JoinNode.DistributionType;
import io.trino.sql.planner.plan.JoinNode.EquiJoinClause;
import io.trino.sql.planner.plan.JoinNode.JoinAlgorithm;
import io.trino.sql.planner.plan.JoinType;

import static io.trino.SystemSessionProperties.getRetryPolicy;
import static io.trino.sql.planner.plan.Patterns.join;

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
        return Result.ofPlanNode(joinNode.withAlgorithm(JoinAlgorithm.MERGE));
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
        if (!joinNode.getDynamicFilters().isEmpty()) {
            // merge join does not collect dynamic filters yet; a registered but never
            // completed local dynamic filter would block the probe side scan forever
            return false;
        }
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
