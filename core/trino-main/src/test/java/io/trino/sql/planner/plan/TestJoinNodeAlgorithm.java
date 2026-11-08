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
package io.trino.sql.planner.plan;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.JsonMapperProvider;
import io.trino.spi.type.Type;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolKeyDeserializer;
import io.trino.type.TypeDeserializer;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.connector.SortOrder.ASC_NULLS_LAST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestJoinNodeAlgorithm
{
    private static final PlanNodeId ID = new PlanNodeId("join");
    private static final Symbol LEFT_KEY = new Symbol(BIGINT, "left_key");
    private static final Symbol RIGHT_KEY = new Symbol(BIGINT, "right_key");
    private static final Symbol LEFT_PASS = new Symbol(BIGINT, "left_pass");
    private static final Symbol RIGHT_PASS = new Symbol(BIGINT, "right_pass");

    private static final ValuesNode LEFT_SOURCE = new ValuesNode(new PlanNodeId("values_left"), ImmutableList.of(LEFT_KEY, LEFT_PASS));
    private static final ValuesNode RIGHT_SOURCE = new ValuesNode(new PlanNodeId("values_right"), ImmutableList.of(RIGHT_KEY, RIGHT_PASS));

    private static JoinNode join(
            JoinType type,
            Optional<JoinNode.DistributionType> distributionType,
            Optional<JoinNode.JoinAlgorithm> algorithm)
    {
        return new JoinNode(
                ID,
                type,
                LEFT_SOURCE,
                RIGHT_SOURCE,
                ImmutableList.of(new JoinNode.EquiJoinClause(LEFT_KEY, RIGHT_KEY)),
                ImmutableList.of(LEFT_KEY, LEFT_PASS),
                ImmutableList.of(RIGHT_KEY, RIGHT_PASS),
                false,
                Optional.empty(),
                distributionType,
                Optional.empty(),
                algorithm,
                ImmutableMap.of(),
                Optional.empty());
    }

    private static JoinNode inner()
    {
        return join(JoinType.INNER, Optional.empty(), Optional.empty());
    }

    @Test
    public void testAlgorithmEmptyByDefault()
    {
        assertThat(inner().getAlgorithm()).isEmpty();
    }

    @Test
    public void testMergeAlgorithmAccepted()
    {
        JoinNode node = join(
                JoinType.INNER,
                Optional.of(JoinNode.DistributionType.PARTITIONED),
                Optional.of(JoinNode.JoinAlgorithm.MERGE));
        assertThat(node.getAlgorithm()).contains(JoinNode.JoinAlgorithm.MERGE);
    }

    @Test
    public void testMergeRequiresPartitionedDistribution()
    {
        assertThatThrownBy(() -> join(
                JoinType.INNER,
                Optional.of(JoinNode.DistributionType.REPLICATED),
                Optional.of(JoinNode.JoinAlgorithm.MERGE)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("PARTITIONED");
        assertThatThrownBy(() -> join(
                JoinType.INNER,
                Optional.empty(),
                Optional.of(JoinNode.JoinAlgorithm.MERGE)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("PARTITIONED");
    }

    @Test
    public void testMergeSupportsAllJoinTypes()
    {
        // RIGHT and FULL are now supported for merge join
        for (JoinType type : ImmutableList.of(JoinType.RIGHT, JoinType.FULL)) {
            JoinNode node = join(
                    type,
                    Optional.of(JoinNode.DistributionType.PARTITIONED),
                    Optional.of(JoinNode.JoinAlgorithm.MERGE));
            assertThat(node.getAlgorithm()).contains(JoinNode.JoinAlgorithm.MERGE);
        }
    }

    @Test
    public void testMergeRequiresEquiCriteria()
    {
        assertThatThrownBy(() -> new JoinNode(
                ID,
                JoinType.INNER,
                LEFT_SOURCE,
                RIGHT_SOURCE,
                ImmutableList.of(),
                ImmutableList.of(LEFT_KEY, LEFT_PASS),
                ImmutableList.of(RIGHT_KEY, RIGHT_PASS),
                false,
                Optional.empty(),
                Optional.of(JoinNode.DistributionType.PARTITIONED),
                Optional.empty(),
                Optional.of(JoinNode.JoinAlgorithm.MERGE),
                ImmutableMap.of(),
                Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("equi-criteria");
    }

    @Test
    public void testFlipChildrenPreservesMergeForInner()
    {
        JoinNode node = join(
                JoinType.INNER,
                Optional.of(JoinNode.DistributionType.PARTITIONED),
                Optional.of(JoinNode.JoinAlgorithm.MERGE));
        JoinNode flipped = node.flipChildren();
        assertThat(flipped.getType()).isEqualTo(JoinType.INNER);
        assertThat(flipped.getAlgorithm()).contains(JoinNode.JoinAlgorithm.MERGE);
    }

    @Test
    public void testFlipChildrenPreservesMergeForLeft()
    {
        JoinNode node = join(
                JoinType.LEFT,
                Optional.of(JoinNode.DistributionType.PARTITIONED),
                Optional.of(JoinNode.JoinAlgorithm.MERGE));
        JoinNode flipped = node.flipChildren();
        assertThat(flipped.getType()).isEqualTo(JoinType.RIGHT);
        // RIGHT is now supported for merge join; the marker is kept
        assertThat(flipped.getAlgorithm()).contains(JoinNode.JoinAlgorithm.MERGE);
    }

    @Test
    public void testCopyMethodsPreserveAlgorithm()
    {
        JoinNode node = join(
                JoinType.INNER,
                Optional.of(JoinNode.DistributionType.PARTITIONED),
                Optional.of(JoinNode.JoinAlgorithm.MERGE));
        assertThat(node.withSpillable(true).getAlgorithm()).contains(JoinNode.JoinAlgorithm.MERGE);
        assertThat(node.withMaySkipOutputDuplicates().getAlgorithm()).contains(JoinNode.JoinAlgorithm.MERGE);
        assertThat(node.withoutDynamicFilters().getAlgorithm()).contains(JoinNode.JoinAlgorithm.MERGE);
        assertThat(((JoinNode) node.replaceChildren(ImmutableList.of(LEFT_SOURCE, RIGHT_SOURCE))).getAlgorithm()).contains(JoinNode.JoinAlgorithm.MERGE);
        assertThat(node.withAlgorithm(JoinNode.JoinAlgorithm.MERGE).getAlgorithm()).contains(JoinNode.JoinAlgorithm.MERGE);
    }

    @Test
    public void testPlanRoundTrip()
    {
        JsonCodec<PlanNode> codec = new JsonCodecFactory(new JsonMapperProvider()
                .withKeyDeserializers(ImmutableMap.of(Symbol.class, new SymbolKeyDeserializer(TESTING_TYPE_MANAGER)))
                .withJsonDeserializers(ImmutableMap.of(Type.class, new TypeDeserializer(TESTING_TYPE_MANAGER)))
                .get()).jsonCodec(PlanNode.class);
        JoinNode original = join(JoinType.FULL, Optional.of(JoinNode.DistributionType.PARTITIONED), Optional.of(JoinNode.JoinAlgorithm.MERGE));
        JoinNode roundTrip = (JoinNode) codec.fromJson(codec.toJson(original));
        assertThat(roundTrip.getAlgorithm()).contains(JoinNode.JoinAlgorithm.MERGE);
        assertThat(roundTrip.getType()).isEqualTo(JoinType.FULL);
        assertThat(roundTrip.getCriteria()).isEqualTo(original.getCriteria());
        for (boolean boundedSpill : ImmutableList.of(false, true)) {
            SortNode sort = new SortNode(
                    new PlanNodeId("sort"),
                    LEFT_SOURCE,
                    new OrderingScheme(ImmutableList.of(LEFT_KEY), ImmutableMap.of(LEFT_KEY, ASC_NULLS_LAST)),
                    true,
                    false,
                    boundedSpill);
            SortNode deserialized = (SortNode) codec.fromJson(codec.toJson(sort));
            assertThat(deserialized.isBoundedSpill()).isEqualTo(boundedSpill);
            assertThat(((SortNode) sort.replaceChildren(ImmutableList.of(LEFT_SOURCE))).isBoundedSpill()).isEqualTo(boundedSpill);
        }
    }
}
