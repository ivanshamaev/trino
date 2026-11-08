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
package io.trino.cost;

import io.trino.cost.MergeJoinCostModel.Input;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestMergeJoinCostModel
{
    private static final double MEMORY = 64 * 1024 * 1024;

    @Test
    public void testUnknownStatisticsAndEmptyInput()
    {
        assertThat(new Input(Double.NaN, 100, 8, 1, true).hasKnownStatistics()).isFalse();
        assertThat(new Input(1, 100, 8, Double.NaN, true).hasKnownStatistics()).isFalse();
        assertThat(new Input(0, 0, 0, 0, true).hasKnownStatistics()).isFalse();
    }

    @Test
    public void testSortedPagesStillRequireMergeAndSpill()
    {
        Input sorted = new Input(10_000_000, 1_000_000_000, 80_000_000, 10_000_000, true);
        Input unsorted = new Input(sorted.rows(), sorted.bytes(), sorted.keyBytes(), sorted.distinctKeys(), false);
        var sortedCost = MergeJoinCostModel.estimate(sorted, sorted, 4, MEMORY, 16, false, true);
        var unsortedCost = MergeJoinCostModel.estimate(unsorted, unsorted, 4, MEMORY, 16, false, true);
        assertThat(sortedCost.merge().getCpuCost()).isLessThan(unsortedCost.merge().getCpuCost());
        assertThat(sortedCost.merge().getCpuCost()).isGreaterThan(sorted.bytes() * 2);
        assertThat(sortedCost.merge().getMaxMemory() / 4).isLessThanOrEqualTo(MEMORY);
        assertThat(sortedCost.hash().getMaxMemory()).isEqualTo(unsortedCost.hash().getMaxMemory());
    }

    @Test
    public void testSkewLimitsWorkerMemoryDiscount()
    {
        Input oneKey = new Input(1_000_000, 80_000_000, 8_000_000, 1, false);
        var oneWorker = MergeJoinCostModel.estimate(oneKey, oneKey, 1, MEMORY, 16, false, true);
        var manyWorkers = MergeJoinCostModel.estimate(oneKey, oneKey, 100, MEMORY, 16, false, true);
        assertThat(manyWorkers.buildBytesPerTask()).isEqualTo(oneWorker.buildBytesPerTask());
        assertThat(manyWorkers.hashMemoryPerTask()).isEqualTo(oneWorker.hashMemoryPerTask());
    }

    @Test
    public void testColocationRemovesNetworkForBothAlgorithms()
    {
        Input input = new Input(1_000_000, 80_000_000, 8_000_000, 1_000_000, true);
        var shuffled = MergeJoinCostModel.estimate(input, input, 4, MEMORY, 16, false, true);
        var colocated = MergeJoinCostModel.estimate(input, input, 4, MEMORY, 16, true, true);
        assertThat(shuffled.merge().getNetworkCost()).isEqualTo(160_000_000);
        assertThat(shuffled.hash().getNetworkCost()).isEqualTo(160_000_000);
        assertThat(colocated.merge().getNetworkCost()).isZero();
        assertThat(colocated.hash().getNetworkCost()).isZero();
    }

    @Test
    public void testHashUnspillRequiresAWholeLocalPartitionToFit()
    {
        Input input = new Input(10_000_000, 1_000_000_000, 80_000_000, 10_000_000, false);
        assertThat(MergeJoinCostModel.estimate(input, input, 1, MEMORY, 1, false, true).hash().getCpuCost()).isInfinite();
        assertThat(MergeJoinCostModel.estimate(input, input, 1, MEMORY, 128, false, true).hash().getCpuCost()).isFinite();
        Input oneKey = new Input(input.rows(), input.bytes(), input.keyBytes(), 1, false);
        assertThat(MergeJoinCostModel.estimate(oneKey, oneKey, 100, MEMORY, 128, false, true).hash().getCpuCost()).isInfinite();
    }

    @Test
    public void testOuterHashCannotSpillButMergeRemainsBounded()
    {
        Input input = new Input(10_000_000, 1_000_000_000, 80_000_000, 10_000_000, false);
        var cost = MergeJoinCostModel.estimate(input, input, 1, MEMORY, 16, false, false);
        assertThat(cost.hash().getCpuCost()).isInfinite();
        assertThat(cost.merge().getCpuCost()).isFinite();
        assertThat(cost.merge().getMaxMemory()).isLessThanOrEqualTo(MEMORY);
    }
}
