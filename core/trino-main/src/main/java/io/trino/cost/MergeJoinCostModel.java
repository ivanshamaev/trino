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

import static java.lang.Double.isFinite;
import static java.lang.Math.ceil;
import static java.lang.Math.log;
import static java.lang.Math.max;
import static java.lang.Math.min;

/**
 * Input-only costs in the same byte-based units as the other join cost calculators.
 * Output construction is common to both algorithms. Disk traffic is charged as CPU
 * work (serialization and deserialization); it is not remote network traffic.
 * Sorted input means sorted pages, not a globally ordered task stream.
 */
public final class MergeJoinCostModel
{
    private static final double PAGE_ROWS = 1024;
    private static final double MAX_PROBE_BLOCK_BYTES = 8 * 1024 * 1024;
    private static final double HASH_ROW_OVERHEAD = 24;

    private MergeJoinCostModel() {}

    public record Input(double rows, double bytes, double keyBytes, double distinctKeys, boolean sortedPages)
    {
        public boolean hasKnownStatistics()
        {
            return isFinite(rows) && rows > 0 && isFinite(bytes) && bytes > 0
                    && isFinite(keyBytes) && keyBytes > 0 && isFinite(distinctKeys) && distinctKeys > 0;
        }

        public double largestTaskFraction(int tasks)
        {
            // A key cannot be split between tasks. NDV supplies a lower bound on skew;
            // it does not describe the frequency of the most common value.
            return max(1.0 / tasks, 1.0 / max(1, distinctKeys));
        }
    }

    public record Estimate(PlanCostEstimate hash, PlanCostEstimate merge, double buildBytesPerTask, double probeBytesPerTask, double hashMemoryPerTask) {}

    public static Estimate estimate(Input probe, Input build, int tasks, double memoryLimit, int hashPartitions, boolean colocated, boolean hashSpillable)
    {
        double memoryPerTask = memoryLimit / 2;
        double probeFraction = probe.largestTaskFraction(tasks);
        double buildFraction = build.largestTaskFraction(tasks);
        double probeBytes = probe.bytes() * probeFraction;
        double buildBytes = build.bytes() * buildFraction;
        double hashMemory = 2 * buildBytes + HASH_ROW_OVERHEAD * build.rows() * buildFraction;
        double network = colocated ? 0 : probe.bytes() + build.bytes();
        double hashCpu = probe.bytes() + build.bytes() + 4 * (probe.keyBytes() + build.keyBytes());
        boolean hashSpills = hashSpillable && hashMemory > memoryPerTask;
        if (hashSpills) {
            hashCpu += 2 * (probe.bytes() + build.bytes());
        }
        // Trino unspills a complete local hash partition into PagesIndex. It does
        // not recursively repartition an oversized partition. A hot key also cannot
        // be divided between these local partitions.
        double unspillFraction = max(1.0 / tasks / hashPartitions, 1.0 / max(1, build.distinctKeys()));
        double unspillMemory = (2 * build.bytes() + HASH_ROW_OVERHEAD * build.rows()) * unspillFraction;
        double hashPeak = hashSpills ? max(min(hashMemory, memoryPerTask), unspillMemory) : hashMemory;
        PlanCostEstimate hash = new PlanCostEstimate(hashCpu, hashPeak * tasks, hashPeak * tasks, network);
        if ((!hashSpillable && hashMemory > memoryLimit) || (hashSpills && unspillMemory > memoryLimit)) {
            hash = PlanCostEstimate.infinite();
        }

        double mergeCpu = probe.bytes() + build.bytes() + probe.keyBytes() + build.keyBytes();
        mergeCpu += sortCpu(probe, probeFraction) + sortCpu(build, buildFraction);
        // Each sort can spill and merge several runs. Sorted pages avoid comparison
        // sorting, but still occupy buffers and participate in the external merge.
        mergeCpu += sortSpill(probe.bytes(), probeBytes, memoryPerTask / 4)
                + sortSpill(build.bytes(), buildBytes, memoryPerTask / 4);
        // The build buffer writes once and scans twice to discover and replay groups.
        double probeGroupBytes = probe.bytes() / max(1, min(probe.distinctKeys(), build.distinctKeys()));
        double replayPasses = max(1, ceil(probeGroupBytes / min(MAX_PROBE_BLOCK_BYTES, memoryPerTask / 4)));
        if (buildBytes > memoryPerTask / 4) {
            mergeCpu += (2 + replayPasses) * build.bytes();
        }
        double mergeMemory = min(memoryPerTask, probeBytes + buildBytes + min(MAX_PROBE_BLOCK_BYTES, probeBytes));
        PlanCostEstimate merge = new PlanCostEstimate(mergeCpu, mergeMemory * tasks, mergeMemory * tasks, network);
        return new Estimate(hash, merge, buildBytes, probeBytes, hashMemory);
    }

    private static double sortCpu(Input input, double taskFraction)
    {
        double rows = input.rows() * taskFraction;
        double comparisons = log2(max(2, rows));
        if (input.sortedPages()) {
            // Includes the linear validation of the per-page ordering.
            comparisons = 1 + log2(max(1, rows / PAGE_ROWS));
        }
        return input.keyBytes() * comparisons;
    }

    private static double sortSpill(double totalBytes, double taskBytes, double bufferBytes)
    {
        double runs = ceil(taskBytes / bufferBytes);
        if (runs <= 1) {
            return 0;
        }
        // Initial write/read plus the bounded pairwise merge levels.
        return 2 * totalBytes * (1 + ceil(log2(runs)));
    }

    private static double log2(double value)
    {
        return log(value) / log(2);
    }
}
