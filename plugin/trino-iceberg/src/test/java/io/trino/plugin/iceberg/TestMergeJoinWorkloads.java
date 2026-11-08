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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.cost.MergeJoinCostModel;
import io.trino.cost.MergeJoinCostModel.Input;
import io.trino.sql.planner.OptimizerConfig.MergeJoinStrategy;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryFailedException;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.parallel.Execution;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Future;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.SystemSessionProperties.PREFER_MERGE_JOIN;
import static io.trino.testing.assertions.Assert.assertEqualsIgnoreOrder;
import static java.nio.file.Files.createTempDirectory;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * Distributed resource validation, enabled explicitly because it writes wide input tables.
 */
@EnabledIfSystemProperty(named = "smj.validation", matches = "true")
@Execution(SAME_THREAD)
public class TestMergeJoinWorkloads
        extends AbstractTestQueryFramework
{
    private static final Logger LOG = Logger.get(TestMergeJoinWorkloads.class);

    private TestTable wide;
    private TestTable skew;
    private TestTable entropy;
    private double entropyVarcharBytes;
    private Session base;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Path spillPath = createTempDirectory("merge-join-workloads");
        closeAfterClass(() -> deleteRecursively(spillPath, ALLOW_INSECURE));
        return IcebergQueryRunner.builder()
                .setWorkerCount(2)
                .addExtraProperty("node-scheduler.include-coordinator", "false")
                .setInitialTables(ImmutableList.of())
                .addExtraProperty("spiller-spill-path", spillPath.toString())
                .addExtraProperty("sink.max-buffer-size", "4MB")
                .addExtraProperty("sink.max-broadcast-buffer-size", "4MB")
                .addExtraProperty("task.max-local-exchange-buffer-size", "4MB")
                .addExtraProperty("exchange.max-response-size", "1MB")
                .addExtraProperty("memory-revoking-threshold", "0.02")
                .addExtraProperty("memory-revoking-target", "0")
                .addIcebergProperty("iceberg.writer-sort-buffer-size", "1MB")
                .build();
    }

    @BeforeAll
    public void prepareInputs()
    {
        base = Session.builder(getSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "PARTITIONED")
                .setSystemProperty(JOIN_REORDERING_STRATEGY, "NONE")
                .setSystemProperty("spill_enabled", "true")
                .setSystemProperty("task_concurrency", "4")
                .setSystemProperty("query_max_memory_per_node", "128MB").build();
        wide = closeAfterClass(new TestTable(
                getQueryRunner()::execute,
                "merge_join_wide",
                "WITH (sorted_by = ARRAY['k ASC NULLS LAST']) AS SELECT k, rpad(to_hex(md5(to_utf8(CAST(k AS VARCHAR)))), 512, 'x') v FROM (SELECT CAST(a AS BIGINT) * 1000 + b k FROM UNNEST(sequence(0, 749)) t(a) CROSS JOIN UNNEST(sequence(0, 999)) u(b))"));
        skew = closeAfterClass(new TestTable(
                getQueryRunner()::execute,
                "merge_join_skew",
                "WITH (sorted_by = ARRAY['k ASC NULLS LAST']) AS SELECT k % 64 k, v FROM " + wide.getName() + " WHERE k < 65536"));
        assertUpdate("ANALYZE " + wide.getName());
        assertUpdate("ANALYZE " + skew.getName());
        entropy = closeAfterClass(new TestTable(
                getQueryRunner()::execute,
                "merge_join_entropy",
                "WITH (sorted_by = ARRAY['k ASC NULLS LAST']) AS SELECT k, concat(to_hex(sha512(to_utf8(CAST(k AS VARCHAR)))), to_hex(sha512(to_utf8(CAST(k AS VARCHAR) || 'a'))), to_hex(sha512(to_utf8(CAST(k AS VARCHAR) || 'b'))), to_hex(sha512(to_utf8(CAST(k AS VARCHAR) || 'c')))) v FROM " + wide.getName()));
        assertUpdate("ANALYZE " + entropy.getName());
        entropyVarcharBytes = logInputStatistics(entropy.getName());
    }

    @Test
    public void testResourceBenefit()
    {
        String query = "SELECT count(*), checksum(l.v), checksum(r.v) FROM %1$s l FULL JOIN %1$s r ON l.k = r.k".formatted(entropy.getName());
        MaterializedResult expected = computeActual(strategy("OFF", "1GB"), query);
        assertThat(expected.getMaterializedRows().getFirst().getField(0)).isEqualTo(750_000L);
        for (String strategy : List.of("OFF", "FORCE", "COST_BASED")) {
            try {
                runWorkload("wide-full-entropy", strategy(strategy, "128MB"), query, expected);
            }
            catch (QueryFailedException failure) {
                if (!strategy.equals("OFF") || !failure.getMessage().contains("memory limit")) {
                    throw failure;
                }
                LOG.info("SMJ workload scenario=wide-full-entropy strategy=OFF memoryLimitExceeded=true");
            }
        }
        assertThat((String) computeActual(strategy("COST_BASED", "128MB"), "EXPLAIN " + query).getOnlyValue()).contains("Algorithm: MERGE");
    }

    @Test
    public void testCompressibleSizeEstimateLimitation()
    {
        logInputStatistics(wide.getName());
        String query = "SELECT count(*), checksum(l.v), checksum(r.v) FROM %1$s l FULL JOIN %1$s r ON l.k = r.k".formatted(wide.getName());
        MaterializedResult expected = computeActual(strategy("OFF", "1GB"), query);
        runWorkload("wide-full-compressible", strategy("FORCE", "128MB"), query, expected);
        Session automatic = strategy("COST_BASED", "128MB");
        String plan = (String) computeActual(automatic, "EXPLAIN " + query).getOnlyValue();
        try {
            runWorkload("wide-full-compressible", automatic, query, expected);
        }
        catch (QueryFailedException failure) {
            // A compressed-size estimate does not bound retained hash memory.
            // Preserve the observation instead of treating AUTO as memory admission.
            // A selected MERGE plan must still pass this workload.
            if (plan.contains("Algorithm: MERGE") || !failure.getMessage().contains("memory limit")) {
                throw failure;
            }
            LOG.info("SMJ workload scenario=wide-full-compressible strategy=COST_BASED merge=false memoryLimitExceeded=true compressedSizeEstimate=true");
        }
    }

    private double logInputStatistics(String table)
    {
        var statistics = computeActual("SHOW STATS FOR " + table).getMaterializedRows();
        LOG.info("SMJ workload table=%s statistics=%s logicalVarcharBytes=%s",
                table,
                statistics,
                computeActual("SELECT sum(length(v)) FROM " + table).getOnlyValue());
        double varcharBytes = statistics.stream()
                .filter(row -> "v".equals(row.getField(0)))
                .mapToDouble(row -> ((Number) row.getField(1)).doubleValue())
                .findFirst()
                .orElseThrow();
        assertThat(varcharBytes).isFinite().isPositive();
        return varcharBytes;
    }

    @Test
    public void testThresholdSweeps()
    {
        // Size the workloads around the model's actual input estimates, rather
        // than assuming that compressed statistics report logical VARCHAR bytes.
        double rows = 750_000;
        Input input = new Input(rows, entropyVarcharBytes + 14 * rows, 9 * rows, rows, true);
        double memoryLimit = 128 * 1024 * 1024;
        var estimate = MergeJoinCostModel.estimate(input, input, 2, memoryLimit, 4, false, false);
        double hashBytesPerRow = estimate.hashMemoryPerTask() / rows;
        double inputBytesPerRow = estimate.buildBytesPerTask() / rows;
        var thresholds = ImmutableMap.of(
                "small", 0.3 * memoryLimit / 2 / hashBytesPerRow,
                "pressure", 0.7 * memoryLimit / 2 / hashBytesPerRow,
                "big", memoryLimit / 2 / inputBytesPerRow);
        for (var threshold : thresholds.entrySet()) {
            for (double fraction : List.of(0.9, 1.0, 1.1)) {
                long selectedRows = Math.round(threshold.getValue() * fraction);
                assertThat(selectedRows).isBetween(1L, 750_000L);
                String query = "SELECT count(*), checksum(l.v), checksum(r.v) FROM (SELECT * FROM %1$s WHERE k < %2$s) l FULL JOIN (SELECT * FROM %1$s WHERE k < %2$s) r ON l.k = r.k".formatted(entropy.getName(), selectedRows);
                MaterializedResult expected = computeActual(strategy("OFF", "1GB"), query);
                for (String strategy : List.of("OFF", "FORCE", "COST_BASED")) {
                    runWorkload("threshold-" + threshold.getKey() + "-" + selectedRows, strategy(strategy, "128MB"), query, expected);
                }
            }
        }
    }

    @Test
    public void testConcurrentJoinChainsAndSkew()
            throws Exception
    {
        String query = "SELECT count(*), checksum(l.v), checksum(r.v), checksum(s.v) FROM (SELECT * FROM %1$s WHERE k < 65536) l JOIN %2$s r ON l.k = r.k LEFT JOIN (SELECT * FROM %1$s WHERE k < 65536 AND to_hex(sha256(to_utf8(v))) <> '') s ON l.k = s.k".formatted(wide.getName(), skew.getName());
        MaterializedResult expected = computeActual(strategy("OFF", "1GB"), query);
        assertThat(expected.getMaterializedRows().getFirst().getField(0)).isEqualTo(65_536L);
        try (var executor = newFixedThreadPool(2)) {
            for (int iteration = 0; iteration < 3; iteration++) {
                Future<?> first = executor.submit(() -> runWorkload("concurrent-chain-skew", strategy("FORCE", "128MB"), query, expected));
                Future<?> second = executor.submit(() -> runWorkload("concurrent-chain-skew", strategy("FORCE", "128MB"), query, expected));
                first.get(3, MINUTES);
                second.get(3, MINUTES);
            }
        }
    }

    private Session strategy(String strategy, String memory)
    {
        return Session.builder(base).setSystemProperty(PREFER_MERGE_JOIN, strategy)
                .setSystemProperty("query_max_memory_per_node", memory).build();
    }

    private void runWorkload(String scenario, Session session, String query, MaterializedResult expected)
    {
        String plan = (String) computeActual(session, "EXPLAIN " + query).getOnlyValue();
        LOG.info("SMJ workload start scenario=%s strategy=%s merge=%s",
                scenario,
                session.getSystemProperty(PREFER_MERGE_JOIN, MergeJoinStrategy.class),
                plan.contains("Algorithm: MERGE"));
        long start = System.nanoTime();
        var result = getQueryRunner().executeWithPlan(session, query);
        long elapsedNanos = System.nanoTime() - start;
        assertEqualsIgnoreOrder(result.result().getMaterializedRows(), expected.getMaterializedRows());
        var stats = getDistributedQueryRunner().getCoordinator().getQueryManager().getFullQueryInfo(result.queryId()).getQueryStats();
        LOG.info("SMJ workload scenario=%s strategy=%s elapsedMs=%.2f peakBytes=%s spillBytes=%s merge=%s",
                scenario,
                session.getSystemProperty(PREFER_MERGE_JOIN, MergeJoinStrategy.class),
                elapsedNanos / 1_000_000.0,
                stats.getPeakUserMemoryReservation().toBytes(),
                stats.getSpilledDataSize().toBytes(),
                plan.contains("Algorithm: MERGE"));
    }
}
