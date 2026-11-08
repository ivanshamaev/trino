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
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.LocalProperty;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import io.trino.tpch.TpchTable;
import org.apache.iceberg.FileFormat;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.SystemSessionProperties.PREFER_MERGE_JOIN;
import static io.trino.SystemSessionProperties.TABLE_SCAN_NODE_PARTITIONING_MIN_BUCKET_TO_TASK_RATIO;
import static io.trino.SystemSessionProperties.USE_TABLE_SCAN_NODE_PARTITIONING;
import static io.trino.plugin.iceberg.IcebergTestUtils.checkOrcFileSorting;
import static io.trino.plugin.iceberg.IcebergTestUtils.checkParquetFileSorting;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.assertions.Assert.assertEqualsIgnoreOrder;
import static java.nio.file.Files.createTempDirectory;
import static java.util.Objects.nonNull;
import static org.apache.iceberg.FileFormat.PARQUET;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class TestIcebergSortedWriting
        extends AbstractTestQueryFramework
{
    private static final Logger LOG = Logger.get(TestIcebergSortedWriting.class);

    private TrinoFileSystem fileSystem;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Path spillPath = createTempDirectory("smj-sorted-writing-spill");
        closeAfterClass(() -> deleteRecursively(spillPath, ALLOW_INSECURE));
        var builder = IcebergQueryRunner.builder()
                .addExtraProperty("spiller-spill-path", spillPath.toString())
                .setInitialTables(ImmutableList.of(TpchTable.LINE_ITEM))
                .addIcebergProperty("iceberg.sorted-writing-enabled", "true")
                // Test staging of sorted writes to local disk
                .addIcebergProperty("iceberg.sorted-writing.local-staging-path", "/tmp/trino-${USER}")
                // Allows testing the sorting writer flushing to the file system with smaller tables
                .addIcebergProperty("iceberg.writer-sort-buffer-size", "1MB");
        if (Boolean.getBoolean("smj.calibration")) {
            // Keep exchange buffers below the intentionally small query allowance,
            // and revoke revocable memory early enough to exercise both spill paths.
            builder.addExtraProperty("sink.max-buffer-size", "4MB")
                    .addExtraProperty("sink.max-broadcast-buffer-size", "4MB")
                    .addExtraProperty("task.max-local-exchange-buffer-size", "4MB")
                    .addExtraProperty("exchange.max-response-size", "1MB")
                    .addExtraProperty("memory-revoking-threshold", "0.02")
                    .addExtraProperty("memory-revoking-target", "0");
        }
        return builder.build();
    }

    @BeforeAll
    public void initFileSystem()
    {
        fileSystem = getFileSystemFactory(getDistributedQueryRunner()).create(SESSION);
    }

    @Test
    public void testSortednessReportedToEngine()
    {
        String sortedTable = "sortedness_report_test_sorted";
        String unsortedTable = "sortedness_report_test_unsorted";
        assertUpdate("CREATE TABLE " + sortedTable + " (k BIGINT, v VARCHAR) WITH (sorted_by = ARRAY['k'])");
        assertUpdate("CREATE TABLE " + unsortedTable + " (k BIGINT, v VARCHAR)");
        String insert = "INSERT INTO %s VALUES (3, 'c'), (1, 'a'), (2, 'b'), (1, 'aa')";
        assertUpdate(insert.formatted(sortedTable), 4);
        assertUpdate(insert.formatted(unsortedTable), 4);

        // metadata-level check: the engine sees the SortingProperty for the sorted table
        // and no local properties for the unsorted table
        inTransaction(session -> {
            Metadata metadata = getQueryRunner().getPlannerContext().getMetadata();
            assertThat(getLocalProperties(metadata, session, sortedTable)).isNotEmpty();
            assertThat(getLocalProperties(metadata, session, unsortedTable)).isEmpty();
        });
    }

    @Test
    public void testMergeJoinCostCalibration()
    {
        assumeTrue(Boolean.getBoolean("smj.calibration"), "Enable with -Dsmj.calibration=true");
        Session base = Session.builder(getSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "PARTITIONED")
                .setSystemProperty(JOIN_REORDERING_STRATEGY, "NONE")
                .setSystemProperty("task_concurrency", "4")
                .setSystemProperty("spill_enabled", "true")
                .setSystemProperty("query_max_memory_per_node", "64MB")
                .build();
        try (TestTable sorted = new TestTable(
                getQueryRunner()::execute,
                "smj_calibration_sorted",
                "WITH (sorted_by = ARRAY['k']) AS SELECT k, rpad(format('%08d', k), 128, 'x') v FROM (SELECT CAST(a AS BIGINT) * 1000 + b k FROM UNNEST(sequence(0, 999)) t(a) CROSS JOIN UNNEST(sequence(0, 999)) u(b))");
                TestTable unsorted = new TestTable(getQueryRunner()::execute, "smj_calibration_unsorted", "AS SELECT * FROM " + sorted.getName());
                TestTable skew = new TestTable(getQueryRunner()::execute, "smj_calibration_skew", "WITH (sorted_by = ARRAY['k']) AS SELECT k % 128 k, v FROM " + sorted.getName());
                TestTable wide = new TestTable(getQueryRunner()::execute, "smj_calibration_wide", "WITH (sorted_by = ARRAY['k']) AS SELECT k, concat(to_hex(sha512(to_utf8(CAST(k AS VARCHAR)))), to_hex(sha512(to_utf8(CAST(k AS VARCHAR) || 'a'))), to_hex(sha512(to_utf8(CAST(k AS VARCHAR) || 'b'))), to_hex(sha512(to_utf8(CAST(k AS VARCHAR) || 'c')))) v FROM " + sorted.getName())) {
            for (String table : List.of(sorted.getName(), unsorted.getName(), skew.getName(), wide.getName())) {
                assertUpdate("ANALYZE " + table);
            }
            for (String scenario : List.of("sorted", "unsorted", "skew", "full")) {
                String left = switch (scenario) {
                    case "unsorted" -> unsorted.getName();
                    case "full" -> wide.getName();
                    default -> sorted.getName();
                };
                Session scenarioBase = base;
                if (scenario.equals("full")) {
                    scenarioBase = Session.builder(base).setSystemProperty("query_max_memory_per_node", "128MB").build();
                }
                String right = scenario.equals("skew") ? skew.getName() : left;
                String join = scenario.equals("full") ? "FULL JOIN" : "JOIN";
                String query = "SELECT count(*), checksum(l.v), checksum(r.v) FROM %s l %s %s r ON l.k = r.k".formatted(left, join, right);
                Session reference = Session.builder(scenarioBase).setSystemProperty(PREFER_MERGE_JOIN, "OFF")
                        .setSystemProperty("query_max_memory_per_node", "1GB").build();
                var expected = getQueryRunner().execute(reference, query);
                assertThat(expected.getMaterializedRows().getFirst().getField(0)).isEqualTo(1_000_000L);
                for (String strategy : List.of("OFF", "FORCE", "COST_BASED")) {
                    Session session = Session.builder(scenarioBase).setSystemProperty(PREFER_MERGE_JOIN, strategy).build();
                    List<Long> elapsed = new ArrayList<>();
                    for (int iteration = 0; iteration < 4; iteration++) {
                        long start = System.nanoTime();
                        try {
                            var result = getQueryRunner().executeWithPlan(session, query);
                            long nanos = System.nanoTime() - start;
                            assertEqualsIgnoreOrder(result.result().getMaterializedRows(), expected.getMaterializedRows());
                            var stats = getDistributedQueryRunner().getCoordinator().getQueryManager().getFullQueryInfo(result.queryId()).getQueryStats();
                            LOG.info("SMJ calibration scenario=%s strategy=%s iteration=%s elapsedMs=%.2f peakBytes=%s spillBytes=%s",
                                    scenario,
                                    strategy,
                                    iteration,
                                    nanos / 1_000_000.0,
                                    stats.getPeakUserMemoryReservation().toBytes(),
                                    stats.getSpilledDataSize().toBytes());
                            if (iteration > 0) {
                                elapsed.add(nanos);
                            }
                        }
                        catch (io.trino.testing.QueryFailedException e) {
                            if (!strategy.equals("OFF") || !scenario.equals("full") || !e.getMessage().contains("memory limit")) {
                                throw e;
                            }
                            LOG.info("SMJ calibration scenario=%s strategy=%s exceeded the memory limit", scenario, strategy);
                            break;
                        }
                    }
                    elapsed.sort(Comparator.naturalOrder());
                    if (!elapsed.isEmpty()) {
                        LOG.info("SMJ calibration median scenario=%s strategy=%s elapsedMs=%.2f", scenario, strategy, elapsed.get(elapsed.size() / 2) / 1_000_000.0);
                    }
                    String plan = (String) getQueryRunner().execute(session, "EXPLAIN " + query).getOnlyValue();
                    LOG.info("SMJ calibration plan scenario=%s strategy=%s: %s", scenario, strategy, plan);
                    if (strategy.equals("COST_BASED")) {
                        if (scenario.equals("full")) {
                            assertThat(plan).contains("Algorithm: MERGE");
                        }
                        else {
                            assertThat(plan).doesNotContain("Algorithm: MERGE");
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testSortednessMetadataPlanningCost()
    {
        assumeTrue(Boolean.getBoolean("smj.large"), "Enable with -Dsmj.large=true");
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "sortedness_metadata_cost",
                "(k BIGINT) WITH (partitioning = ARRAY['bucket(k, 128)'], sorted_by = ARRAY['k ASC NULLS LAST'])")) {
            assertUpdate("INSERT INTO %s SELECT k FROM UNNEST(sequence(1, 8192)) AS t(k)".formatted(table.getName()), 8192);
            long fileCount = (long) computeActual("SELECT count(*) FROM \"%s$files\"".formatted(table.getName())).getOnlyValue();
            assertThat(fileCount).isGreaterThanOrEqualTo(64);

            inTransaction(session -> {
                Metadata metadata = getQueryRunner().getPlannerContext().getMetadata();
                QualifiedObjectName name = new QualifiedObjectName(
                        session.getCatalog().orElseThrow(),
                        session.getSchema().orElseThrow(),
                        table.getName());
                var handle = metadata.getTableHandle(session, name).orElseThrow();

                long start = System.nanoTime();
                List<LocalProperty<ColumnHandle>> expected = metadata.getTableProperties(session, handle).getLocalProperties();
                long coldNanos = System.nanoTime() - start;
                assertThat(expected).isNotEmpty();

                var projectedHandle = handle.withConnectorHandle(((IcebergTableHandle) handle.connectorHandle())
                        .withProjectedColumns(ImmutableSet.of((IcebergColumnHandle) metadata.getColumnHandles(session, handle).get("k"))));
                start = System.nanoTime();
                assertThat(metadata.getTableProperties(session, projectedHandle).getLocalProperties()).isEqualTo(expected);
                long projectedNanos = System.nanoTime() - start;

                start = System.nanoTime();
                for (int i = 0; i < 10; i++) {
                    assertThat(metadata.getTableProperties(session, i % 2 == 0 ? handle : projectedHandle).getLocalProperties()).isEqualTo(expected);
                }
                long warmNanos = (System.nanoTime() - start) / 10;
                LOG.info("Iceberg sortedness metadata: %s files, cold=%s ns, first projected=%s ns, cached=%s ns per call", fileCount, coldNanos, projectedNanos, warmNanos);
            });
        }
    }

    @Test
    public void testSortedMergeJoinAcrossMultipleFiles()
    {
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "sorted_merge_join_multifile",
                "(k BIGINT, v VARCHAR) WITH (partitioning = ARRAY['bucket(k, 2)'], sorted_by = ARRAY['k ASC NULLS LAST'])")) {
            assertUpdate("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (1, 'c'), (3, 'd')".formatted(table.getName()), 4);
            assertUpdate("INSERT INTO %s VALUES (1, 'e'), (2, 'f'), (1, 'g'), (3, 'h')".formatted(table.getName()), 4);
            assertThat((long) computeActual("SELECT count(*) FROM \"%s$files\"".formatted(table.getName())).getOnlyValue()).isGreaterThanOrEqualTo(2);

            Session mergeSession = Session.builder(getSession())
                    .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "PARTITIONED")
                    .setSystemProperty(JOIN_REORDERING_STRATEGY, "NONE")
                    .setSystemProperty(PREFER_MERGE_JOIN, "FORCE")
                    .setSystemProperty(TABLE_SCAN_NODE_PARTITIONING_MIN_BUCKET_TO_TASK_RATIO, "0.01")
                    .build();
            for (boolean useScanPartitioning : ImmutableList.of(true, false)) {
                Session session = Session.builder(mergeSession)
                        .setSystemProperty(USE_TABLE_SCAN_NODE_PARTITIONING, Boolean.toString(useScanPartitioning))
                        .build();
                Session hashSession = Session.builder(session)
                        .setSystemProperty(PREFER_MERGE_JOIN, "OFF")
                        .build();
                for (String joinType : ImmutableList.of("JOIN", "FULL JOIN")) {
                    String query = "SELECT l.k, l.v, r.v FROM %1$s l %2$s %1$s r ON l.k = r.k".formatted(table.getName(), joinType);
                    QueryRunner.MaterializedResultWithPlan result = getQueryRunner().executeWithPlan(session, query);
                    assertEqualsIgnoreOrder(result.result().getMaterializedRows(), computeActual(hashSession, query).getMaterializedRows());
                    assertThat((String) computeActual(session, "EXPLAIN " + query).getOnlyValue())
                            .contains("Algorithm: MERGE")
                            .contains("MergeSortedPages");
                    var operators = getDistributedQueryRunner().getCoordinator().getQueryManager()
                            .getFullQueryInfo(result.queryId()).getQueryStats().getOperatorSummaries();
                    long mergedRows = operators.stream()
                            .map(operator -> operator.getMetrics().getMetrics().get("Merged pre-sorted input rows"))
                            .filter(metric -> nonNull(metric))
                            .mapToLong(metric -> ((LongCount) metric).getTotal()).sum();
                    long fallbackRows = operators.stream()
                            .map(operator -> operator.getMetrics().getMetrics().get("Fallback sorted input rows"))
                            .filter(metric -> nonNull(metric))
                            .mapToLong(metric -> ((LongCount) metric).getTotal()).sum();
                    assertThat(mergedRows).isGreaterThan(0);
                    assertThat(fallbackRows).isZero();
                }
            }
        }
    }

    private static List<LocalProperty<ColumnHandle>> getLocalProperties(Metadata metadata, Session session, String tableName)
    {
        QualifiedObjectName qualifiedObjectName = new QualifiedObjectName(
                session.getCatalog().orElseThrow(),
                session.getSchema().orElseThrow(),
                tableName);
        return metadata.getTableProperties(session, metadata.getTableHandle(session, qualifiedObjectName).orElseThrow())
                .getLocalProperties();
    }

    @Test
    public void testSortedWritingWithLocalStaging()
    {
        testSortedWritingWithLocalStaging(FileFormat.ORC);
        testSortedWritingWithLocalStaging(FileFormat.PARQUET);
    }

    private void testSortedWritingWithLocalStaging(FileFormat format)
    {
        // Using a larger table forces buffered data to be written to disk
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_sorted_lineitem_table",
                "WITH (sorted_by = ARRAY['comment'], format = '" + format.name() + "') AS TABLE tpch.tiny.lineitem WITH NO DATA")) {
            assertUpdate(
                    "INSERT INTO " + table.getName() + " TABLE tpch.tiny.lineitem",
                    "VALUES 60175");
            for (Object filePath : computeActual("SELECT file_path from \"" + table.getName() + "$files\"").getOnlyColumnAsSet()) {
                assertThat(isFileSorted(Location.of((String) filePath), "comment", format)).isTrue();
            }
            assertQuery("SELECT * FROM " + table.getName(), "SELECT * FROM lineitem");
        }
    }

    private boolean isFileSorted(Location path, String sortColumnName, FileFormat format)
    {
        if (format == PARQUET) {
            return checkParquetFileSorting(fileSystem.newInputFile(path), sortColumnName);
        }
        return checkOrcFileSorting(fileSystem, path, sortColumnName);
    }
}
