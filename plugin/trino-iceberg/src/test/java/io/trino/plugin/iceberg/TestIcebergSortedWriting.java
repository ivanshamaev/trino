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
import io.trino.operator.join.merge.MergeJoinOperatorInfo;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.LocalProperty;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryFailedException;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import io.trino.tpch.TpchTable;
import org.apache.avro.generic.IndexedRecord;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expressions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

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
    public void testMergeJoinWithPartialAggregationAndProjection()
    {
        try (TestTable fact = new TestTable(getQueryRunner()::execute, "audit_v03_fact", "WITH (sorted_by = ARRAY['k']) AS SELECT CAST(n % 16 AS BIGINT) k, CAST(n AS BIGINT) v FROM UNNEST(sequence(1, 640)) t(n)");
                TestTable dimension = new TestTable(getQueryRunner()::execute, "audit_v03_dim", "WITH (sorted_by = ARRAY['k']) AS SELECT CAST(n AS BIGINT) k, CAST(n % 2 AS BIGINT) g FROM UNNEST(sequence(0, 15)) t(n)")) {
            assertUpdate("ANALYZE " + fact.getName());
            assertUpdate("ANALYZE " + dimension.getName());
            Session forced = Session.builder(getSession()).setSystemProperty(PREFER_MERGE_JOIN, "FORCE")
                    .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "PARTITIONED")
                    .setSystemProperty(JOIN_REORDERING_STRATEGY, "NONE")
                    .setSystemProperty("spill_enabled", "true")
                    .setSystemProperty("push_partial_aggregation_through_join", "true").build();
            Session hash = Session.builder(forced).setSystemProperty(PREFER_MERGE_JOIN, "OFF").build();
            for (String expression : List.of("l.v", "l.v + 1")) {
                String sql = "SELECT r.k, r.g, sum(" + expression + ") FROM " + fact.getName() + " l JOIN " + dimension.getName() + " r ON l.k = r.k GROUP BY r.k, r.g";
                assertThat((String) computeActual(forced, "EXPLAIN " + sql).getOnlyValue()).contains("Algorithm: MERGE");
                var expected = computeActual(hash, sql).getMaterializedRows();
                Session noPushdown = Session.builder(forced).setSystemProperty("push_partial_aggregation_through_join", "false").build();
                var control = computeActual(noPushdown, sql).getMaterializedRows();
                assertEqualsIgnoreOrder(control, expected);
                assertEqualsIgnoreOrder(computeActual(forced, sql).getMaterializedRows(), expected);
            }
        }
    }

    @Test
    public void testMultipleSortedFilesWithMergeJoinDisabled()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "audit_v03_sorted", "(k BIGINT, v BIGINT) WITH (sorted_by = ARRAY['k', 'v'])")) {
            for (int insert = 0; insert < 5; insert++) {
                assertUpdate("INSERT INTO " + table.getName() + " SELECT k, 10 * k + " + insert + " FROM UNNEST(sequence(1, 50)) t(k)", 50);
            }
            for (String concurrency : List.of("1", "4")) {
                Session enabled = Session.builder(getSession()).setSystemProperty(PREFER_MERGE_JOIN, "OFF")
                        .setSystemProperty("task_concurrency", concurrency).build();
                Session disabled = Session.builder(enabled).setCatalogSessionProperty("iceberg", "report_sortedness", "false").build();
                for (String query : List.of(
                        "SELECT k, v, row_number() OVER (ORDER BY k, v) FROM %s",
                        "SELECT k, v, sum(v) OVER (PARTITION BY k ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM %s",
                        "SELECT k, sum(v), count(*) FROM %s GROUP BY k",
                        "SELECT k, v FROM %s ORDER BY k, v FETCH FIRST 30 ROWS WITH TIES")) {
                    String sql = query.formatted(table.getName());
                    assertEqualsIgnoreOrder(computeActual(enabled, sql).getMaterializedRows(), computeActual(disabled, sql).getMaterializedRows());
                }
            }
        }
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
                        catch (QueryFailedException e) {
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
                    assertThat(operators.stream().map(operator -> operator.getInfo())
                            .filter(MergeJoinOperatorInfo.class::isInstance).map(MergeJoinOperatorInfo.class::cast).toList())
                            .isNotEmpty().allSatisfy(info -> {
                                assertThat(info.isFinal()).isTrue();
                                assertThat(info.buildRows()).isGreaterThan(0);
                                assertThat(info.outputRows()).isGreaterThan(0);
                            });
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

    @ParameterizedTest
    @NullSource
    @ValueSource(ints = {0, 1, 999})
    public void testFileSortOrderCompatibility(Integer sortOrderId)
            throws Exception
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "file_sort_order", "(k BIGINT) WITH (sorted_by = ARRAY['k'])")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES 3, 1", 2);
            assertUpdate("INSERT INTO " + table.getName() + " VALUES 2", 1);
            Table icebergTable = loadIcebergTable(table.getName());
            DataFile original;
            try (var files = icebergTable.newScan().planFiles()) {
                original = files.iterator().next().file().copy();
            }
            DataFile replacement = withFileSortOrder(icebergTable, original, sortOrderId);
            icebergTable.newOverwrite().deleteFile(original).addFile(replacement).commit();
            try (var files = icebergTable.newScan().planFiles()) {
                assertThat(ImmutableList.copyOf(files)).anySatisfy(task -> assertThat(task.file().sortOrderId()).isEqualTo(sortOrderId));
            }
            inTransaction(session -> {
                Metadata metadata = getQueryRunner().getPlannerContext().getMetadata();
                assertThat(getLocalProperties(metadata, session, table.getName()).isEmpty()).isEqualTo(sortOrderId == null || sortOrderId != icebergTable.sortOrder().orderId());
            });
            assertOffQueries(table.getName(), "VALUES 1, 2, 3", "VALUES (1, 1), (2, 2), (3, 3)", "VALUES (6, 3)");
        }
    }

    @Test
    public void testSortOrderEvolutionAndDeletes()
            throws Exception
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "sort_order_evolution", "(k BIGINT) WITH (sorted_by = ARRAY['k'])")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES 1, 2", 2);
            Table icebergTable = loadIcebergTable(table.getName());
            icebergTable.replaceSortOrder().desc("k").commit();
            assertUpdate("INSERT INTO " + table.getName() + " VALUES 3", 1);
            inTransaction(session -> assertThat(getLocalProperties(getQueryRunner().getPlannerContext().getMetadata(), session, table.getName())).isEmpty());
            assertOffQueries(table.getName(), "VALUES 1, 2, 3", "VALUES (1, 1), (2, 2), (3, 3)", "VALUES (6, 3)");
            assertUpdate("DELETE FROM " + table.getName() + " WHERE k = 2", 1);
            assertOffQueries(table.getName(), "VALUES 1, 3", "VALUES (1, 1), (3, 2)", "VALUES (4, 2)");
        }
    }

    @Test
    public void testIdentitySortPrefix()
            throws Exception
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "sort_transform_prefix", "(k BIGINT, v BIGINT) WITH (sorted_by = ARRAY['k'])")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (1, 3), (2, 1), (3, 2)", 3);
            Table icebergTable = loadIcebergTable(table.getName());
            icebergTable.replaceSortOrder().asc(Expressions.bucket("v", 2)).asc("k").commit();
            inTransaction(session -> assertThat(getLocalProperties(getQueryRunner().getPlannerContext().getMetadata(), session, table.getName())).isEmpty());
            icebergTable.replaceSortOrder().asc("k").asc(Expressions.bucket("v", 2)).commit();
            // Unique k values mean the existing rows also obey the new secondary sort term.
            try (var files = icebergTable.newScan().planFiles()) {
                var overwrite = icebergTable.newOverwrite();
                for (var file : files) {
                    overwrite.deleteFile(file.file()).addFile(withFileSortOrder(icebergTable, file.file(), icebergTable.sortOrder().orderId()));
                }
                overwrite.commit();
            }
            inTransaction(session -> assertThat(getLocalProperties(getQueryRunner().getPlannerContext().getMetadata(), session, table.getName())).hasSize(1));
            assertOffQueries(table.getName(), "VALUES 1, 2, 3", "VALUES (1, 1), (2, 2), (3, 3)", "VALUES (6, 3)");
        }
    }

    private Table loadIcebergTable(String table)
    {
        return IcebergTestUtils.loadTable(
                table,
                IcebergTestUtils.getHiveMetastore(getQueryRunner()),
                IcebergTestUtils.getFileSystemFactory(getQueryRunner()),
                getSession().getCatalog().orElseThrow(),
                getSession().getSchema().orElseThrow());
    }

    private static DataFile withFileSortOrder(Table table, DataFile file, Integer sortOrderId)
    {
        // Rebuild the full record: planFiles can return a projected Avro record.
        DataFiles.Builder builder = DataFiles.builder(table.specs().get(file.specId())).copy(file);
        if (sortOrderId != null) {
            return builder.withSortOrderId(sortOrderId).build();
        }
        DataFile result = builder.build();
        IndexedRecord record = (IndexedRecord) result;
        record.put(record.getSchema().getField(DataFile.SORT_ORDER_ID.name()).pos(), sortOrderId);
        return result;
    }

    private void assertOffQueries(String table, String expectedRows, String expectedWindow, String expectedAggregation)
    {
        Session off = Session.builder(getSession()).setSystemProperty(PREFER_MERGE_JOIN, "OFF").build();
        assertQuery(off, "SELECT k FROM " + table, expectedRows);
        assertQuery(off, "SELECT k FROM " + table + " ORDER BY k LIMIT 2", expectedRows + " ORDER BY 1 LIMIT 2");
        assertQuery(off, "SELECT k, row_number() OVER (ORDER BY k) FROM " + table, expectedWindow);
        assertQuery(off, "SELECT sum(k), count(DISTINCT k) FROM " + table, expectedAggregation);
    }

    @Test
    public void testMergeJoinAcrossFileSplits()
            throws Exception
    {
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "sorted_split_join",
                "(k BIGINT, v VARCHAR) WITH (sorted_by = ARRAY['k'], parquet_writer_row_group_size = '1kB')")) {
            assertUpdate("INSERT INTO " + table.getName() + " SELECT k, rpad(CAST(k AS VARCHAR), 128, 'x') FROM UNNEST(sequence(1, 10000)) t(k)", 10000);
            Table icebergTable = loadIcebergTable(table.getName());
            long fileCount = 0;
            long splitCount = 0;
            try (var files = icebergTable.newScan().planFiles()) {
                for (var file : files) {
                    fileCount++;
                    for (var split : file.split(1024)) {
                        assertThat(split.length()).isPositive();
                        splitCount++;
                    }
                }
            }
            assertThat(splitCount).isGreaterThan(fileCount);
            Session base = Session.builder(getSession())
                    .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "max_split_size", "1kB")
                    .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "PARTITIONED")
                    .setSystemProperty(JOIN_REORDERING_STRATEGY, "NONE").build();
            for (String join : List.of("JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN")) {
                String query = "SELECT count(*), checksum(l.v), checksum(r.v) FROM (SELECT * FROM %1$s WHERE k %% 3 <> 0) l %2$s (SELECT * FROM %1$s WHERE k %% 5 <> 0) r ON l.k = r.k".formatted(table.getName(), join);
                Session hash = Session.builder(base).setSystemProperty(PREFER_MERGE_JOIN, "OFF").build();
                Session merge = Session.builder(base).setSystemProperty(PREFER_MERGE_JOIN, "FORCE").build();
                assertThat((String) computeActual(merge, "EXPLAIN " + query).getOnlyValue()).contains("Algorithm: MERGE");
                assertEqualsIgnoreOrder(computeActual(merge, query).getMaterializedRows(), computeActual(hash, query).getMaterializedRows());
            }
        }
    }

    @Test
    public void testRightMergeJoinCompletesDynamicFilter()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "merge_join_right_df", "(k BIGINT) WITH (sorted_by = ARRAY['k'])")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES 1, 2, 3", 3);
            Session session = Session.builder(getSession())
                    .setSystemProperty("prefer_merge_join", "FORCE")
                    .setSystemProperty("join_distribution_type", "PARTITIONED")
                    .setSystemProperty("join_reordering_strategy", "NONE")
                    .setSystemProperty("enable_dynamic_filtering", "true")
                    .build();
            for (String join : new String[] {"JOIN", "RIGHT JOIN"}) {
                String query = "SELECT l.k, r.k FROM " + table.getName() + " l " + join + " " + table.getName() + " r ON l.k = r.k";
                String explain = (String) getQueryRunner().execute(session, "EXPLAIN " + query).getOnlyValue();
                assertThat(explain).contains("Algorithm: MERGE");
                var result = getQueryRunner().executeWithPlan(session, query);
                assertThat(result.result().getMaterializedRows()).hasSize(3);
                var stats = getDistributedQueryRunner().getCoordinator().getQueryManager().getFullQueryInfo(result.queryId())
                        .getQueryStats().getDynamicFiltersStats();
                assertThat(stats.getTotalDynamicFilters()).isGreaterThan(0);
                assertThat(stats.getDynamicFiltersCompleted()).isEqualTo(stats.getTotalDynamicFilters());
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
