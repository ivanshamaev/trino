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
import io.trino.Session;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.LocalProperty;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import io.trino.tpch.TpchTable;
import org.apache.iceberg.FileFormat;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.SystemSessionProperties.PREFER_MERGE_JOIN;
import static io.trino.SystemSessionProperties.TABLE_SCAN_NODE_PARTITIONING_MIN_BUCKET_TO_TASK_RATIO;
import static io.trino.plugin.iceberg.IcebergTestUtils.checkOrcFileSorting;
import static io.trino.plugin.iceberg.IcebergTestUtils.checkParquetFileSorting;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.assertions.Assert.assertEqualsIgnoreOrder;
import static org.apache.iceberg.FileFormat.PARQUET;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIcebergSortedWriting
        extends AbstractTestQueryFramework
{
    private TrinoFileSystem fileSystem;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setInitialTables(ImmutableList.of(TpchTable.LINE_ITEM))
                .addIcebergProperty("iceberg.sorted-writing-enabled", "true")
                // Test staging of sorted writes to local disk
                .addIcebergProperty("iceberg.sorted-writing.local-staging-path", "/tmp/trino-${USER}")
                // Allows testing the sorting writer flushing to the file system with smaller tables
                .addIcebergProperty("iceberg.writer-sort-buffer-size", "1MB")
                .build();
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
            Session hashSession = Session.builder(mergeSession)
                    .setSystemProperty(PREFER_MERGE_JOIN, "OFF")
                    .build();
            String query = "SELECT l.k, l.v, r.v FROM %1$s l JOIN %1$s r ON l.k = r.k".formatted(table.getName());

            assertEqualsIgnoreOrder(
                    computeActual(mergeSession, query).getMaterializedRows(),
                    computeActual(hashSession, query).getMaterializedRows());
            assertThat((String) computeActual(mergeSession, "EXPLAIN " + query).getOnlyValue()).contains("Algorithm: MERGE");
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
