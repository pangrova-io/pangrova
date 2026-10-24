package org.pangrova.connector.iceberg;

import org.pangrova.connector.iceberg.write.IcebergDeleteWriter;
import org.pangrova.spi.index.statistics.FileColumnStatistics;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies G-INDEX-07 fixes for the Iceberg connector's
 * file-statistics path:
 *
 * <ol>
 *   <li>{@link IcebergFileStatisticsReader#liveRowCount} returns
 *       {@code recordCount - |deletedPositions|} for files with v3 DV
 *       Puffin delete files (deletion-vector awareness).</li>
 *   <li>{@link IcebergFileStatisticsReader#buildFileColumnStatistics} threads
 *       Puffin NDV (Iceberg standard) into per-file column statistics.</li>
 * </ol>
 *
 * <p>Uses real HadoopCatalog on local filesystem (no mocks per pangrova-rules §12).
 */
class IcebergFileStatisticsReaderTest {

    @TempDir
    Path warehouse;

    private Catalog catalog;

    private static final String CATALOG_NAME = "stats_reader_catalog";
    private static final String NAMESPACE = "stats_reader_db";

    private static final Schema TEST_SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()));

    @BeforeEach
    void setUp() {
        HadoopTestGuard.assumeHadoopCompatible();
        catalog = buildHadoopCatalog();
        if (catalog instanceof SupportsNamespaces ns) {
            ns.createNamespace(Namespace.of(NAMESPACE));
        }
    }

    @Test
    void liveRowCount_noDeletes_returnsFullRecordCount() {
        Table table = createV3Table("live_no_deletes");
        writeDataFile(table, 10);

        FileScanTask task = singleScanTask(table);
        long live = IcebergFileStatisticsReader.liveRowCount(table, task);

        assertThat(live).isEqualTo(10L);
        assertThat(task.deletes()).isEmpty();
    }

    @Test
    void liveRowCount_withDvDeletes_subtractsDeletedPositions() {
        Table table = createV3Table("live_with_dv");
        String dataFilePath = writeDataFile(table, 20);
        commitDvFor(table, dataFilePath, List.of(1L, 3L, 5L, 7L, 9L));

        FileScanTask task = singleScanTask(table);
        long live = IcebergFileStatisticsReader.liveRowCount(table, task);

        assertThat(live).isEqualTo(15L); // 20 - 5 deleted
        assertThat(task.deletes()).isNotEmpty();
    }

    @Test
    void liveRowCount_neverNegative_evenIfAllRowsDeleted() {
        Table table = createV3Table("live_all_deleted");
        String dataFilePath = writeDataFile(table, 5);
        commitDvFor(table, dataFilePath, List.of(0L, 1L, 2L, 3L, 4L));

        FileScanTask task = singleScanTask(table);
        long live = IcebergFileStatisticsReader.liveRowCount(table, task);

        assertThat(live).isEqualTo(0L);
    }

    @Test
    void liveRowCount_withEqualityDelete_returnsUnknownInsteadOfOverCount() {
        Table table = createTable("stats_eq_delete", "2");
        writeDataFile(table, 10);
        commitEqualityDeleteFor(table, List.of(2L, 4L));

        FileScanTask task = singleScanTask(table);

        assertThat(task.deletes()).isNotEmpty();
        assertThat(IcebergFileStatisticsReader.liveRowCount(table, task))
                .isEqualTo(IcebergFileStatisticsReader.UNKNOWN_ROW_COUNT);
    }

    @Test
    void buildFileColumnStatistics_withEqualityDelete_reportsUnknownRowCount() {
        Table table = createTable("stats_eq_delete_columns", "2");
        writeDataFile(table, 10);
        commitEqualityDeleteFor(table, List.of(2L));

        FileScanTask task = singleScanTask(table);

        List<FileColumnStatistics> stats = IcebergFileStatisticsReader
                .buildFileColumnStatistics(table, task, table.schema(), Map.of());

        assertThat(stats).isNotEmpty();
        for (FileColumnStatistics cs : stats) {
            assertThat(cs.rowCount())
                    .isEqualTo(IcebergFileStatisticsReader.UNKNOWN_ROW_COUNT);
        }
    }

    @Test
    void buildFileColumnStatistics_withoutPuffin_setsNdvToMinusOne() {
        Table table = createV3Table("stats_no_puffin");
        writeDataFile(table, 8);

        FileScanTask task = singleScanTask(table);

        List<FileColumnStatistics> stats = IcebergFileStatisticsReader
                .buildFileColumnStatistics(table, task, table.schema(), Map.of());

        assertThat(stats).isNotEmpty();
        // No Puffin stats → NDV reported as -1 (unknown), distinct from 0.
        for (FileColumnStatistics cs : stats) {
            assertThat(cs.approximateNdv()).isEqualTo(-1L);
        }
    }

    @Test
    void buildFileColumnStatistics_withPuffinNdv_threadsNdvIntoColumnStats() {
        Table table = createV3Table("stats_with_puffin");
        writeDataFile(table, 12);

        FileScanTask task = singleScanTask(table);
        // Simulate: Spark/Trino ANALYZE would have produced these per Iceberg field id.
        Map<Integer, Long> puffinNdv = Map.of(1, 12L, 2, 11L);

        List<FileColumnStatistics> stats = IcebergFileStatisticsReader
                .buildFileColumnStatistics(table, task, table.schema(), puffinNdv);

        assertThat(stats).hasSizeGreaterThanOrEqualTo(1);
        FileColumnStatistics idStats = findColumn(stats, "id");
        assertThat(idStats).isNotNull();
        assertThat(idStats.approximateNdv()).isEqualTo(12L);
    }

    @Test
    void buildFileColumnStatistics_capsNdvByLiveRowCount_afterDeletes() {
        Table table = createV3Table("stats_dv_caps_ndv");
        String dataFilePath = writeDataFile(table, 20);
        commitDvFor(table, dataFilePath, List.of(0L, 1L, 2L, 3L, 4L)); // 5 deletes, live=15

        FileScanTask task = singleScanTask(table);
        // Puffin reports a stale NDV of 20 (pre-deletion).
        Map<Integer, Long> puffinNdv = Map.of(1, 20L);

        List<FileColumnStatistics> stats = IcebergFileStatisticsReader
                .buildFileColumnStatistics(table, task, table.schema(), puffinNdv);

        FileColumnStatistics idStats = findColumn(stats, "id");
        assertThat(idStats).isNotNull();
        // NDV must be capped by live row count to remain a valid upper bound.
        assertThat(idStats.approximateNdv()).isLessThanOrEqualTo(15L);
        assertThat(idStats.rowCount()).isEqualTo(15L);
    }

    @Test
    void buildFileColumnStatistics_dvAware_rowCountReflectsDeletes() {
        Table table = createV3Table("stats_row_count_after_dv");
        String dataFilePath = writeDataFile(table, 30);
        commitDvFor(table, dataFilePath, List.of(0L, 5L, 10L)); // 3 deletes, live=27

        FileScanTask task = singleScanTask(table);
        List<FileColumnStatistics> stats = IcebergFileStatisticsReader
                .buildFileColumnStatistics(table, task, table.schema(), Map.of());

        assertThat(stats).isNotEmpty();
        for (FileColumnStatistics cs : stats) {
            assertThat(cs.rowCount()).isEqualTo(27L);
        }
    }

    private static FileColumnStatistics findColumn(List<FileColumnStatistics> stats, String name) {
        return stats.stream()
                .filter(s -> s.columnName().equals(name))
                .findFirst()
                .orElse(null);
    }

    private FileScanTask singleScanTask(Table table) {
        table.refresh();
        List<FileScanTask> tasks = new ArrayList<>();
        try (var iter = table.newScan().planFiles()) {
            iter.forEach(tasks::add);
        } catch (IOException e) {
            throw new UncheckedIOException("plan failed", e);
        }
        assertThat(tasks).hasSize(1);
        return tasks.get(0);
    }

    private void commitDvFor(Table table, String dataFilePath, List<Long> positions) {
        List<DeleteFile> deleteFiles = IcebergDeleteWriter.buildDeletionVectorFile(
                table, dataFilePath, positions);
        var rowDelta = table.newRowDelta();
        for (DeleteFile df : deleteFiles) {
            rowDelta.addDeletes(df);
        }
        rowDelta.commit();
        table.refresh();
    }

    private void commitEqualityDeleteFor(Table table, List<Long> ids) {
        Schema deleteSchema = new Schema(table.schema().findField("id"));
        List<Record> deleted = new ArrayList<>();
        for (long id : ids) {
            GenericRecord record = GenericRecord.create(deleteSchema);
            record.setField("id", id);
            deleted.add(record);
        }
        DeleteFile deleteFile = IcebergDeleteWriter.buildEqualityDeleteFile(
                table,
                new int[] {table.schema().findField("id").fieldId()},
                deleteSchema,
                deleted);
        table.newRowDelta().addDeletes(deleteFile).commit();
        table.refresh();
    }

    private Catalog buildHadoopCatalog() {
        Map<String, String> props = Map.of(
                "type", "hadoop",
                "warehouse", warehouse.toString());
        return org.apache.iceberg.CatalogUtil.buildIcebergCatalog(
                CATALOG_NAME, props, new org.apache.hadoop.conf.Configuration());
    }

    private Table createV3Table(String tableName) {
        return createTable(tableName, "3");
    }

    private Table createTable(String tableName, String formatVersion) {
        TableIdentifier tableId = TableIdentifier.of(NAMESPACE, tableName);
        return catalog.createTable(tableId, TEST_SCHEMA,
                PartitionSpec.unpartitioned(),
                Map.of("format-version", formatVersion,
                        "write.metadata.metrics.default", "full"));
    }

    private String writeDataFile(Table table, int rowCount) {
        String path = table.location() + "/data/" + UUID.randomUUID() + ".parquet";
        OutputFile outputFile = table.io().newOutputFile(path);
        try {
            DataWriter<Record> writer = Parquet.writeData(outputFile)
                    .schema(table.schema())
                    .createWriterFunc(GenericParquetWriter::create)
                    .overwrite()
                    .withSpec(PartitionSpec.unpartitioned())
                    .metricsConfig(org.apache.iceberg.MetricsConfig.forTable(table))
                    .build();
            for (int i = 0; i < rowCount; i++) {
                GenericRecord record = GenericRecord.create(table.schema());
                record.setField("id", (long) i);
                record.setField("name", "row_" + i);
                writer.write(record);
            }
            writer.close();
            table.newAppend().appendFile(writer.toDataFile()).commit();
            return path;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to write data file", e);
        }
    }
}
