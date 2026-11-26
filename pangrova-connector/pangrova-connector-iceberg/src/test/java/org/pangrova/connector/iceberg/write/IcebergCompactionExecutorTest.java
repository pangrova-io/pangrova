package org.pangrova.connector.iceberg.write;

import org.pangrova.connector.iceberg.HadoopTestGuard;
import org.pangrova.connector.iceberg.IcebergMetadata;
import org.pangrova.connector.api.index.SnapshotPinOwner;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests for {@link IcebergCompactionExecutor}.
 * Validates that compaction materializes deletion vectors (DVs) into
 * new clean data files, removing all DV files from the table.
 *
 * <p>Uses real HadoopCatalog on local filesystem (no mocks per pangrova-rules §12).
 */
class IcebergCompactionExecutorTest {

    @TempDir
    Path warehouse;

    private Catalog catalog;

    private static final String CATALOG_NAME = "compact_test_catalog";
    private static final String NAMESPACE = "compact_db";

    private static final Schema TEST_SCHEMA = new Schema(
            Types.NestedField.optional(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()));

    @BeforeEach
    void setUp() {
        HadoopTestGuard.assumeHadoopCompatible();
        catalog = buildHadoopCatalog();
        createNamespace();
    }

    @Test
    void reclusterSortsAcrossFilesAndRecordsOnePublication() {
        Table table = createTable("recluster_linear", "2");
        writeDataFile(table, 3);
        writeDataFile(table, 3);
        table.replaceSortOrder().asc("id", NullOrder.NULLS_LAST).commit();
        table.refresh();
        var prior = table.currentSnapshot().snapshotId();
        UUID runId = UUID.randomUUID();

        UUID attempt = UUID.randomUUID();
        var staged = IcebergCompactionExecutor.stageRecluster(table, table.sortOrder(), attempt,
                rows -> IcebergCompactionExecutor.sortBySortOrder(rows, table.sortOrder(), table.schema()), () -> {});
        table.refresh();
        assertThat(table.currentSnapshot().snapshotId()).isEqualTo(prior);
        assertThatThrownBy(() -> IcebergCompactionExecutor.publishRecluster(
                table, table.sortOrder(), runId, "test-order", UUID.randomUUID(), staged))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(IcebergCompactionExecutor.publishRecluster(
                table, table.sortOrder(), runId, "test-order", attempt, staged)).isTrue();

        table.refresh();
        assertThat(table.currentSnapshot().parentId()).isEqualTo(prior);
        assertThat(table.currentSnapshot().summary().get(IcebergCompactionExecutor.RECLUSTER_RUN))
                .isEqualTo(runId.toString());
        assertThat(readAllRows(table)).extracting(row -> row.getField("id")).containsExactly(0L, 0L, 1L, 1L, 2L, 2L);
        try (var files = table.newScan().planFiles()) {
            assertThat(files).singleElement().satisfies(task ->
                    assertThat(task.file().sortOrderId()).isEqualTo(table.sortOrder().orderId()));
        } catch (IOException failure) {
            throw new java.io.UncheckedIOException(failure);
        }
    }

    @Test
    void compact_tableWithDeletionVectors_materializesDvsIntoCleanDataFiles() {
        Table table = createV3Table("dv_compact_test");
        writeDataFile(table, 20);

        // Create DV: delete rows at positions 1, 5, 10, 15
        String dataFilePath = findDataFilePath(table);
        List<DeleteFile> dvFiles = IcebergDeleteWriter.buildDeletionVectorFile(
                table, dataFilePath, List.of(1L, 5L, 10L, 15L));
        commitDeletes(table, dvFiles);

        // Verify DV exists before compaction
        assertThat(countDeleteFiles(table)).isGreaterThan(0);

        // Run compaction
        var summary = IcebergCompactionExecutor.compact(table);

        // Verify compaction results
        assertThat(summary.filesCompacted()).isEqualTo(1);
        assertThat(summary.filesProduced()).isEqualTo(1);
        assertThat(summary.bytesRewritten()).isGreaterThan(0L);
        assertThat(summary.bytesSaved()).isGreaterThan(0L);
        assertThat(summary.deleteFilesRemoved()).isGreaterThan(0);

        // Verify no delete files remain after compaction
        table.refresh();
        assertThat(countDeleteFiles(table)).isZero();

        // Verify data correctness: 20 original - 4 deleted = 16 surviving
        List<Record> rows = readAllRows(table);
        assertThat(rows).hasSize(16);

        // Verify deleted rows are not present
        List<Long> ids = rows.stream()
                .map(r -> (Long) r.getField("id"))
                .sorted()
                .toList();
        assertThat(ids).doesNotContain(1L, 5L, 10L, 15L);

        // Verify surviving rows are correct
        assertThat(ids).contains(0L, 2L, 3L, 4L, 6L, 7L, 8L, 9L,
                11L, 12L, 13L, 14L, 16L, 17L, 18L, 19L);
    }

    @Test
    void compact_tableWithPositionDeletes_materializesIntoCleanDataFiles() {
        // Position deletes are a v2 feature; DVs are v3
        Table table = createTable("pos_delete_compact_test", "2");
        writeDataFile(table, 10);

        String dataFilePath = findDataFilePath(table);
        DeleteFile posDeleteFile = IcebergDeleteWriter.buildPositionDeleteFile(
                table, dataFilePath, List.of(2L, 4L, 6L));
        commitDeletes(table, List.of(posDeleteFile));

        assertThat(countDeleteFiles(table)).isGreaterThan(0);

        var summary = IcebergCompactionExecutor.compact(table);

        assertThat(summary.filesCompacted()).isEqualTo(1);
        assertThat(summary.filesProduced()).isEqualTo(1);

        table.refresh();
        assertThat(countDeleteFiles(table)).isZero();

        List<Record> rows = readAllRows(table);
        assertThat(rows).hasSize(7);

        List<Long> ids = rows.stream()
                .map(r -> (Long) r.getField("id"))
                .sorted()
                .toList();
        assertThat(ids).containsExactly(0L, 1L, 3L, 5L, 7L, 8L, 9L);
    }

    @Test
    void positionDeleteRemainsAttachedWhenADataPredicateIsPushedDown() throws IOException {
        Table table = createTable("pos_delete_predicate", "2");
        writeDataFile(table, 3);
        DeleteFile deleteFile = IcebergDeleteWriter.buildPositionDeleteFile(
                table, findDataFilePath(table), List.of(1L));
        commitDeletes(table, List.of(deleteFile));
        table.refresh();

        try (var tasks = table.newScan().filter(org.apache.iceberg.expressions.Expressions.equal("id", 1L))
                .planFiles()) {
            assertThat(tasks).singleElement().satisfies(task -> assertThat(task.deletes())
                    .extracting(file -> file.path().toString()).containsExactly(deleteFile.path().toString()));
        }
    }

    @Test
    void rejectsConcurrentDeleteCommittedAfterRewritePlanning() {
        Table table = createTable("compaction_racing_delete", "2");
        writeDataFile(table, 10);
        String originalFile = findDataFilePath(table);
        commitDeletes(table, List.of(
                IcebergDeleteWriter.buildPositionDeleteFile(table, originalFile, List.of(1L))));
        var operations = ((HasTableOperations) table).operations();
        Table compactionTable = new BaseTable(operations, table.name()) {
            @Override
            public RewriteFiles newRewrite() {
                commitDeletes(table, List.of(
                        IcebergDeleteWriter.buildPositionDeleteFile(table, originalFile, List.of(3L))));
                refresh();
                return super.newRewrite();
            }
        };

        assertThatThrownBy(() -> IcebergCompactionExecutor.compact(compactionTable))
                .isInstanceOf(ValidationException.class);
        assertThat(findAllDataFilePaths(table)).containsExactly(originalFile);
        assertThat(countDeleteFiles(table)).isEqualTo(2);
    }

    @Test
    void compact_manySmallFilesWithoutDeletes_mergesIntoOneTargetSizedFile() {
        Table table = createV3Table("small_files_merge_test");
        for (int file = 0; file < 6; file++) {
            writeDataFile(table, 5);
        }
        table.refresh();
        assertThat(findAllDataFilePaths(table)).hasSize(6);

        var summary = IcebergCompactionExecutor.compact(table);

        assertThat(summary.filesCompacted()).isEqualTo(6);
        assertThat(summary.filesProduced()).isEqualTo(1);
        assertThat(summary.deleteFilesRemoved()).isZero();

        table.refresh();
        assertThat(findAllDataFilePaths(table)).hasSize(1);
        assertThat(readAllRows(table)).hasSize(30);
    }

    @Test
    void physicalCompactionAndExpirationPreserveEachExactIndexHold() throws IOException {
        String name = "pinned_physical_compaction";
        Table table = createV3Table(name);
        for (int file = 0; file < 5; file++) writeDataFile(table, 2);
        table.refresh();
        long oldSnapshot = table.currentSnapshot().snapshotId();
        String oldToken = Long.toString(oldSnapshot);
        List<String> oldFiles = findAllDataFilePaths(table);
        assertThat(oldFiles).hasSize(5);
        var first = new SnapshotPinOwner("index-one", 0, "attempt-one");
        var second = new SnapshotPinOwner("index-two", 0, "attempt-two");
        var replacement = new SnapshotPinOwner("index-one", 1, "attempt-three");
        var metadata = new IcebergMetadata(catalog, CATALOG_NAME);
        var pins = metadata.getIndexSupport().orElseThrow().getSnapshotPinProvider().orElseThrow();
        pins.pinSnapshot(CATALOG_NAME, NAMESPACE, name, oldToken, first);
        pins.pinSnapshot(CATALOG_NAME, NAMESPACE, name, oldToken, second);

        writeDataFile(table, 3);
        var compacted = IcebergCompactionExecutor.compact(table);
        assertThat(compacted.filesCompacted()).isEqualTo(6);
        assertThat(compacted.filesProduced()).isEqualTo(1);
        table.refresh();
        long currentSnapshot = table.currentSnapshot().snapshotId();
        String currentToken = Long.toString(currentSnapshot);
        assertThat(currentSnapshot).isNotEqualTo(oldSnapshot);
        assertThat(findAllDataFilePaths(table)).hasSize(1).doesNotContainAnyElementsOf(oldFiles);
        pins.pinSnapshot(CATALOG_NAME, NAMESPACE, name, currentToken, replacement);

        table.expireSnapshots().expireOlderThan(System.currentTimeMillis() + 1_000)
                .retainLast(1).cleanExpiredFiles(true).commit();
        table.refresh();
        var reopenedCatalog = buildHadoopCatalog();
        try {
            var reopened = new IcebergMetadata(reopenedCatalog, CATALOG_NAME);
            var reopenedPins = reopened.getIndexSupport().orElseThrow().getSnapshotPinProvider().orElseThrow();
            reopenedPins.validateSnapshotPin(CATALOG_NAME, NAMESPACE, name, oldToken, first);
            reopenedPins.validateSnapshotPin(CATALOG_NAME, NAMESPACE, name, oldToken, second);
            assertThat(readRowsAtSnapshot(table, oldSnapshot)).extracting(row -> row.getField("id"))
                    .containsExactlyInAnyOrder(0L, 1L, 0L, 1L, 0L, 1L, 0L, 1L, 0L, 1L);
            assertThat(readRowsAtSnapshot(table, currentSnapshot)).hasSize(13);
            assertThat(oldFiles).allMatch(path -> table.io().newInputFile(path).exists());

            reopenedPins.unpinSnapshot(CATALOG_NAME, NAMESPACE, name, oldToken, first);
            table.expireSnapshots().expireOlderThan(System.currentTimeMillis() + 1_000)
                    .retainLast(1).cleanExpiredFiles(true).commit();
            table.refresh();
            reopenedPins.validateSnapshotPin(CATALOG_NAME, NAMESPACE, name, oldToken, second);
            assertThat(readRowsAtSnapshot(table, oldSnapshot)).hasSize(10);
            reopenedPins.unpinSnapshot(CATALOG_NAME, NAMESPACE, name, oldToken, second);
            table.expireSnapshots().expireSnapshotId(oldSnapshot).cleanExpiredFiles(true).commit();
            table.refresh();
            assertThat(table.snapshot(oldSnapshot)).isNull();
            assertThat(oldFiles).noneMatch(path -> table.io().newInputFile(path).exists());
            reopenedPins.validateSnapshotPin(CATALOG_NAME, NAMESPACE, name, currentToken, replacement);
            assertThat(readRowsAtSnapshot(table, currentSnapshot)).hasSize(13);
        } finally {
            if (reopenedCatalog instanceof AutoCloseable closeable) {
                try {
                    closeable.close();
                } catch (Exception e) {
                    throw new IOException(e);
                }
            }
        }
    }

    @Test
    void compact_tableWithNoDeletes_skipsCompaction() {
        Table table = createV3Table("no_deletes_test");
        writeDataFile(table, 5);

        var summary = IcebergCompactionExecutor.compact(table);

        assertThat(summary.filesCompacted()).isZero();
        assertThat(summary.filesProduced()).isZero();
        assertThat(summary.bytesRewritten()).isZero();
        assertThat(summary.bytesSaved()).isZero();
        assertThat(summary.deleteFilesRemoved()).isZero();

        // Data unchanged
        List<Record> rows = readAllRows(table);
        assertThat(rows).hasSize(5);
    }

    @Test
    void compact_multipleDvsOnSameFile_mergesAndMaterializes() {
        Table table = createV3Table("multi_dv_compact_test");
        writeDataFile(table, 20);

        String dataFilePath = findDataFilePath(table);

        // First DV: delete positions 0, 2
        List<DeleteFile> dv1 = IcebergDeleteWriter.buildDeletionVectorFile(
                table, dataFilePath, List.of(0L, 2L));
        commitDeletes(table, dv1);

        // Second DV: merge with first, delete positions 0, 2, 4, 6
        table.refresh();
        List<DeleteFile> mergedDv = IcebergDeleteWriter.mergeDeletionVectors(
                table, dataFilePath,
                List.of(List.of(0L, 2L), List.of(4L, 6L)));

        // Replace old DV with merged DV
        var rewrite = table.newRewrite();
        rewrite.rewriteFiles(
                java.util.Set.of(),
                java.util.Set.copyOf(dv1),
                java.util.Set.of(),
                java.util.Set.copyOf(mergedDv));
        rewrite.commit();

        // Compact
        var summary = IcebergCompactionExecutor.compact(table);

        assertThat(summary.filesCompacted()).isEqualTo(1);
        assertThat(summary.filesProduced()).isEqualTo(1);

        table.refresh();
        assertThat(countDeleteFiles(table)).isZero();

        List<Record> rows = readAllRows(table);
        assertThat(rows).hasSize(16); // 20 - 4 deleted

        List<Long> ids = rows.stream()
                .map(r -> (Long) r.getField("id"))
                .sorted()
                .toList();
        assertThat(ids).doesNotContain(0L, 2L, 4L, 6L);
    }

    @Test
    void compact_allRowsDeleted_producesNoNewDataFile() {
        Table table = createV3Table("all_deleted_test");
        writeDataFile(table, 3);

        String dataFilePath = findDataFilePath(table);
        List<DeleteFile> dvFiles = IcebergDeleteWriter.buildDeletionVectorFile(
                table, dataFilePath, List.of(0L, 1L, 2L));
        commitDeletes(table, dvFiles);

        var summary = IcebergCompactionExecutor.compact(table);

        assertThat(summary.filesCompacted()).isEqualTo(1);
        assertThat(summary.filesProduced()).isZero();

        table.refresh();
        assertThat(countDeleteFiles(table)).isZero();

        List<Record> rows = readAllRows(table);
        assertThat(rows).isEmpty();
    }

    @Test
    void compact_multipleDataFilesWithDvs_compactsAllAndTracksMetrics() {
        Table table = createV3Table("multi_file_dv_compact_test");

        // Write two separate data files
        writeDataFile(table, 10); // file 1: rows 0-9
        writeDataFile(table, 10); // file 2: rows 0-9

        // Get both data file paths
        table.refresh();
        List<String> dataFilePaths = findAllDataFilePaths(table);
        assertThat(dataFilePaths).hasSize(2);

        // Create DVs on both files
        List<DeleteFile> dv1 = IcebergDeleteWriter.buildDeletionVectorFile(
                table, dataFilePaths.get(0), List.of(0L, 1L, 2L));
        commitDeletes(table, dv1);

        List<DeleteFile> dv2 = IcebergDeleteWriter.buildDeletionVectorFile(
                table, dataFilePaths.get(1), List.of(3L, 4L, 5L, 6L));
        commitDeletes(table, dv2);

        assertThat(countDeleteFiles(table)).isEqualTo(2);

        // Run compaction
        var summary = IcebergCompactionExecutor.compact(table);

        // Verify metrics: bin-pack merges both tiny files into one target-sized
        // output, so two inputs still produce a single file.
        assertThat(summary.filesCompacted()).isEqualTo(2);
        assertThat(summary.filesProduced()).isEqualTo(1);
        assertThat(summary.bytesRewritten()).isGreaterThan(0L);
        assertThat(summary.bytesSaved()).isGreaterThan(0L);
        assertThat(summary.deleteFilesRemoved()).isEqualTo(2);

        // Verify no delete files remain
        table.refresh();
        assertThat(countDeleteFiles(table)).isZero();

        // Verify data correctness: file1 lost 3 rows, file2 lost 4 rows
        List<Record> rows = readAllRows(table);
        assertThat(rows).hasSize(13); // (10 - 3) + (10 - 4) = 13
    }

    // -- helpers --

    private Catalog buildHadoopCatalog() {
        Map<String, String> props = Map.of(
                "type", "hadoop",
                "warehouse", warehouse.toString());
        return org.apache.iceberg.CatalogUtil.buildIcebergCatalog(
                CATALOG_NAME, props, new org.apache.hadoop.conf.Configuration());
    }

    private void createNamespace() {
        if (catalog instanceof SupportsNamespaces nsCatalog) {
            nsCatalog.createNamespace(Namespace.of(NAMESPACE));
        }
    }

    private Table createV3Table(String tableName) {
        return createTable(tableName, "3");
    }

    private Table createTable(String tableName, String formatVersion) {
        TableIdentifier tableId = TableIdentifier.of(NAMESPACE, tableName);
        return catalog.createTable(tableId, TEST_SCHEMA,
                PartitionSpec.unpartitioned(),
                Map.of("format-version", formatVersion));
    }

    private void writeDataFile(Table table, int rowCount) {
        String path = table.location() + "/data/" + UUID.randomUUID() + ".parquet";
        OutputFile outputFile = table.io().newOutputFile(path);

        try {
            DataWriter<Record> writer = Parquet.writeData(outputFile)
                    .schema(table.schema())
                    .createWriterFunc(GenericParquetWriter::create)
                    .overwrite()
                    .withSpec(PartitionSpec.unpartitioned())
                    .build();

            for (int i = 0; i < rowCount; i++) {
                GenericRecord record = GenericRecord.create(table.schema());
                record.setField("id", (long) i);
                record.setField("name", "row_" + i);
                writer.write(record);
            }
            writer.close();
            table.newAppend().appendFile(writer.toDataFile()).commit();
        } catch (IOException e) {
            throw new java.io.UncheckedIOException("Failed to write data file", e);
        }
    }

    private String findDataFilePath(Table table) {
        table.refresh();
        List<FileScanTask> tasks = new ArrayList<>();
        try (CloseableIterable<FileScanTask> scanTasks = table.newScan().planFiles()) {
            scanTasks.forEach(tasks::add);
        } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
        assertThat(tasks).isNotEmpty();
        return tasks.get(0).file().path().toString();
    }

    private List<String> findAllDataFilePaths(Table table) {
        table.refresh();
        List<String> paths = new ArrayList<>();
        try (CloseableIterable<FileScanTask> scanTasks = table.newScan().planFiles()) {
            for (FileScanTask task : scanTasks) {
                paths.add(task.file().path().toString());
            }
        } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
        return paths;
    }

    private void commitDeletes(Table table, List<DeleteFile> deleteFiles) {
        var rowDelta = table.newRowDelta();
        deleteFiles.forEach(rowDelta::addDeletes);
        rowDelta.commit();
    }

    private int countDeleteFiles(Table table) {
        table.refresh();
        int count = 0;
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                count += task.deletes().size();
            }
        } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
        return count;
    }

    private List<Record> readAllRows(Table table) {
        table.refresh();
        return readRowsAtSnapshot(table, table.currentSnapshot().snapshotId());
    }

    private List<Record> readRowsAtSnapshot(Table table, long snapshot) {
        List<Record> allRows = new ArrayList<>();
        Schema schema = table.schema();

        try (CloseableIterable<FileScanTask> tasks = table.newScan().useSnapshot(snapshot).planFiles()) {
            for (FileScanTask task : tasks) {
                try (CloseableIterable<Record> reader = Parquet
                        .read(table.io().newInputFile(task.file().path().toString()))
                        .project(schema)
                        .createReaderFunc(fs ->
                                GenericParquetReaders.buildReader(schema, fs))
                        .build()) {
                    reader.forEach(allRows::add);
                }
            }
        } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
        return allRows;
    }
}
