package org.pangrova.connector.iceberg.write;

import org.pangrova.connector.iceberg.HadoopTestGuard;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
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
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinReader;
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
 * Integration tests for Iceberg v3 Deletion Vector (DV) write path.
 * Validates that {@link IcebergDeleteWriter#buildDeletionVectorFile} correctly
 * generates Puffin files containing RoaringBitmap deletion vectors.
 *
 * <p>Uses real HadoopCatalog on local filesystem (no mocks per pangrova-rules §12).
 */
class IcebergDeletionVectorWriterTest {

    @TempDir
    Path warehouse;

    private Catalog catalog;

    private static final String CATALOG_NAME = "dv_writer_catalog";
    private static final String NAMESPACE = "dv_writer_db";

    private static final Schema TEST_SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()));

    @BeforeEach
    void setUp() {
        HadoopTestGuard.assumeHadoopCompatible();
        catalog = buildHadoopCatalog();
        createNamespace();
    }

    @Test
    void buildDeletionVectorFile_createsPuffinFormatFile() throws IOException {
        Table table = createV3Table("puffin_format_test");
        String dataFilePath = writeDataFile(table, 10);

        List<DeleteFile> deleteFiles = IcebergDeleteWriter.buildDeletionVectorFile(
                table, dataFilePath, List.of(0L, 2L, 4L));

        assertThat(deleteFiles).isNotEmpty();
        DeleteFile dvFile = deleteFiles.get(0);

        // Verify the file is Puffin format
        assertThat(dvFile.format()).isEqualTo(FileFormat.PUFFIN);

        // Verify it is a deletion vector content type
        assertThat(dvFile.content()).isEqualTo(FileContent.POSITION_DELETES);

        // Verify the Puffin file exists and is readable
        assertPuffinFileReadable(table, dvFile.path().toString());
    }

    @Test
    void buildDeletionVectorFile_positionsEncodedCorrectly() {
        Table table = createV3Table("positions_test");
        String dataFilePath = writeDataFile(table, 100);

        List<Long> positions = List.of(0L, 5L, 10L, 50L, 99L);
        List<DeleteFile> deleteFiles = IcebergDeleteWriter.buildDeletionVectorFile(
                table, dataFilePath, positions);

        assertThat(deleteFiles).isNotEmpty();
        DeleteFile dvFile = deleteFiles.get(0);

        // The DV file should reference the correct number of deleted positions
        assertThat(dvFile.recordCount()).isEqualTo(positions.size());
    }

    @Test
    void buildDeletionVectorFile_roundTrip_positionsMatchAfterCommitAndScan() {
        Table table = createV3Table("round_trip_test");
        String dataFilePath = writeDataFile(table, 20);

        List<Long> positions = List.of(1L, 3L, 7L, 15L, 19L);
        List<DeleteFile> deleteFiles = IcebergDeleteWriter.buildDeletionVectorFile(
                table, dataFilePath, positions);

        assertThat(deleteFiles).isNotEmpty();

        // Commit the DV to the table
        var rowDelta = table.newRowDelta();
        for (DeleteFile df : deleteFiles) {
            rowDelta.addDeletes(df);
        }
        rowDelta.commit();

        // Verify the table now has delete files in its scan tasks
        table.refresh();
        List<org.apache.iceberg.FileScanTask> scanTasks = new ArrayList<>();
        table.newScan().planFiles().forEach(scanTasks::add);

        assertThat(scanTasks).hasSize(1);
        List<org.apache.iceberg.DeleteFile> taskDeletes = new ArrayList<>(scanTasks.get(0).deletes());
        assertThat(taskDeletes).isNotEmpty();

        // Verify the delete file is Puffin format
        org.apache.iceberg.DeleteFile committedDv = taskDeletes.get(0);
        assertThat(committedDv.format()).isEqualTo(FileFormat.PUFFIN);
        assertThat(committedDv.recordCount()).isEqualTo(positions.size());
    }

    @Test
    void buildDeletionVectorFile_singlePosition() {
        Table table = createV3Table("single_pos_test");
        String dataFilePath = writeDataFile(table, 5);

        List<DeleteFile> deleteFiles = IcebergDeleteWriter.buildDeletionVectorFile(
                table, dataFilePath, List.of(2L));

        assertThat(deleteFiles).isNotEmpty();
        assertThat(deleteFiles.get(0).recordCount()).isEqualTo(1);
        assertThat(deleteFiles.get(0).format()).isEqualTo(FileFormat.PUFFIN);
    }

    @Test
    void buildDeletionVectorFile_largePositionSet() {
        Table table = createV3Table("large_pos_test");
        String dataFilePath = writeDataFile(table, 10_000);

        List<Long> positions = new ArrayList<>();
        for (long i = 0; i < 10_000; i += 2) {
            positions.add(i);
        }

        List<DeleteFile> deleteFiles = IcebergDeleteWriter.buildDeletionVectorFile(
                table, dataFilePath, positions);

        assertThat(deleteFiles).isNotEmpty();
        assertThat(deleteFiles.get(0).recordCount()).isEqualTo(5_000);
        assertThat(deleteFiles.get(0).format()).isEqualTo(FileFormat.PUFFIN);
    }

    @Test
    void buildDeletionVectorFile_emptyPositions_throwsIllegalArgument() {
        Table table = createV3Table("empty_pos_test");
        String dataFilePath = writeDataFile(table, 5);

        assertThatThrownBy(() ->
                IcebergDeleteWriter.buildDeletionVectorFile(table, dataFilePath, List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("positions must not be empty");
    }

    @Test
    void buildDeletionVectorFile_dvFilePathContainsPuffinExtension() {
        Table table = createV3Table("path_ext_test");
        String dataFilePath = writeDataFile(table, 5);

        List<DeleteFile> deleteFiles = IcebergDeleteWriter.buildDeletionVectorFile(
                table, dataFilePath, List.of(0L, 1L));

        assertThat(deleteFiles).isNotEmpty();
        String dvPath = deleteFiles.get(0).path().toString();
        assertThat(dvPath).endsWith(".puffin");
    }

    // -- DV merge tests --

    @Test
    void mergeDeletionVectors_twoFragmentsSameFile_allPositionsPresent() {
        Table table = createV3Table("merge_two_fragments_test");
        String dataFilePath = writeDataFile(table, 20);

        List<Long> fragment1 = List.of(1L, 3L, 5L);
        List<Long> fragment2 = List.of(7L, 9L, 11L);

        List<DeleteFile> mergedDvFiles = IcebergDeleteWriter.mergeDeletionVectors(
                table, dataFilePath, List.of(fragment1, fragment2));

        assertThat(mergedDvFiles).isNotEmpty();
        DeleteFile mergedDv = mergedDvFiles.get(0);
        assertThat(mergedDv.format()).isEqualTo(FileFormat.PUFFIN);
        // Total positions = 3 + 3 = 6
        assertThat(mergedDv.recordCount()).isEqualTo(6);
    }

    @Test
    void mergeDeletionVectors_overlappingPositions_deduplicatedViaBitmapOr() {
        Table table = createV3Table("merge_overlap_test");
        String dataFilePath = writeDataFile(table, 20);

        // Overlapping positions: 3 and 5 appear in both fragments
        List<Long> fragment1 = List.of(1L, 3L, 5L);
        List<Long> fragment2 = List.of(3L, 5L, 7L);

        List<DeleteFile> mergedDvFiles = IcebergDeleteWriter.mergeDeletionVectors(
                table, dataFilePath, List.of(fragment1, fragment2));

        assertThat(mergedDvFiles).isNotEmpty();
        DeleteFile mergedDv = mergedDvFiles.get(0);
        // Unique positions: {1, 3, 5, 7} = 4 (bitmap OR deduplicates)
        assertThat(mergedDv.recordCount()).isEqualTo(4);
    }

    @Test
    void mergeDeletionVectors_commitViaRowDelta_scanShowsMergedDv() {
        Table table = createV3Table("merge_commit_test");
        String dataFilePath = writeDataFile(table, 20);

        List<Long> fragment1 = List.of(0L, 2L, 4L);
        List<Long> fragment2 = List.of(6L, 8L, 10L);

        List<DeleteFile> mergedDvFiles = IcebergDeleteWriter.mergeDeletionVectors(
                table, dataFilePath, List.of(fragment1, fragment2));

        // Commit the merged DV
        var rowDelta = table.newRowDelta();
        for (DeleteFile df : mergedDvFiles) {
            rowDelta.addDeletes(df);
        }
        rowDelta.commit();

        // Verify via scan
        table.refresh();
        List<org.apache.iceberg.FileScanTask> scanTasks = new ArrayList<>();
        table.newScan().planFiles().forEach(scanTasks::add);

        assertThat(scanTasks).hasSize(1);
        List<org.apache.iceberg.DeleteFile> taskDeletes =
                new ArrayList<>(scanTasks.get(0).deletes());
        assertThat(taskDeletes).hasSize(1);

        org.apache.iceberg.DeleteFile committedDv = taskDeletes.get(0);
        assertThat(committedDv.format()).isEqualTo(FileFormat.PUFFIN);
        assertThat(committedDv.recordCount()).isEqualTo(6);
    }

    @Test
    void mergeDeletionVectors_threeFragments_allMergedCorrectly() {
        Table table = createV3Table("merge_three_fragments_test");
        String dataFilePath = writeDataFile(table, 30);

        List<Long> fragment1 = List.of(0L, 1L, 2L);
        List<Long> fragment2 = List.of(10L, 11L, 12L);
        List<Long> fragment3 = List.of(20L, 21L, 22L);

        List<DeleteFile> mergedDvFiles = IcebergDeleteWriter.mergeDeletionVectors(
                table, dataFilePath, List.of(fragment1, fragment2, fragment3));

        assertThat(mergedDvFiles).isNotEmpty();
        assertThat(mergedDvFiles.get(0).recordCount()).isEqualTo(9);
    }

    @Test
    void mergeDeletionVectors_emptyPositionSets_throwsIllegalArgument() {
        Table table = createV3Table("merge_empty_sets_test");

        assertThatThrownBy(() ->
                IcebergDeleteWriter.mergeDeletionVectors(
                        table, "fake/path", List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("positionSets must not be empty");
    }

    @Test
    void mergeDeletionVectors_emptyInnerSet_throwsIllegalArgument() {
        Table table = createV3Table("merge_empty_inner_test");

        assertThatThrownBy(() ->
                IcebergDeleteWriter.mergeDeletionVectors(
                        table, "fake/path",
                        List.of(List.of(1L), List.of())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Each position set must be non-empty");
    }

    @Test
    void twoSeparateDvCommits_sameDataFile_requiresMerge() {
        Table table = createV3Table("two_commits_test");
        String dataFilePath = writeDataFile(table, 20);

        // First DELETE: positions 1, 3, 5
        List<DeleteFile> dv1 = IcebergDeleteWriter.buildDeletionVectorFile(
                table, dataFilePath, List.of(1L, 3L, 5L));
        var rowDelta1 = table.newRowDelta();
        for (DeleteFile df : dv1) {
            rowDelta1.addDeletes(df);
        }
        rowDelta1.commit();

        // Second DELETE targets the same data file.
        // Iceberg v3 enforces at most one DV per data file, so we must
        // merge the existing positions with the new ones, then use
        // RewriteFiles to atomically replace the old DV with the merged one.
        table.refresh();
        List<Long> secondDeletePositions = List.of(7L, 9L, 11L);
        List<DeleteFile> mergedDv = IcebergDeleteWriter.mergeDeletionVectors(
                table, dataFilePath,
                List.of(List.of(1L, 3L, 5L), secondDeletePositions));

        // Replace old DV with merged DV using RewriteFiles
        var rewrite = table.newRewrite();
        rewrite.rewriteFiles(
                java.util.Set.of(),
                java.util.Set.copyOf(dv1),
                java.util.Set.of(),
                java.util.Set.copyOf(mergedDv));
        rewrite.commit();

        // Verify the merged DV contains all 6 positions
        table.refresh();
        List<org.apache.iceberg.FileScanTask> scanTasks = new ArrayList<>();
        table.newScan().planFiles().forEach(scanTasks::add);

        assertThat(scanTasks).hasSize(1);
        List<org.apache.iceberg.DeleteFile> taskDeletes =
                new ArrayList<>(scanTasks.get(0).deletes());
        assertThat(taskDeletes).hasSize(1);

        org.apache.iceberg.DeleteFile committedDv = taskDeletes.get(0);
        assertThat(committedDv.format()).isEqualTo(FileFormat.PUFFIN);
        // All 6 unique positions from both deletes
        assertThat(committedDv.recordCount()).isEqualTo(6);
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
        TableIdentifier tableId = TableIdentifier.of(NAMESPACE, tableName);
        return catalog.createTable(tableId, TEST_SCHEMA,
                PartitionSpec.unpartitioned(),
                Map.of("format-version", "3"));
    }

    /**
     * Writes a data file with the given number of rows and commits it.
     * Returns the data file path for use in DV tests.
     */
    private String writeDataFile(Table table, int rowCount) {
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
            return path;
        } catch (IOException e) {
            throw new java.io.UncheckedIOException("Failed to write data file", e);
        }
    }

    /**
     * Verifies that the Puffin file at the given path is readable.
     */
    private void assertPuffinFileReadable(Table table, String puffinPath) throws IOException {
        try (PuffinReader reader = Puffin.read(table.io().newInputFile(puffinPath)).build()) {
            var fileMetadata = reader.fileMetadata();
            assertThat(fileMetadata.blobs()).isNotEmpty();
        }
    }
}
