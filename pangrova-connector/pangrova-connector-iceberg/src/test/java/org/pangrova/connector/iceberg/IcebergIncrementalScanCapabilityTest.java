package org.pangrova.connector.iceberg;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration test for {@link IcebergIncrementalScanCapability}
 * using real HadoopCatalog.
 *
 * <p>Snapshot IDs deliberately decrease while real SDK commits advance their
 * parent chain. No retry loop or random ordering precondition is used.</p>
 */
class IcebergIncrementalScanCapabilityTest {

    @TempDir
    Path warehouse;

    private Catalog catalog;
    private IcebergIncrementalScanCapability capability;

    private static final Schema TEST_SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()));

    @BeforeEach
    void setUp() {
        HadoopTestGuard.assumeHadoopCompatible();
        catalog = createHadoopCatalog();
        capability = new IcebergIncrementalScanCapability(catalog);
    }

    @Test
    void supportsIncrementalAppendScan_returnsTrue() {
        assertThat(capability.supportsIncrementalAppendScan()).isTrue();
    }

    @Test
    void supportsChangelogScan_returnsFalse() {
        assertThat(capability.supportsChangelogScan()).isFalse();
    }

    /**
     * Append-only range: two consecutive append commits, no deletes.
     */
    @Test
    void isAppendOnly_appendOnlyRange_returnsTrue() {
        SnapshotPair pair = createAppendOnlyTable("append_only");
        assertThat(capability.isAppendOnly(
                "db." + pair.tableName, pair.fromSnapshotId, pair.toSnapshotId))
                .isTrue();
    }

    /**
     * Range with delete files: append data then add position deletes.
     */
    @Test
    void isAppendOnly_withDeleteFiles_returnsFalse() {
        SnapshotPair pair = createTableWithDeletes("with_deletes");
        assertThat(capability.isAppendOnly(
                "db." + pair.tableName, pair.fromSnapshotId, pair.toSnapshotId))
                .isFalse();
    }

    @Test
    void isAppendOnly_reversedAncestryIsRejectedRegardlessOfNumericOrder() {
        SnapshotPair pair = createAppendOnlyTable("backwards");
        assertThat(pair.fromSnapshotId).isGreaterThan(pair.toSnapshotId);
        assertThatThrownBy(() -> capability.isAppendOnly(
                "db." + pair.tableName, pair.toSnapshotId, pair.fromSnapshotId))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("not an ancestor");
    }

    @Test
    void isAppendOnly_sameExistingSnapshotReturnsTrueButMissingSnapshotFails() {
        SnapshotPair pair = createAppendOnlyTable("same_snapshot");
        assertThat(capability.isAppendOnly("db." + pair.tableName,
                pair.fromSnapshotId, pair.fromSnapshotId)).isTrue();
        assertThatThrownBy(() -> capability.isAppendOnly("db." + pair.tableName, 42, 42))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("not found");
    }

    @Test
    void overwriteWithoutDeleteManifestsIsNotAppendOnly() {
        Table table = createTable("db", "overwrite");
        DataFile original = dataFile("/data/original.parquet", 1024);
        table.newAppend().appendFile(original).commit();
        long from = table.currentSnapshot().snapshotId();
        table.newOverwrite().deleteFile(original).addFile(dataFile("/data/replacement.parquet", 2048)).commit();
        long to = table.currentSnapshot().snapshotId();
        assertThat(table.currentSnapshot().deleteManifests(table.io())).isEmpty();
        assertThat(capability.isAppendOnly("db.overwrite", from, to)).isFalse();
    }

    private SnapshotPair createAppendOnlyTable(String tableName) {
        Table table = createTable("db", tableName);
        appendDataFile(table, "/data/file1.parquet", 1024L);
        long from = table.currentSnapshot().snapshotId();
        appendDataFile(table, "/data/file2.parquet", 2048L);
        long to = table.currentSnapshot().snapshotId();
        assertThat(from).isGreaterThan(to);
        return new SnapshotPair(tableName, from, to);
    }

    private SnapshotPair createTableWithDeletes(String tableName) {
        Table table = createTable("db", tableName);
        appendDataFile(table, "/data/file1.parquet", 1024L);
        long from = table.currentSnapshot().snapshotId();
        DeleteFile deleteFile = FileMetadata.deleteFileBuilder(table.spec())
                .ofPositionDeletes().withPath("/data/delete1.parquet").withFileSizeInBytes(128L)
                .withRecordCount(5).withFormat(FileFormat.PARQUET).build();
        table.newRowDelta().addDeletes(deleteFile).commit();
        long to = table.currentSnapshot().snapshotId();
        assertThat(from).isGreaterThan(to);
        return new SnapshotPair(tableName, from, to);
    }

    private record SnapshotPair(String tableName, long fromSnapshotId, long toSnapshotId) {}

    // -- catalog helpers --

    private Table createTable(String namespace, String tableName) {
        createNamespace(namespace);
        TableIdentifier id = TableIdentifier.of(namespace, tableName);
        return DeterministicSnapshotTable.descendingIds(catalog.createTable(id, TEST_SCHEMA));
    }

    private void appendDataFile(Table table, String filePath, long fileSizeBytes) {
        table.newAppend().appendFile(dataFile(filePath, fileSizeBytes)).commit();
    }

    private DataFile dataFile(String filePath, long fileSizeBytes) {
        return DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath(filePath)
                .withFileSizeInBytes(fileSizeBytes)
                .withRecordCount(100)
                .withFormat(FileFormat.PARQUET)
                .build();
    }

    private Catalog createHadoopCatalog() {
        Map<String, String> props = Map.of(
                "type", "hadoop",
                "warehouse", warehouse.toString());
        org.apache.hadoop.conf.Configuration hadoopConf =
                new org.apache.hadoop.conf.Configuration();
        return org.apache.iceberg.CatalogUtil.buildIcebergCatalog(
                "test_catalog", props, hadoopConf);
    }

    private void createNamespace(String name) {
        if (catalog instanceof SupportsNamespaces nsCatalog) {
            try {
                nsCatalog.createNamespace(Namespace.of(name));
            } catch (org.apache.iceberg.exceptions.AlreadyExistsException ignored) {
                // namespace already exists
            }
        }
    }
}
