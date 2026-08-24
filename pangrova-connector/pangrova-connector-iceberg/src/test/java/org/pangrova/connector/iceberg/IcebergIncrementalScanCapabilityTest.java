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
 * <p>Note: Iceberg snapshot IDs are random longs, not sequential.
 * Tests that need {@code from < to} numerically create snapshots
 * in a loop until the ordering condition is met.</p>
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
     * Keeps creating tables until snapshot IDs satisfy from < to.
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
     * Keeps creating tables until snapshot IDs satisfy from < to.
     */
    @Test
    void isAppendOnly_withDeleteFiles_returnsFalse() {
        SnapshotPair pair = createTableWithDeletes("with_deletes");
        assertThat(capability.isAppendOnly(
                "db." + pair.tableName, pair.fromSnapshotId, pair.toSnapshotId))
                .isFalse();
    }

    @Test
    void isAppendOnly_fromGreaterThanTo_throwsIllegalArgument() {
        assertThatThrownBy(() -> capability.isAppendOnly("db.any_table", 100L, 50L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not be greater than");
    }

    @Test
    void isAppendOnly_sameSnapshot_returnsTrue() {
        assertThat(capability.isAppendOnly("db.any_table", 42L, 42L)).isTrue();
    }

    // -- snapshot pair helpers --

    /**
     * Creates an append-only table, retrying with different table names
     * until the first snapshot ID is numerically less than the second.
     * Iceberg snapshot IDs are random, so this is necessary to satisfy
     * the SPI contract {@code fromSnapshotId < toSnapshotId}.
     */
    private SnapshotPair createAppendOnlyTable(String baseName) {
        for (int attempt = 0; attempt < 50; attempt++) {
            String tableName = baseName + "_" + attempt;
            Table table = createTable("db", tableName);
            appendDataFile(table, "/data/file1.parquet", 1024L);
            long snap1 = table.currentSnapshot().snapshotId();

            appendDataFile(table, "/data/file2.parquet", 2048L);
            long snap2 = table.currentSnapshot().snapshotId();

            if (snap1 < snap2) {
                return new SnapshotPair(tableName, snap1, snap2);
            }
        }
        throw new AssertionError(
                "Could not create table with from < to after 50 attempts");
    }

    /**
     * Creates a table with append + delete, retrying until snapshot IDs
     * satisfy the numeric ordering contract.
     */
    private SnapshotPair createTableWithDeletes(String baseName) {
        for (int attempt = 0; attempt < 50; attempt++) {
            String tableName = baseName + "_" + attempt;
            Table table = createTable("db", tableName);
            appendDataFile(table, "/data/file1.parquet", 1024L);
            long snap1 = table.currentSnapshot().snapshotId();

            DeleteFile deleteFile = FileMetadata.deleteFileBuilder(table.spec())
                    .ofPositionDeletes()
                    .withPath("/data/delete1.parquet")
                    .withFileSizeInBytes(128L)
                    .withRecordCount(5)
                    .withFormat(FileFormat.PARQUET)
                    .build();
            table.newRowDelta().addDeletes(deleteFile).commit();
            long snap2 = table.currentSnapshot().snapshotId();

            if (snap1 < snap2) {
                return new SnapshotPair(tableName, snap1, snap2);
            }
        }
        throw new AssertionError(
                "Could not create table with from < to after 50 attempts");
    }

    private record SnapshotPair(String tableName, long fromSnapshotId, long toSnapshotId) {}

    // -- catalog helpers --

    private Table createTable(String namespace, String tableName) {
        createNamespace(namespace);
        TableIdentifier id = TableIdentifier.of(namespace, tableName);
        return catalog.createTable(id, TEST_SCHEMA);
    }

    private void appendDataFile(Table table, String filePath, long fileSizeBytes) {
        DataFile dataFile = DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath(filePath)
                .withFileSizeInBytes(fileSizeBytes)
                .withRecordCount(100)
                .withFormat(FileFormat.PARQUET)
                .build();
        table.newAppend().appendFile(dataFile).commit();
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
