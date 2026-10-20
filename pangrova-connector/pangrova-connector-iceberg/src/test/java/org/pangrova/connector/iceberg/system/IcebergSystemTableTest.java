package org.pangrova.connector.iceberg.system;

import org.pangrova.connector.iceberg.HadoopTestGuard;
import org.pangrova.spi.connector.data.TypedRow;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for Iceberg system tables using a real HadoopCatalog.
 * Validates $snapshots, $files, $history, and $refs system tables return
 * correct metadata after data is committed to a real Iceberg table.
 */
class IcebergSystemTableTest {

    @TempDir
    static Path warehouse;

    private static Table testTable;

    private static final Schema TEST_SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()));

    @BeforeAll
    static void setUp() {
        HadoopTestGuard.assumeHadoopCompatible();
        Catalog catalog = createCatalog();
        if (catalog instanceof SupportsNamespaces nsCatalog) {
            nsCatalog.createNamespace(Namespace.of("db"));
        }
        testTable = catalog.createTable(TableIdentifier.of("db", "test_table"), TEST_SCHEMA);
        appendSyntheticDataFile(testTable, "data-001.parquet", 100, 4096);
    }

    @Test
    void snapshotsSystemTable_returnsSnapshotAfterCommit() {
        IcebergSnapshotsSystemTable systemTable = new IcebergSnapshotsSystemTable(testTable);

        assertThat(systemTable.columns()).containsExactly(
                "snapshot_id", "parent_id", "timestamp_ms", "operation", "summary");

        List<TypedRow> rows = systemTable.rows();
        assertThat(rows).hasSize(1);

        TypedRow first = rows.get(0);
        assertThat(first.getLong("snapshot_id")).isNotNull();
        assertThat(first.getLong("timestamp_ms")).isGreaterThan(0);
        assertThat(first.getString("operation")).isEqualTo("append");
    }

    @Test
    void snapshotsSystemTable_emptyTableReturnsNoRows() {
        Catalog catalog = createCatalog();
        if (catalog instanceof SupportsNamespaces nsCatalog) {
            try { nsCatalog.createNamespace(Namespace.of("empty_db")); } catch (Exception ignored) {}
        }
        Table emptyTable = catalog.createTable(
                TableIdentifier.of("empty_db", "empty_table"), TEST_SCHEMA);

        IcebergSnapshotsSystemTable systemTable = new IcebergSnapshotsSystemTable(emptyTable);
        assertThat(systemTable.rows()).isEmpty();
    }

    @Test
    void filesSystemTable_returnsDataFileAfterCommit() {
        IcebergFilesSystemTable systemTable = new IcebergFilesSystemTable(testTable);

        assertThat(systemTable.columns()).containsExactly(
                "content", "file_path", "file_format", "record_count", "file_size_in_bytes");

        List<TypedRow> rows = systemTable.rows();
        assertThat(rows).hasSize(1);

        TypedRow first = rows.get(0);
        assertThat(first.getString("content")).isEqualTo("DATA");
        assertThat(first.getString("file_path")).contains("data-001.parquet");
        assertThat(first.getString("file_format")).isEqualTo("PARQUET");
        assertThat(first.getLong("record_count")).isEqualTo(100);
        assertThat(first.getLong("file_size_in_bytes")).isEqualTo(4096);
    }

    @Test
    void historySystemTable_returnsHistoryAfterCommit() {
        IcebergHistorySystemTable systemTable = new IcebergHistorySystemTable(testTable);

        assertThat(systemTable.columns()).containsExactly(
                "made_current_at", "snapshot_id", "parent_id", "is_current_ancestor");

        List<TypedRow> rows = systemTable.rows();
        assertThat(rows).hasSize(1);

        TypedRow first = rows.get(0);
        assertThat(first.getLong("snapshot_id")).isEqualTo(testTable.currentSnapshot().snapshotId());
        assertThat(first.getLong("made_current_at")).isGreaterThan(0);
    }

    @Test
    void refsSystemTable_returnsMainBranch() {
        IcebergRefsSystemTable systemTable = new IcebergRefsSystemTable(testTable);

        assertThat(systemTable.columns()).containsExactly(
                "name", "type", "snapshot_id", "max_reference_age_in_ms",
                "min_snapshots_to_keep", "max_snapshot_age_in_ms");

        List<TypedRow> rows = systemTable.rows();
        assertThat(rows).isNotEmpty();

        boolean hasMain = rows.stream()
                .anyMatch(r -> "main".equals(r.getString("name"))
                        && "BRANCH".equals(r.getString("type")));
        assertThat(hasMain).isTrue();
    }

    private static Catalog createCatalog() {
        Map<String, String> props = Map.of(
                "type", "hadoop",
                "warehouse", warehouse.toString());
        org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        return org.apache.iceberg.CatalogUtil.buildIcebergCatalog("test_catalog", props, hadoopConf);
    }

    private static void appendSyntheticDataFile(Table table, String fileName,
                                                 long recordCount, long fileSizeBytes) {
        DataFile dataFile = DataFiles.builder(table.spec())
                .withPath(table.location() + "/data/" + fileName)
                .withFormat(FileFormat.PARQUET)
                .withRecordCount(recordCount)
                .withFileSizeInBytes(fileSizeBytes)
                .build();
        table.newAppend().appendFile(dataFile).commit();
    }
}
