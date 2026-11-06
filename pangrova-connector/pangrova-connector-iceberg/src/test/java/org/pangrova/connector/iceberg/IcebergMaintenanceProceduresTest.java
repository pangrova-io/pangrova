package org.pangrova.connector.iceberg;

import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.error.ConnectorException;
import org.pangrova.types.value.TypedRow;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for G-CONN-07: Iceberg maintenance procedures.
 *
 * <p>Verifies that {@link IcebergMaintenanceProcedures} dispatches each procedure
 * to the matching Iceberg SDK API and returns a structured result row.
 * Uses real {@code HadoopCatalog} on the local filesystem (no mocks per
 * pangrova-rules §12).</p>
 */
class IcebergMaintenanceProceduresTest {

    @TempDir
    Path warehouse;

    private Catalog catalog;
    private IcebergMetadata metadata;
    private IcebergMaintenanceProcedures procedures;

    private static final String CATALOG_NAME = "maintenance_catalog";
    private static final String NAMESPACE = "maint_db";

    private static final ConnectorSession SESSION = new ConnectorSession() {
        @Override
        public java.util.Optional<org.pangrova.connector.api.query.ConnectorQueryAuthority> getQueryAuthority() {
            return java.util.Optional.empty();
        }

        @Override public String getUser() { return "test"; }
        @Override public Optional<String> getCatalog() { return Optional.of(CATALOG_NAME); }
        @Override public Optional<String> getSchema() { return Optional.empty(); }
        @Override public Map<String, String> getProperties() { return Map.of(); }
    };

    private static final Schema TEST_SCHEMA = new Schema(
            Types.NestedField.optional(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()));

    /**
     * Lazy catalog initialization so argument-validation tests that don't touch
     * the catalog can run under JDK 26 (HadoopCatalog uses
     * {@code UserGroupInformation.getSubject} which throws on JDK 26 / JEP 486).
     */
    private void initCatalog() {
        HadoopTestGuard.assumeHadoopCompatible();
        catalog = buildHadoopCatalog();
        if (catalog instanceof SupportsNamespaces nsCatalog) {
            nsCatalog.createNamespace(Namespace.of(NAMESPACE));
        }
        metadata = new IcebergMetadata(catalog, CATALOG_NAME);
        procedures = new IcebergMaintenanceProcedures(catalog);
    }

    /**
     * Procedures-only handle that does not require a real catalog. Used by the
     * argument-validation tests that fail before any catalog interaction.
     */
    private IcebergMaintenanceProcedures proceduresWithoutCatalog() {
        return new IcebergMaintenanceProcedures(/*unused*/ null);
    }

    // -- compact ---------------------------------------------------------------

    @Test
    void compact_emptyTable_returnsZeroMetrics() {
        initCatalog();
        createTable("compact_empty");
        List<TypedRow> result = procedures.execute("compact",
                Map.of("arg0", NAMESPACE + ".compact_empty"));
        assertThat(result).hasSize(1);
        TypedRow row = result.get(0);
        assertThat(row.getString("result")).isEqualTo("compact succeeded");
        assertThat(row.getString("table")).isEqualTo(NAMESPACE + ".compact_empty");
        assertThat(row.getLong("files_compacted")).isZero();
        assertThat(row.getLong("files_produced")).isZero();
        assertThat(row.getLong("delete_files_removed")).isZero();
        assertThat(row.getLong("duration_ms")).isGreaterThanOrEqualTo(0L);
    }

    // -- expire_snapshots ------------------------------------------------------

    @Test
    void expireSnapshots_retainsLastSnapshotsAndCleansFiles() {
        initCatalog();
        Table table = createTable("expire_test");
        // create 3 snapshots
        writeDataFile(table, 5);
        writeDataFile(table, 5);
        writeDataFile(table, 5);
        table.refresh();
        assertThat(countSnapshots(table)).isEqualTo(3);

        List<TypedRow> result = procedures.execute("expire_snapshots",
                Map.of("arg0", NAMESPACE + ".expire_test",
                        "arg1", "1",
                        "arg2", String.valueOf(System.currentTimeMillis() + 60_000)));
        TypedRow row = result.get(0);
        assertThat(row.getString("result")).isEqualTo("expire_snapshots succeeded");
        assertThat(row.getLong("expired_snapshots")).isGreaterThanOrEqualTo(2L);
        assertThat(row.getLong("retain_last")).isEqualTo(1L);
        // After expiry the table holds only the retained snapshot(s)
        Table refreshed = catalog.loadTable(TableIdentifier.of(NAMESPACE, "expire_test"));
        assertThat(countSnapshots(refreshed)).isEqualTo(1);
    }

    @Test
    void expireSnapshots_defaultsRetainLastWhenNotProvided() {
        initCatalog();
        Table table = createTable("expire_default");
        writeDataFile(table, 5);
        writeDataFile(table, 5);
        table.refresh();
        assertThat(countSnapshots(table)).isEqualTo(2);

        // The default retains a recovery-safe window, so two snapshots are never expired.
        List<TypedRow> result = procedures.execute("expire_snapshots",
                Map.of("arg0", NAMESPACE + ".expire_default",
                        "arg2", String.valueOf(System.currentTimeMillis() + 60_000)));
        TypedRow row = result.get(0);
        assertThat(row.getString("result")).isEqualTo("expire_snapshots succeeded");
        assertThat(row.getLong("retain_last")).isEqualTo(10L);
        assertThat(row.getLong("expired_snapshots")).isZero();
        Table refreshed = catalog.loadTable(TableIdentifier.of(NAMESPACE, "expire_default"));
        assertThat(countSnapshots(refreshed)).isEqualTo(2);
    }

    @Test
    void expireSnapshots_dryRunReportsCandidatesWithoutCommitting() {
        initCatalog();
        Table table = createTable("expire_dry");
        writeDataFile(table, 5);
        writeDataFile(table, 5);
        writeDataFile(table, 5);
        table.refresh();
        assertThat(countSnapshots(table)).isEqualTo(3);

        List<TypedRow> result = procedures.execute("expire_snapshots",
                Map.of("arg0", NAMESPACE + ".expire_dry",
                        "arg1", "1",
                        "arg2", String.valueOf(System.currentTimeMillis() + 60_000),
                        "arg3", "true"));
        TypedRow row = result.get(0);
        assertThat(row.getString("result")).isEqualTo("expire_snapshots dry run");
        assertThat(row.getString("dry_run")).isEqualTo("true");
        assertThat(row.getLong("expired_snapshots")).isEqualTo(2L);
        Table refreshed = catalog.loadTable(TableIdentifier.of(NAMESPACE, "expire_dry"));
        assertThat(countSnapshots(refreshed)).isEqualTo(3);
    }

    @Test
    void expireSnapshots_rejectsNonPositiveRetainLastBeforeTouchingTheCatalog() {
        IcebergMaintenanceProcedures p = proceduresWithoutCatalog();
        assertThatThrownBy(() -> p.execute("expire_snapshots",
                Map.of("arg0", NAMESPACE + ".x", "arg1", "0")))
                .isInstanceOf(ConnectorException.class)
                .hasMessageContaining("retain_last must be at least 1");
    }

    // -- remove_orphan_files ---------------------------------------------------

    @Test
    void removeOrphanFiles_deletesFilesNotReachableFromManifests() throws Exception {
        initCatalog();
        Table table = createTable("orphan_test");
        writeDataFile(table, 5);

        // Create an orphan file in the table location with a back-dated mtime so
        // it falls before the cutoff used by the procedure.
        Path orphanDir = Path.of(table.location().replaceFirst("^file:", ""), "data");
        Files.createDirectories(orphanDir);
        Path orphan = orphanDir.resolve("orphan-" + UUID.randomUUID() + ".parquet");
        Files.writeString(orphan, "garbage");
        Files.setLastModifiedTime(orphan,
                java.nio.file.attribute.FileTime.fromMillis(0L));
        long preCount = countFilesUnder(orphanDir);
        assertThat(Files.exists(orphan)).isTrue();

        List<TypedRow> result = procedures.execute("remove_orphan_files",
                Map.of("arg0", NAMESPACE + ".orphan_test",
                        "arg1", String.valueOf(System.currentTimeMillis() - 25L * 3_600_000L)));
        TypedRow row = result.get(0);
        assertThat(row.getString("result")).isEqualTo("remove_orphan_files succeeded");
        assertThat(row.getLong("scanned_file_count")).isGreaterThan(0L);
        assertThat(row.getLong("deleted_file_count")).isGreaterThanOrEqualTo(1L);
        assertThat(Files.exists(orphan)).isFalse();
        // Reachable data file is still there
        assertThat(countFilesUnder(orphanDir)).isLessThan(preCount);
    }

    @Test
    void removeOrphanFiles_doesNotDeleteRecentFiles() throws Exception {
        initCatalog();
        Table table = createTable("orphan_recent");
        writeDataFile(table, 5);

        Path orphanDir = Path.of(table.location().replaceFirst("^file:", ""), "data");
        Files.createDirectories(orphanDir);
        Path orphan = orphanDir.resolve("recent-orphan-" + UUID.randomUUID() + ".parquet");
        Files.writeString(orphan, "junk");
        // mtime = now, cutoff = now-25h, so orphan is "too new" to delete
        long olderThanMs = System.currentTimeMillis() - 25L * 3_600_000L;

        List<TypedRow> result = procedures.execute("remove_orphan_files",
                Map.of("arg0", NAMESPACE + ".orphan_recent",
                        "arg1", String.valueOf(olderThanMs)));
        TypedRow row = result.get(0);
        assertThat(row.getString("result")).isEqualTo("remove_orphan_files succeeded");
        assertThat(Files.exists(orphan)).isTrue(); // recent: not deleted
    }

    @Test
    void removeOrphanFiles_rejectsCutoffYoungerThanTheSafetyMinimum() {
        IcebergMaintenanceProcedures p = proceduresWithoutCatalog();
        assertThatThrownBy(() -> p.execute("remove_orphan_files",
                Map.of("arg0", NAMESPACE + ".orphan_too_new",
                        "arg1", String.valueOf(System.currentTimeMillis()))))
                .isInstanceOf(ConnectorException.class)
                .hasMessageContaining("at least 24 hours old");
    }

    @Test
    void removeOrphanFiles_dryRunReportsOrphansWithoutDeleting() throws Exception {
        initCatalog();
        Table table = createTable("orphan_dry");
        writeDataFile(table, 5);

        Path orphanDir = Path.of(table.location().replaceFirst("^file:", ""), "data");
        Files.createDirectories(orphanDir);
        Path orphan = orphanDir.resolve("dry-orphan-" + UUID.randomUUID() + ".parquet");
        Files.writeString(orphan, "garbage");
        Files.setLastModifiedTime(orphan,
                java.nio.file.attribute.FileTime.fromMillis(0L));

        List<TypedRow> result = procedures.execute("remove_orphan_files",
                Map.of("arg0", NAMESPACE + ".orphan_dry",
                        "arg1", String.valueOf(System.currentTimeMillis() - 25L * 3_600_000L),
                        "arg2", "true"));
        TypedRow row = result.get(0);
        assertThat(row.getString("result")).isEqualTo("remove_orphan_files dry run");
        assertThat(row.getLong("orphan_file_count")).isGreaterThanOrEqualTo(1L);
        assertThat(row.getLong("deleted_file_count")).isZero();
        assertThat(Files.exists(orphan)).isTrue();
    }

    // -- rewrite_manifests -----------------------------------------------------

    @Test
    void rewriteManifests_executesAndReportsManifestCounts() {
        initCatalog();
        Table table = createTable("rewrite_test");
        writeDataFile(table, 5);
        writeDataFile(table, 5);

        List<TypedRow> result = procedures.execute("rewrite_manifests",
                Map.of("arg0", NAMESPACE + ".rewrite_test"));
        TypedRow row = result.get(0);
        assertThat(row.getString("result")).isEqualTo("rewrite_manifests succeeded");
        assertThat(row.getLong("manifests_before")).isGreaterThan(0L);
        assertThat(row.getLong("manifests_after")).isGreaterThanOrEqualTo(0L);
    }

    // -- rollback_to_snapshot --------------------------------------------------

    @Test
    void rollbackToSnapshot_movesCurrentRefBack() {
        initCatalog();
        Table table = createTable("rollback_test");
        writeDataFile(table, 5);
        long firstSnapshotId = table.currentSnapshot().snapshotId();
        writeDataFile(table, 5);
        long secondSnapshotId = table.currentSnapshot().snapshotId();
        assertThat(firstSnapshotId).isNotEqualTo(secondSnapshotId);

        List<TypedRow> result = procedures.execute("rollback_to_snapshot",
                Map.of("arg0", NAMESPACE + ".rollback_test",
                        "arg1", String.valueOf(firstSnapshotId)));
        TypedRow row = result.get(0);
        assertThat(row.getString("result")).isEqualTo("rollback_to_snapshot succeeded");
        assertThat(row.getLong("snapshot_id")).isEqualTo(firstSnapshotId);

        Table refreshed = catalog.loadTable(TableIdentifier.of(NAMESPACE, "rollback_test"));
        assertThat(refreshed.currentSnapshot().snapshotId()).isEqualTo(firstSnapshotId);
    }

    // -- rollback_to_snapshot — argument-validation tests do not need a catalog
    //    (they fail before any catalog interaction), so they run under JDK 26.

    @Test
    void rollbackToSnapshot_missingSnapshotIdThrows() {
        // Argument validation runs before loadTable; a null catalog is fine here
        // because the failure path never reaches it.
        IcebergMaintenanceProcedures p = proceduresWithoutCatalog();
        assertThatThrownBy(() -> p.execute("rollback_to_snapshot",
                Map.of("arg0", NAMESPACE + ".rollback_missing")))
                .isInstanceOf(ConnectorException.class)
                .hasMessageContaining("snapshot_id");
    }

    @Test
    void rollbackToSnapshot_invalidSnapshotIdThrows() {
        IcebergMaintenanceProcedures p = proceduresWithoutCatalog();
        assertThatThrownBy(() -> p.execute("rollback_to_snapshot",
                Map.of("arg0", NAMESPACE + ".rollback_invalid",
                        "arg1", "not-a-number")))
                .isInstanceOf(ConnectorException.class)
                .hasMessageContaining("snapshot_id");
    }

    // -- argument validation ---------------------------------------------------
    // These tests exercise pure argument validation and do not require a catalog,
    // so they run successfully under both JDK 17 (CI) and JDK 26 (local dev).

    @Test
    void execute_unknownProcedure_throwsUnsupported() {
        IcebergMaintenanceProcedures p = proceduresWithoutCatalog();
        assertThatThrownBy(() -> p.execute("unknown_proc",
                Map.of("arg0", NAMESPACE + ".x")))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("unknown_proc");
    }

    @Test
    void execute_missingTableNameThrows() {
        IcebergMaintenanceProcedures p = proceduresWithoutCatalog();
        assertThatThrownBy(() -> p.execute("compact", Map.of()))
                .isInstanceOf(ConnectorException.class)
                .hasMessageContaining("table name");
    }

    @Test
    void execute_unqualifiedTableNameThrows() {
        IcebergMaintenanceProcedures p = proceduresWithoutCatalog();
        assertThatThrownBy(() -> p.execute("compact",
                Map.of("arg0", "no_schema_qualifier")))
                .isInstanceOf(ConnectorException.class)
                .hasMessageContaining("schema-qualified");
    }

    @Test
    void execute_nonexistentTableThrows() {
        initCatalog();
        assertThatThrownBy(() -> procedures.execute("compact",
                Map.of("arg0", NAMESPACE + ".nonexistent")))
                .isInstanceOf(ConnectorException.class)
                .hasMessageContaining("Table not found");
    }

    // -- IcebergMetadata.executeProcedure dispatch -----------------------------

    @Test
    void icebergMetadata_executeProcedure_dispatchesToProcedures() {
        initCatalog();
        createTable("dispatch_test");
        List<TypedRow> result = metadata.executeProcedure(SESSION, "compact",
                Map.of("arg0", NAMESPACE + ".dispatch_test"));
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getString("result")).isEqualTo("compact succeeded");
    }

    // -- helpers ---------------------------------------------------------------

    private Catalog buildHadoopCatalog() {
        Map<String, String> props = Map.of(
                "type", "hadoop",
                "warehouse", warehouse.toString());
        return org.apache.iceberg.CatalogUtil.buildIcebergCatalog(
                CATALOG_NAME, props, new org.apache.hadoop.conf.Configuration());
    }

    private Table createTable(String tableName) {
        TableIdentifier id = TableIdentifier.of(NAMESPACE, tableName);
        return catalog.createTable(id, TEST_SCHEMA,
                PartitionSpec.unpartitioned(),
                Map.of("format-version", "2"));
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
            table.refresh();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to write data file", e);
        }
    }

    private static int countSnapshots(Table table) {
        int count = 0;
        for (Snapshot ignored : table.snapshots()) {
            count++;
        }
        return count;
    }

    private static long countFilesUnder(Path dir) throws IOException {
        try (var stream = Files.walk(dir)) {
            return stream.filter(Files::isRegularFile).count();
        }
    }
}
