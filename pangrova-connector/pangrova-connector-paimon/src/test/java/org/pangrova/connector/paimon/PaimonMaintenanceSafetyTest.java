package org.pangrova.connector.paimon;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.types.value.TypedRow;
import org.pangrova.connector.api.exception.ConnectorException;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Destructive Paimon procedures use safe defaults, support dry-run, and never fake success. */
class PaimonMaintenanceSafetyTest {

    @TempDir
    Path warehouse;

    private Catalog catalog;
    private PaimonMetadata metadata;
    private Identifier tableId;

    @BeforeEach
    void setUp() throws Exception {
        Options options = new Options();
        options.set("warehouse", warehouse.toUri().toString());
        catalog = CatalogFactory.createCatalog(CatalogContext.create(options));
        catalog.createDatabase("db", false);
        Schema schema = Schema.newBuilder()
                .column("id", DataTypes.BIGINT())
                .column("value", DataTypes.STRING())
                .primaryKey("id")
                .option("bucket", "1")
                .build();
        tableId = Identifier.create("db", "safety");
        catalog.createTable(tableId, schema, false);
        metadata = new PaimonMetadata(catalog, "safety_catalog", new PaimonSplitRegistry());
    }

    @Test
    void expireSnapshotsKeepsASafetyWindowByDefault() throws Exception {
        for (int i = 0; i < 3; i++) {
            commitData(i);
        }
        long before = snapshotCount();

        TypedRow row = metadata.executeProcedure(null, "expire_snapshots",
                Map.of("arg0", "db.safety")).get(0);

        assertThat(row.getString("result")).isEqualTo("expire_snapshots succeeded");
        assertThat(row.getLong("retain_max")).isEqualTo(30L);
        assertThat(row.getLong("retain_min")).isEqualTo(10L);
        assertThat(row.getLong("expired_snapshots")).isZero();
        assertThat(snapshotCount()).isEqualTo(before);
    }

    @Test
    void expireSnapshotsDryRunReportsCandidatesWithoutExpiring() throws Exception {
        for (int i = 0; i < 4; i++) {
            commitData(i);
        }
        long before = snapshotCount();

        TypedRow row = metadata.executeProcedure(null, "expire_snapshots",
                Map.of("arg0", "db.safety", "arg1", "1", "arg3", "true")).get(0);

        assertThat(row.getString("result")).isEqualTo("expire_snapshots dry run");
        assertThat(row.getString("dry_run")).isEqualTo("true");
        assertThat(row.getLong("expired_snapshots")).isEqualTo(before - 1);
        assertThat(snapshotCount()).isEqualTo(before);
    }

    @Test
    void expireSnapshotsRejectsInvalidRetentionBounds() {
        assertThatThrownBy(() -> metadata.executeProcedure(null, "expire_snapshots",
                Map.of("arg0", "db.safety", "arg1", "2", "arg2", "3")))
                .isInstanceOf(ConnectorException.class)
                .hasMessageContaining("retain_min");
        assertThatThrownBy(() -> metadata.executeProcedure(null, "expire_snapshots",
                Map.of("arg0", "db.safety", "arg1", "0")))
                .isInstanceOf(ConnectorException.class)
                .hasMessageContaining("retain_max");
    }

    @Test
    void removeOrphanFilesRejectsCutoffYoungerThanTheSafetyMinimum() {
        assertThatThrownBy(() -> metadata.executeProcedure(null, "remove_orphan_files",
                Map.of("arg0", "db.safety", "arg1", String.valueOf(System.currentTimeMillis()))))
                .isInstanceOf(ConnectorException.class)
                .hasMessageContaining("at least 24 hours old");
    }

    @Test
    void removeOrphanFilesDryRunReportsWithoutDeleting() throws Exception {
        FileStoreTable table = (FileStoreTable) catalog.getTable(tableId);
        // Paimon scans bucket-* directories directly under the table root for orphan data files.
        Path bucketDir = Path.of(table.location().toString().replaceFirst("^file:", ""), "bucket-0");
        Files.createDirectories(bucketDir);
        Path orphan = bucketDir.resolve("orphan-" + System.nanoTime() + ".parquet");
        Files.writeString(orphan, "garbage");
        Files.setLastModifiedTime(orphan, java.nio.file.attribute.FileTime.fromMillis(0L));

        long cutoff = System.currentTimeMillis() - 25L * 3_600_000L;
        TypedRow row = metadata.executeProcedure(null, "remove_orphan_files",
                Map.of("arg0", "db.safety", "arg1", String.valueOf(cutoff), "arg2", "true"))
                .get(0);

        assertThat(row.getString("result")).isEqualTo("remove_orphan_files dry run");
        assertThat(row.getString("dry_run")).isEqualTo("true");
        assertThat(row.getLong("orphan_file_count")).isGreaterThanOrEqualTo(1L);
        assertThat(row.getLong("deleted_file_count")).isZero();
        assertThat(Files.exists(orphan)).isTrue();
    }

    @Test
    void unimplementedProceduresFailInsteadOfReportingSuccess() {
        for (String procedure : List.of(
                "merge_branch", "repair", "rewrite_file_index", "reset_consumer", "rescale_bucket")) {
            assertThatThrownBy(() -> metadata.executeProcedure(null, procedure,
                    Map.of("arg0", "db.safety", "arg1", "x")))
                    .as(procedure)
                    .isInstanceOf(ConnectorException.class)
                    .hasMessageContaining("not implemented");
        }
    }

    private long snapshotCount() throws Exception {
        return ((FileStoreTable) catalog.getTable(tableId)).snapshotManager().snapshotCount();
    }

    private void commitData(long id) throws Exception {
        Table table = catalog.getTable(tableId);
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
             BatchTableCommit commit = writeBuilder.newCommit()) {
            write.write(org.apache.paimon.data.GenericRow.of(id,
                    org.apache.paimon.data.BinaryString.fromString("val-" + id)));
            commit.commit(write.prepareCommit());
        }
    }
}
