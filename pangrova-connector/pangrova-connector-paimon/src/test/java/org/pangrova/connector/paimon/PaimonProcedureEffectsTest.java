package org.pangrova.connector.paimon;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.connector.api.error.ConnectorException;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** SDK effects only; connector procedure availability does not authorize SQL CALL. */
class PaimonProcedureEffectsTest {

    private static final String TABLE_NAME = "db.effects";
    private static final Identifier TABLE_ID = Identifier.create("db", "effects");

    @TempDir
    Path warehouse;

    private Catalog catalog;
    private PaimonMetadata metadata;

    @BeforeEach
    void setUp() throws Exception {
        Options options = new Options();
        options.set("warehouse", warehouse.toUri().toString());
        catalog = CatalogFactory.createCatalog(CatalogContext.create(options));
        catalog.createDatabase("db", false);
        catalog.createTable(TABLE_ID, Schema.newBuilder()
                .column("id", DataTypes.BIGINT())
                .column("value", DataTypes.STRING())
                .primaryKey("id")
                .option("bucket", "1")
                .build(), false);
        metadata = new PaimonMetadata(catalog, "procedure_effects", new PaimonSplitRegistry());
    }

    @AfterEach
    void tearDown() throws Exception {
        if (catalog != null) {
            catalog.close();
        }
    }

    @Test
    void compactPreservesLatestPrimaryKeyValues() throws Exception {
        write(1, "before");
        write(2, "retained");
        write(1, "after");
        var expected = Map.of(1L, "after", 2L, "retained");
        assertThat(rows()).isEqualTo(expected);

        var result = metadata.executeProcedure(null, "compact", Map.of("arg0", TABLE_NAME));

        assertThat(result.get(0).getString("result")).isEqualTo("compact succeeded");
        assertThat(rows()).isEqualTo(expected);
        assertThat(freshTable().latestSnapshot().orElseThrow().commitKind().name())
                .isEqualTo("COMPACT");
    }

    @Test
    void tagsPinTheRequestedSnapshotAndCanBeDeletedWithoutChangingRows() throws Exception {
        write(1, "before");
        long pinned = snapshotId();
        write(1, "after");

        metadata.executeProcedure(null, "create_tag", Map.of(
                "arg0", TABLE_NAME, "arg1", "pinned", "arg2", Long.toString(pinned), "arg3", "1 d"));

        var manager = freshTable().tagManager();
        assertThat(manager.tagExists("pinned")).isTrue();
        assertThat(manager.getOrThrow("pinned").id()).isEqualTo(pinned);
        assertThat(manager.getOrThrow("pinned").getTagTimeRetained()).isEqualTo(java.time.Duration.ofDays(1));
        assertThat(rows()).containsExactlyEntriesOf(Map.of(1L, "after"));

        metadata.executeProcedure(null, "delete_tag", Map.of("arg0", TABLE_NAME, "arg1", "pinned"));

        assertThat(freshTable().tagManager().tagExists("pinned")).isFalse();
        assertThat(rows()).containsExactlyEntriesOf(Map.of(1L, "after"));
    }

    @Test
    void rollbackToTagRestoresThePinnedRowsForAReopenedCatalog() throws Exception {
        write(1, "before");
        metadata.executeProcedure(null, "create_tag", Map.of("arg0", TABLE_NAME, "arg1", "baseline"));
        long pinned = snapshotId();
        write(1, "after");
        write(2, "new-row");
        assertThat(rows()).hasSize(2);

        metadata.executeProcedure(null, "rollback_to", Map.of("arg0", TABLE_NAME, "arg1", "baseline"));

        catalog.close();
        Options options = new Options();
        options.set("warehouse", warehouse.toUri().toString());
        catalog = CatalogFactory.createCatalog(CatalogContext.create(options));
        assertThat(snapshotId()).isEqualTo(pinned);
        assertThat(rows()).containsExactlyEntriesOf(Map.of(1L, "before"));
    }

    @Test
    void branchesAreCreatedAndDeletedWithoutChangingMainRows() throws Exception {
        write(1, "main");
        long before = snapshotId();

        metadata.executeProcedure(null, "create_branch", Map.of("arg0", TABLE_NAME, "arg1", "review"));

        assertThat(freshTable().branchManager().branches()).contains("review");
        assertThat(snapshotId()).isEqualTo(before);
        assertThat(rows()).containsExactlyEntriesOf(Map.of(1L, "main"));

        metadata.executeProcedure(null, "delete_branch", Map.of("arg0", TABLE_NAME, "arg1", "review"));

        assertThat(freshTable().branchManager().branches()).doesNotContain("review");
        assertThat(snapshotId()).isEqualTo(before);
        assertThat(rows()).containsExactlyEntriesOf(Map.of(1L, "main"));
    }

    @Test
    void orphanCleanupDeletesOldUnreferencedFilesAndPreservesLiveData() throws Exception {
        write(1, "live");
        long before = snapshotId();
        Path root = Path.of(freshTable().location().toUri());
        Path orphan = root.resolve("bucket-0/unreferenced.parquet");
        Files.createDirectories(orphan.getParent());
        Files.writeString(orphan, "unreferenced");
        Files.setLastModifiedTime(orphan, FileTime.fromMillis(0L));

        var result = metadata.executeProcedure(null, "remove_orphan_files", Map.of(
                "arg0", TABLE_NAME, "arg1", Long.toString(System.currentTimeMillis() - 25L * 3_600_000L)));

        assertThat(result.get(0).getLong("deleted_file_count")).isGreaterThanOrEqualTo(1L);
        assertThat(orphan).doesNotExist();
        assertThat(snapshotId()).isEqualTo(before);
        assertThat(rows()).containsExactlyEntriesOf(Map.of(1L, "live"));
    }

    @Test
    void rejectedProceduresAreNotAdvertisedAndLeaveDurableStateUnchanged() throws Exception {
        write(1, "live");
        long before = snapshotId();
        var advertised = metadata.listTableProcedures().stream().map(procedure -> procedure.name()).toList();
        assertThat(advertised).containsExactlyInAnyOrder(
                "compact", "expire_snapshots", "remove_orphan_files", "create_tag",
                "delete_tag", "rollback_to", "create_branch", "delete_branch");

        for (String procedure : List.of(
                "merge_branch", "repair", "rewrite_file_index", "reset_consumer", "rescale_bucket")) {
            assertThatThrownBy(() -> metadata.executeProcedure(null, procedure,
                    Map.of("arg0", TABLE_NAME, "arg1", "review")))
                    .as(procedure).isInstanceOf(ConnectorException.class).hasMessageContaining("not implemented");
            assertThat(snapshotId()).as(procedure).isEqualTo(before);
            assertThat(freshTable().branchManager().branches()).as(procedure).doesNotContain("review");
            assertThat(rows()).as(procedure).containsExactlyEntriesOf(Map.of(1L, "live"));
        }
    }

    private FileStoreTable freshTable() throws Exception {
        catalog.invalidateTable(TABLE_ID);
        return (FileStoreTable) catalog.getTable(TABLE_ID);
    }

    private long snapshotId() throws Exception {
        return freshTable().snapshotManager().latestSnapshotId();
    }

    private void write(long id, String value) throws Exception {
        var builder = freshTable().newBatchWriteBuilder();
        try (var write = builder.newWrite(); var commit = builder.newCommit()) {
            write.write(GenericRow.of(id, BinaryString.fromString(value)));
            commit.commit(write.prepareCommit());
        }
    }

    private Map<Long, String> rows() throws Exception {
        var builder = freshTable().newReadBuilder();
        Map<Long, String> result = new HashMap<>();
        try (var iterator = builder.newRead().createReader(builder.newScan().plan()).toCloseableIterator()) {
            while (iterator.hasNext()) {
                var row = iterator.next();
                assertThat(result.put(row.getLong(0), row.getString(1).toString())).isNull();
            }
        }
        return result;
    }
}
