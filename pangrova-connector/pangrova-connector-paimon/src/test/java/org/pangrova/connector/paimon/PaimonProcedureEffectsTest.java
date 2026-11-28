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
import org.pangrova.connector.api.exception.ConnectorException;
import org.pangrova.connector.api.exception.SnapshotExpiredException;
import org.pangrova.connector.api.index.SnapshotPinOwner;
import org.pangrova.connector.api.index.SnapshotDataFile;
import org.pangrova.common.connector.SnapshotPinTags;

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
    void physicalCompactionAndExpirationPreserveEachExactIndexHold() throws Exception {
        write(1, "before");
        write(2, "retained");
        String oldToken = Long.toString(snapshotId());
        var first = new SnapshotPinOwner("index-one", 0, "attempt-one");
        var second = new SnapshotPinOwner("index-two", 0, "attempt-two");
        var replacement = new SnapshotPinOwner("index-one", 1, "attempt-three");
        var pins = metadata.getIndexSupport().orElseThrow().getSnapshotPinProvider().orElseThrow();
        pins.pinSnapshot("procedure_effects", "db", "effects", oldToken, first);
        pins.pinSnapshot("procedure_effects", "db", "effects", oldToken, second);
        var snapshots = metadata.getSnapshotAwareMetadata("db", "effects").orElseThrow();
        var oldPaths = snapshots.listFilesAtSnapshot(oldToken).stream().map(SnapshotDataFile::path).toList();
        assertThat(oldPaths).isNotEmpty();
        assertThat(rows()).isEqualTo(Map.of(1L, "before", 2L, "retained"));
        write(1, "after");
        write(3, "new");
        String preCompaction = Long.toString(snapshotId());

        var result = metadata.executeProcedure(null, "compact", Map.of("arg0", TABLE_NAME));
        assertThat(result.get(0).getString("result")).isEqualTo("compact succeeded");
        assertThat(freshTable().latestSnapshot().orElseThrow().commitKind().name()).isEqualTo("COMPACT");
        String currentToken = Long.toString(snapshotId());
        assertThat(currentToken).isNotEqualTo(preCompaction);
        var currentPaths = metadata.getSnapshotAwareMetadata("db", "effects").orElseThrow()
                .listFilesAtSnapshot(currentToken).stream().map(SnapshotDataFile::path).toList();
        var replacedPaths = oldPaths.stream().filter(path -> !currentPaths.contains(path)).toList();
        assertThat(replacedPaths).isNotEmpty();
        pins.pinSnapshot("procedure_effects", "db", "effects", currentToken, replacement);
        metadata.executeProcedure(null, "expire_snapshots", Map.of("arg0", TABLE_NAME, "retain_max", "1"));
        assertThat(freshTable().snapshotManager().snapshotExists(Long.parseLong(oldToken))).isFalse();

        var options = new Options();
        options.set("warehouse", warehouse.toUri().toString());
        try (var reopened = CatalogFactory.createCatalog(CatalogContext.create(options))) {
            var reopenedMetadata = new PaimonMetadata(reopened, "procedure_effects", new PaimonSplitRegistry());
            var reopenedPins = reopenedMetadata.getIndexSupport().orElseThrow().getSnapshotPinProvider().orElseThrow();
            var reopenedSnapshots = reopenedMetadata.getSnapshotAwareMetadata("db", "effects").orElseThrow();
            var reopenedTable = (FileStoreTable) reopened.getTable(TABLE_ID);
            reopenedPins.validateSnapshotPin("procedure_effects", "db", "effects", oldToken, first);
            reopenedPins.validateSnapshotPin("procedure_effects", "db", "effects", oldToken, second);
            assertThat(reopenedSnapshots.listFilesAtSnapshot(oldToken)).extracting(SnapshotDataFile::path)
                    .containsExactlyInAnyOrderElementsOf(oldPaths);
            for (String path : replacedPaths) {
                assertThat(reopenedTable.fileIO().exists(new org.apache.paimon.fs.Path(path))).isTrue();
            }
            assertThat(rowsAtTag(reopenedTable, first, oldToken)).isEqualTo(Map.of(1L, "before", 2L, "retained"));
            reopenedPins.unpinSnapshot("procedure_effects", "db", "effects", oldToken, first);
            reopenedPins.validateSnapshotPin("procedure_effects", "db", "effects", oldToken, second);
            assertThat(rowsAtTag(reopenedTable, second, oldToken)).isEqualTo(Map.of(1L, "before", 2L, "retained"));
            reopenedPins.unpinSnapshot("procedure_effects", "db", "effects", oldToken, second);
            assertThatThrownBy(() -> reopenedSnapshots.listFilesAtSnapshot(oldToken))
                    .isInstanceOf(SnapshotExpiredException.class);
            reopenedPins.validateSnapshotPin("procedure_effects", "db", "effects", currentToken, replacement);
            assertThat(rows()).isEqualTo(Map.of(1L, "after", 2L, "retained", 3L, "new"));
        }
    }

    private Map<Long, String> rowsAtTag(FileStoreTable table, SnapshotPinOwner owner, String token) throws Exception {
        String tag = SnapshotPinTags.forSnapshot(owner.indexId(), owner.generation(), owner.attemptId(), token);
        return rows(table.copy(Map.of("scan.tag-name", tag)));
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
        return rows(freshTable());
    }

    private Map<Long, String> rows(org.apache.paimon.table.Table table) throws Exception {
        var builder = table.newReadBuilder();
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
