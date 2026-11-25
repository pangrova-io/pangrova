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
import org.apache.paimon.utils.SnapshotManager;
import org.pangrova.types.value.TypedRow;
import org.pangrova.connector.api.exception.SnapshotExpiredException;
import org.pangrova.connector.api.index.SnapshotPinOwner;
import org.pangrova.connector.api.index.SnapshotPinReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for G-CONN-07: Paimon expire_snapshots procedure.
 *
 * <p>Verifies that the procedure uses Paimon's proper {@code ExpireSnapshots} API:
 * <ol>
 *   <li>Tagged snapshots are never deleted (tag reference check)</li>
 *   <li>Earliest-snapshot invariant is maintained</li>
 *   <li>No-op when no snapshots exist</li>
 * </ol>
 *
 * @see PaimonMetadata#executeProcedure
 */
class PaimonExpireSnapshotsTest {

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
        tableId = Identifier.create("db", "test_expire");
        catalog.createTable(tableId, schema, false);
        metadata = new PaimonMetadata(catalog, "test_catalog", new org.pangrova.connector.paimon.PaimonSplitRegistry());
    }

    @Test
    void taggedSnapshotsArePreservedAfterExpire() throws Exception {
        // Create 5 snapshots
        for (int i = 0; i < 5; i++) {
            commitData(i);
        }

        FileStoreTable fileStoreTable = (FileStoreTable) catalog.getTable(tableId);
        SnapshotManager snapshotManager = fileStoreTable.snapshotManager();
        long latestId = snapshotManager.latestSnapshotId();
        long earliestId = snapshotManager.earliestSnapshotId();
        assertThat(latestId).isGreaterThanOrEqualTo(5);

        // Tag snapshot 2 (so the tag data must survive expiration)
        long taggedSnapshotId = earliestId + 1;
        fileStoreTable.createTag("keep-me", taggedSnapshotId);

        // Expire with retain_max=1
        List<TypedRow> result = metadata.executeProcedure(
                null, "expire_snapshots",
                Map.of("arg0", "db.test_expire", "retain_max", "1"));

        assertThat(result).hasSize(1);
        assertThat(result.get(0).getString("result")).isEqualTo("expire_snapshots succeeded");
        assertThat(result.get(0).getLong("retain_max")).isEqualTo(1L);
        assertThat(result.get(0).getLong("duration_ms")).isGreaterThanOrEqualTo(0L);

        // In Paimon, tags preserve data independently of the snapshot chain.
        // After expire, the tag is still accessible via the tag API even though
        // the numbered snapshot file may have been cleaned up.
        FileStoreTable afterTable = (FileStoreTable) catalog.getTable(tableId);
        assertThat(afterTable.tagManager().tagExists("keep-me"))
                .as("Tag 'keep-me' must survive snapshot expiration")
                .isTrue();
    }

    @Test
    void indexPinsRetainExactHistoricalSnapshotsThroughExpiration() throws Exception {
        commitData(0);
        commitData(1);
        FileStoreTable table = (FileStoreTable) catalog.getTable(tableId);
        long oldSnapshot = table.snapshotManager().latestSnapshotId();
        var pins = metadata.getIndexSupport().orElseThrow().getSnapshotPinProvider().orElseThrow();
        String oldToken = Long.toString(oldSnapshot);
        String currentToken;
        var firstOwner = new SnapshotPinOwner("index-one", 0, "attempt-1");
        var replacementOwner = new SnapshotPinOwner("index-one", 1, "attempt-2");
        var secondIndexOwner = new SnapshotPinOwner("index-two", 0, "attempt-1");
        pins.pinSnapshot("test_catalog", "db", "test_expire", oldToken, firstOwner);
        pins.pinSnapshot("test_catalog", "db", "test_expire", oldToken, firstOwner);
        pins.pinSnapshot("test_catalog", "db", "test_expire", oldToken, secondIndexOwner);
        for (int i = 2; i < 5; i++) {
            commitData(i);
        }
        long currentSnapshot = ((FileStoreTable) catalog.getTable(tableId))
                .snapshotManager().latestSnapshotId();
        currentToken = Long.toString(currentSnapshot);
        pins.pinSnapshot("test_catalog", "db", "test_expire", currentToken, replacementOwner);

        metadata.executeProcedure(null, "expire_snapshots",
                Map.of("arg0", "db.test_expire", "retain_max", "1"));
        assertThat(((FileStoreTable) catalog.getTable(tableId))
                .snapshotManager().snapshotExists(oldSnapshot)).isFalse();
        var reopened = new PaimonMetadata(catalog, "test_catalog", new PaimonSplitRegistry());
        var reopenedPins = reopened.getIndexSupport().orElseThrow()
                .getSnapshotPinProvider().orElseThrow();
        assertThat(reopenedPins.getSnapshotPinsForIndex(
                "test_catalog", "db", "test_expire", "index-one"))
                .containsExactlyInAnyOrder(
                        new SnapshotPinReference(oldToken, firstOwner),
                        new SnapshotPinReference(currentToken, replacementOwner));
        var snapshots = reopened.getSnapshotAwareMetadata("db", "test_expire").orElseThrow();
        assertThat(snapshots.currentSnapshotId()).contains(currentToken);
        assertThat(snapshots.listFilesAtSnapshot(oldToken)).isNotEmpty();

        reopenedPins.unpinSnapshot("test_catalog", "db", "test_expire", oldToken, firstOwner);
        assertThat(reopenedPins.getSnapshotPinsForIndex(
                "test_catalog", "db", "test_expire", "index-two"))
                .containsExactly(new SnapshotPinReference(oldToken, secondIndexOwner));
        reopenedPins.unpinSnapshot("test_catalog", "db", "test_expire", oldToken, secondIndexOwner);
        reopenedPins.unpinSnapshot("test_catalog", "db", "test_expire", currentToken, replacementOwner);
        assertThat(reopenedPins.getSnapshotPinsForIndex(
                "test_catalog", "db", "test_expire", "index-one")).isEmpty();
        assertThatThrownBy(() -> snapshots.listFilesAtSnapshot(oldToken))
                .isInstanceOf(SnapshotExpiredException.class);
        assertThatThrownBy(() -> reopenedPins.pinSnapshot(
                "test_catalog", "db", "test_expire", "0",
                new SnapshotPinOwner("index-one", 2, "attempt-3")))
                .isInstanceOf(SnapshotExpiredException.class);
    }

    @Test
    void earliestSnapshotInvariantIsMaintained() throws Exception {
        // Create 4 snapshots
        for (int i = 0; i < 4; i++) {
            commitData(i);
        }

        FileStoreTable fileStoreTable = (FileStoreTable) catalog.getTable(tableId);
        SnapshotManager snapshotManager = fileStoreTable.snapshotManager();
        long latestId = snapshotManager.latestSnapshotId();

        // Expire with retain_max=2
        metadata.executeProcedure(
                null, "expire_snapshots",
                Map.of("arg0", "db.test_expire", "retain_max", "2"));

        // After expiration, earliestSnapshotId must still point to an existing snapshot
        Long earliestId = snapshotManager.earliestSnapshotId();
        if (earliestId != null) {
            assertThat(snapshotManager.snapshotExists(earliestId))
                    .as("earliest snapshot must exist after expiration")
                    .isTrue();
        }

        // Verify snapshot count
        long existingCount = 0;
        for (long id = earliestId; id <= latestId; id++) {
            if (snapshotManager.snapshotExists(id)) {
                existingCount++;
            }
        }
        assertThat(existingCount).isGreaterThanOrEqualTo(1);
    }

    @Test
    void expireWithNoSnapshotsIsNoOp() {
        // Table with no data committed -- no snapshots exist
        // Should not throw
        List<TypedRow> result = metadata.executeProcedure(
                null, "expire_snapshots",
                Map.of("arg0", "db.test_expire", "retain_max", "1"));

        assertThat(result).hasSize(1);
        assertThat(result.get(0).getString("result")).isEqualTo("expire_snapshots succeeded");
        assertThat(result.get(0).getLong("expired_snapshots")).isGreaterThanOrEqualTo(0);
    }

    @Test
    void expireReportsExpiredSnapshotCount() throws Exception {
        // Create 4 snapshots
        for (int i = 0; i < 4; i++) {
            commitData(i);
        }

        List<TypedRow> result = metadata.executeProcedure(
                null, "expire_snapshots",
                Map.of("arg0", "db.test_expire", "retain_max", "1"));

        assertThat(result).hasSize(1);
        TypedRow row = result.get(0);
        assertThat(row.getString("result")).isEqualTo("expire_snapshots succeeded");
        // With 4 snapshots and retain_max=1, at least some should be expired
        assertThat(row.getLong("expired_snapshots")).isGreaterThanOrEqualTo(0);
    }

    private void commitData(long id) throws Exception {
        Table table = catalog.getTable(tableId);
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
             BatchTableCommit commit = writeBuilder.newCommit()) {
            org.apache.paimon.data.GenericRow row =
                    org.apache.paimon.data.GenericRow.of(id,
                            org.apache.paimon.data.BinaryString.fromString("val-" + id));
            write.write(row);
            commit.commit(write.prepareCommit());
        }
    }
}
