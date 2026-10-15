package org.pangrova.metadata.coordination;

import org.pangrova.spi.coordination.JournalWriteException;
import org.pangrova.spi.coordination.WriteCommitEntry;
import org.pangrova.spi.coordination.WriteCommitFingerprint;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.metadata.testinfra.SharedPostgresContainer;
import org.pangrova.testing.ContainerTestBase;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link JdbcWriteCommitJournal} using a PostgreSQL TestContainer.
 * Verifies the full intent → commit → purge lifecycle and idempotency guarantees.
 */
class JdbcWriteCommitJournalTest extends ContainerTestBase {

    private DataSource dataSource;
    private JdbcWriteCommitJournal journal;

    @BeforeEach
    void setUp() throws SQLException {
        dataSource = SharedPostgresContainer.migratedDataSource("write_commit_journal");
        journal = new JdbcWriteCommitJournal(dataSource);
    }

    @Test
    void confirmedAbortRetainsOnlyExactIntentBarrierAndIsIdempotent() {
        var intent = entry("q-abort", "catalog-a", "table", WriteCommitEntry.State.PENDING, 1L);
        journal.recordIntent(intent);

        assertThatThrownBy(() -> journal.markAborted("q-abort", "wrong-fingerprint"))
                .isInstanceOf(IllegalStateException.class);
        assertThat(journal.getEntry("q-abort")).isPresent();
        journal.markAborted("q-abort", intent.commitFingerprint());
        journal.markAborted("q-abort", intent.commitFingerprint());

        assertThat(journal.getEntry("q-abort")).get()
                .extracting(WriteCommitEntry::state).isEqualTo(WriteCommitEntry.State.ABORTED);
        assertThat(journal.replayPendingEntries(Set.of("q-abort"))).isEmpty();
        assertThatThrownBy(() -> journal.recordIntent(intent)).isInstanceOf(JournalWriteException.class);
        assertThatThrownBy(() -> journal.markCommitted("q-abort")).isInstanceOf(IllegalStateException.class);
        journal.purge("q-abort");
        assertThat(journal.getEntry("q-abort")).isPresent();
    }

    @Test
    void confirmedAbortCannotReplaceCommittedPublication() {
        var intent = entry("q-abort", "catalog-a", "table", WriteCommitEntry.State.PENDING, 1L);
        journal.recordIntent(intent);
        journal.markCommitted("q-abort");

        assertThatThrownBy(() -> journal.markAborted("q-abort", intent.commitFingerprint()))
                .isInstanceOf(IllegalStateException.class);
        assertThat(journal.isCommitted("q-abort")).isTrue();
    }

    @Test
    void recordIntentPersistsEntry() {
        WriteCommitEntry entry = entry(
                "q-1", "catalog-a", "schema.table",
                WriteCommitEntry.State.PENDING, System.currentTimeMillis());

        journal.recordIntent(entry);

        Optional<WriteCommitEntry> retrieved = journal.getEntry("q-1");
        assertThat(retrieved).isPresent();
        assertThat(retrieved.get().queryId()).isEqualTo("q-1");
        assertThat(retrieved.get().catalogName()).isEqualTo("catalog-a");
        assertThat(retrieved.get().tableName()).isEqualTo("schema.table");
        assertThat(retrieved.get().state()).isEqualTo(WriteCommitEntry.State.PENDING);
    }

    @Test
    void recordIntentPersistsReplayPayloadAcrossDatabaseRoundTrip() {
        // A-COMMIT-01: the durable entry must be a replayable redo record — the worker commit
        // fragments, write mode and idempotency key must survive a DB round-trip so a replacement
        // coordinator can reconstruct the commit after a crash rather than just observe a marker.
        java.util.List<byte[]> fragments = java.util.List.of(
                new byte[]{1, 2, 3}, new byte[]{4, 5, 6, 7});
        String fingerprint = WriteCommitFingerprint.compute(
                new org.pangrova.spi.coordination.WriteCommitFingerprint.Intent(
                        "q-replay",
                        "tenant-a",
                        "iceberg",
                        "analytics.events",
                        "OVERWRITE",
                        41L,
                        new byte[]{42},
                        "alice",
                        org.pangrova.metadata.testinfra.MetadataWriteReceiptFixtures.receipt(), null),
                fragments);
        journal.recordIntent(new WriteCommitEntry(
                "q-replay",
                "iceberg",
                "analytics.events",
                WriteCommitEntry.State.PENDING,
                System.currentTimeMillis(),
                fragments,
                "OVERWRITE",
                fingerprint,
                "tenant-a",
                "alice",
                org.pangrova.metadata.testinfra.MetadataWriteReceiptFixtures.receipt(),
                41L,
                new byte[]{42},
                fingerprint,
                "coord-A",
                "process-A",
                7L,
                null, null));

        WriteCommitEntry reloaded = journal.getEntry("q-replay").orElseThrow();
        assertThat(reloaded.fragments()).hasSize(2);
        assertThat(reloaded.fragments().get(0)).isEqualTo(new byte[]{1, 2, 3});
        assertThat(reloaded.fragments().get(1)).isEqualTo(new byte[]{4, 5, 6, 7});
        assertThat(reloaded.writeMode()).isEqualTo("OVERWRITE");
        assertThat(reloaded.idempotencyKey()).isEqualTo(fingerprint);
        assertThat(reloaded.tenantId()).isEqualTo("tenant-a");
        assertThat(reloaded.baseSnapshotId()).isEqualTo(41L);
        assertThat(reloaded.writeOutputBinding()).isEqualTo(new byte[]{42});
        assertThat(reloaded.commitFingerprint()).isEqualTo(fingerprint);
        assertThat(reloaded.ownerId()).isEqualTo("coord-A");
        assertThat(reloaded.ownerIncarnation()).isEqualTo("process-A");
        assertThat(reloaded.executionFencingToken()).isEqualTo(7L);
    }

    @Test
    void markCommittedUpdatesState() {
        journal.recordIntent(entry(
                "q-1", "catalog-a", "table",
                WriteCommitEntry.State.PENDING, System.currentTimeMillis()));

        journal.markCommitted("q-1");

        Optional<WriteCommitEntry> entry = journal.getEntry("q-1");
        assertThat(entry).isPresent();
        assertThat(entry.get().state()).isEqualTo(WriteCommitEntry.State.COMMITTED);
    }

    @Test
    void isCommittedReturnsTrueAfterMarkCommitted() {
        journal.recordIntent(entry(
                "q-1", "catalog-a", "table",
                WriteCommitEntry.State.PENDING, System.currentTimeMillis()));

        assertThat(journal.isCommitted("q-1")).isFalse();

        journal.markCommitted("q-1");

        assertThat(journal.isCommitted("q-1")).isTrue();
    }

    @Test
    void isCommittedReturnsFalseForNonExistentQuery() {
        assertThat(journal.isCommitted("nonexistent")).isFalse();
    }

    @Test
    void purgeRequiresExplicitCommittedAbsenceAcknowledgement() {
        journal.recordIntent(entry(
                "q-1", "catalog-a", "table",
                WriteCommitEntry.State.PENDING, System.currentTimeMillis()));

        journal.purge("q-1");

        assertThat(journal.getEntry("q-1")).isPresent();
        assertThatThrownBy(() -> journal.acknowledgeNoDeferredWork(
                journal.getEntry("q-1").orElseThrow().commitFingerprint()))
                .isInstanceOf(IllegalStateException.class);
        journal.markCommitted("q-1");
        String fingerprint = journal.getEntry("q-1").orElseThrow().commitFingerprint();
        assertThat(journal.isDeferredWorkAcknowledged(fingerprint)).isFalse();
        journal.purge("q-1");
        assertThat(journal.getEntry("q-1")).isPresent();
        journal.acknowledgeNoDeferredWork(fingerprint);
        assertThat(journal.isDeferredWorkAcknowledged(fingerprint)).isTrue();
        journal.purge("q-1");

        assertThat(journal.getEntry("q-1")).isEmpty();
        assertThat(journal.isCommitted("q-1")).isFalse();
    }

    @Test
    void replayPendingEntriesReturnsOnlyPending() {
        journal.recordIntent(entry(
                "q-1", "catalog-a", "table1",
                WriteCommitEntry.State.PENDING, 1000L));
        journal.recordIntent(entry(
                "q-2", "catalog-a", "table2",
                WriteCommitEntry.State.PENDING, 2000L));
        journal.recordIntent(entry(
                "q-3", "catalog-b", "table3",
                WriteCommitEntry.State.PENDING, 3000L));

        // Mark one as committed
        journal.markCommitted("q-2");

        List<WriteCommitEntry> pending = journal.replayPendingEntries(
                Set.of("q-1", "q-2", "q-3"));
        assertThat(pending).hasSize(2);
        assertThat(pending.get(0).queryId()).isEqualTo("q-1");
        assertThat(pending.get(1).queryId()).isEqualTo("q-3");
    }

    @Test
    void acknowledgedRedoIdentityRemainsUntilItsCommitQueueEventIsConsumed() throws SQLException {
        var intent = entry("q-queue", "catalog-a", "schema.table", WriteCommitEntry.State.PENDING, 1_000L);
        journal.recordIntent(intent);
        journal.markCommitted(intent.queryId());
        journal.acknowledgeNoDeferredWork(intent.commitFingerprint());
        try (var connection = dataSource.getConnection(); var statement = connection.prepareStatement(
                "INSERT INTO index_commit_log (tenant_id,catalog_name,schema_name,table_name,snapshot_id,"
                        + "event_type,created_at,commit_fingerprint,table_sequence) "
                        + "VALUES ('tenant-a','catalog-a','schema','table','41','APPEND',CURRENT_TIMESTAMP,?,1)")) {
            statement.setString(1, intent.commitFingerprint());
            statement.executeUpdate();
        }
        journal.purge(intent.queryId());
        assertThat(journal.findByFingerprint(intent.commitFingerprint())).isPresent();
        try (var connection = dataSource.getConnection(); var statement = connection.prepareStatement(
                "UPDATE index_commit_log SET processed=TRUE WHERE commit_fingerprint=?")) {
            statement.setString(1, intent.commitFingerprint());
            statement.executeUpdate();
        }
        journal.purge(intent.queryId());
        assertThat(journal.findByFingerprint(intent.commitFingerprint())).isEmpty();
    }

    @Test
    void replayPendingEntriesReturnsEmptyWhenAllCommitted() {
        journal.recordIntent(entry(
                "q-1", "catalog-a", "table",
                WriteCommitEntry.State.PENDING, 1000L));
        journal.markCommitted("q-1");

        assertThat(journal.replayPendingEntries(Set.of("q-1"))).isEmpty();
    }

    @Test
    void replayPendingEntriesOrderedByTimestamp() {
        journal.recordIntent(entry(
                "q-late", "catalog-a", "table",
                WriteCommitEntry.State.PENDING, 3000L));
        journal.recordIntent(entry(
                "q-early", "catalog-a", "table",
                WriteCommitEntry.State.PENDING, 1000L));

        List<WriteCommitEntry> pending = journal.replayPendingEntries(
                Set.of("q-late", "q-early"));
        assertThat(pending).hasSize(2);
        assertThat(pending.get(0).queryId()).isEqualTo("q-early");
        assertThat(pending.get(1).queryId()).isEqualTo("q-late");
    }

    @Test
    void duplicateIntentThrows() {
        journal.recordIntent(entry(
                "q-1", "catalog-a", "table",
                WriteCommitEntry.State.PENDING, System.currentTimeMillis()));

        assertThatThrownBy(() -> journal.recordIntent(entry(
                "q-1", "catalog-b", "other-table",
                WriteCommitEntry.State.PENDING, System.currentTimeMillis())))
                .isInstanceOf(JournalWriteException.class);
    }

    @Test
    void identicalIntentCanBeRecordedAgainAfterAmbiguousJournalResponse() {
        WriteCommitEntry intent = entry(
                "q-retry", "catalog-a", "table",
                WriteCommitEntry.State.PENDING, System.currentTimeMillis());

        journal.recordIntent(intent);
        journal.recordIntent(intent);

        WriteCommitEntry stored = journal.getEntry("q-retry").orElseThrow();
        assertThat(stored.commitFingerprint()).isEqualTo(intent.commitFingerprint());
        assertThat(stored.state()).isEqualTo(WriteCommitEntry.State.PENDING);
    }

    @Test
    void markCommittedOnNonExistentEntryFailsClosed() {
        assertThatThrownBy(() -> journal.markCommitted("nonexistent"))
                .isInstanceOf(IllegalStateException.class);
        assertThat(journal.isCommitted("nonexistent")).isFalse();
    }

    @Test
    void purgeNonExistentEntryIsNoOp() {
        // Should not throw
        journal.purge("nonexistent");
    }

    @Test
    void tableAutoCreatedOnConstruction() throws SQLException {
        // Verify the table was created by JdbcWriteCommitJournal constructor
        try (Connection conn = dataSource.getConnection();
             ResultSet rs = conn.getMetaData().getTables(null, null,
                     "write_commit_journal", null)) {
            assertThat(rs.next()).isTrue();
        }
    }

    @Test
    void getEntryReturnsEmptyForNonExistent() {
        assertThat(journal.getEntry("does-not-exist")).isEmpty();
    }

    private static WriteCommitEntry entry(
            String queryId, String catalog, String table,
            WriteCommitEntry.State state, long timestampMs) {
        String mode = "INSERT";
        String tenant = "tenant-a";
        String fingerprint = WriteCommitFingerprint.compute(
                new org.pangrova.spi.coordination.WriteCommitFingerprint.Intent(
                        queryId,
                        tenant,
                        catalog,
                        table,
                        mode,
                        -1L,
                        new byte[]{42},
                        "alice",
                        org.pangrova.metadata.testinfra.MetadataWriteReceiptFixtures.receipt(), null),
                List.of());
        return new WriteCommitEntry(
                queryId,
                catalog,
                table,
                state,
                timestampMs,
                List.of(),
                mode,
                fingerprint,
                tenant,
                "alice",
                org.pangrova.metadata.testinfra.MetadataWriteReceiptFixtures.receipt(),
                -1L,
                new byte[]{42},
                fingerprint,
                null,
                null,
                0L,
                null, null);
    }
}
