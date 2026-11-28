package org.pangrova.metadata.coordination;

import org.pangrova.persistence.query.api.enums.WriteCommitState;
import org.pangrova.persistence.query.postgresql.transaction.JdbcWriteCommitJournal;
import org.pangrova.persistence.query.api.exception.JournalWriteException;
import org.pangrova.persistence.query.api.commit.CdcSourceIdentity;
import org.pangrova.persistence.query.api.commit.WriteCommitEntry;
import org.pangrova.persistence.query.api.commit.WriteCommitFingerprint;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.testing.testkit.postgres.SharedPostgresContainer;
import org.pangrova.testing.testkit.ContainerTestBase;
import org.pangrova.execution.api.ownership.ExecutionAnchor;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;

import org.pangrova.persistence.query.api.state.StateJournalConfig;
import org.pangrova.persistence.query.api.state.RecoveredTaskDrainJournal.DrainOutcome;
import org.pangrova.persistence.query.api.state.RecoveredTaskDrainJournal.DrainReceipt;
import org.pangrova.persistence.query.postgresql.coordination.JdbcExecutionAnchorStore;
import org.pangrova.persistence.query.postgresql.state.JdbcQueryStateJournal;
import org.pangrova.query.api.journal.JournalEventContext;
import org.pangrova.query.api.journal.QueryStateEvent;
import org.pangrova.query.api.journal.TaskIssuance;
import org.pangrova.query.api.journal.TaskStateEvent;
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
        var intent = entry("q-abort", "catalog-a", "table", WriteCommitState.PENDING, 1L);
        journal.recordIntent(intent);

        assertThatThrownBy(() -> journal.markAborted("q-abort", "wrong-fingerprint"))
                .isInstanceOf(IllegalStateException.class);
        assertThat(journal.getEntry("q-abort")).isPresent();
        journal.markAborted("q-abort", intent.commitFingerprint());
        journal.markAborted("q-abort", intent.commitFingerprint());

        assertThat(journal.getEntry("q-abort")).get()
                .extracting(WriteCommitEntry::state).isEqualTo(WriteCommitState.ABORTED);
        assertThat(journal.replayPendingEntries(Set.of("q-abort"))).isEmpty();
        assertThatThrownBy(() -> journal.recordIntent(intent)).isInstanceOf(JournalWriteException.class);
        assertThatThrownBy(() -> journal.markCommitted("q-abort")).isInstanceOf(IllegalStateException.class);
        journal.purge("q-abort");
        assertThat(journal.getEntry("q-abort")).isPresent();
    }

    @Test
    void confirmedAbortCannotReplaceCommittedPublication() {
        var intent = entry("q-abort", "catalog-a", "table", WriteCommitState.PENDING, 1L);
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
                WriteCommitState.PENDING, System.currentTimeMillis());

        journal.recordIntent(entry);

        Optional<WriteCommitEntry> retrieved = journal.getEntry("q-1");
        assertThat(retrieved).isPresent();
        assertThat(retrieved.get().queryId()).isEqualTo("q-1");
        assertThat(retrieved.get().catalogName()).isEqualTo("catalog-a");
        assertThat(retrieved.get().tableName()).isEqualTo("schema.table");
        assertThat(retrieved.get().state()).isEqualTo(WriteCommitState.PENDING);
    }

    @Test
    void recordIntentPersistsReplayPayloadAcrossDatabaseRoundTrip() {
        // A-COMMIT-01: the durable entry must be a replayable redo record — the worker commit
        // fragments, write mode and idempotency key must survive a DB round-trip so a replacement
        // coordinator can reconstruct the commit after a crash rather than just observe a marker.
        java.util.List<byte[]> fragments = java.util.List.of(
                new byte[]{0, (byte) 0xff, 44}, new byte[0], new byte[]{1, (byte) 0x80});
        String fingerprint = WriteCommitFingerprint.compute(
                new org.pangrova.persistence.query.api.commit.WriteCommitFingerprint.Intent(
                        "q-replay",
                        "tenant-a",
                        "iceberg",
                        "analytics.events",
                        "OVERWRITE",
                        41L,
                        new byte[]{0, 42, (byte) 0xff},
                        "alice",
                        org.pangrova.metadata.testinfra.MetadataWriteReceiptFixtures.receipt(), null),
                fragments);
        journal.recordIntent(new WriteCommitEntry(
                "q-replay",
                "iceberg",
                "analytics.events",
                WriteCommitState.PENDING,
                System.currentTimeMillis(),
                fragments,
                "OVERWRITE",
                fingerprint,
                "tenant-a",
                "alice",
                org.pangrova.metadata.testinfra.MetadataWriteReceiptFixtures.receipt(),
                41L,
                new byte[]{0, 42, (byte) 0xff},
                fingerprint,
                "coord-A",
                "process-A",
                7L,
                null, null, null));

        WriteCommitEntry reloaded = journal.getEntry("q-replay").orElseThrow();
        var replayed = journal.replayPendingEntries(Set.of("q-replay"));
        assertThat(replayed).hasSize(1);
        for (var copy : List.of(reloaded, replayed.get(0))) {
            assertThat(copy.fragments()).hasSize(fragments.size());
            for (int index = 0; index < fragments.size(); index++) {
                assertThat(copy.fragments().get(index)).isEqualTo(fragments.get(index));
            }
        }
        assertThat(reloaded.writeMode()).isEqualTo("OVERWRITE");
        assertThat(reloaded.idempotencyKey()).isEqualTo(fingerprint);
        assertThat(reloaded.tenantId()).isEqualTo("tenant-a");
        assertThat(reloaded.baseSnapshotId()).isEqualTo(41L);
        assertThat(reloaded.writeOutputBinding()).isEqualTo(new byte[]{0, 42, (byte) 0xff});
        assertThat(reloaded.commitFingerprint()).isEqualTo(fingerprint);
        assertThat(reloaded.ownerId()).isEqualTo("coord-A");
        assertThat(reloaded.ownerIncarnation()).isEqualTo("process-A");
        assertThat(reloaded.executionFencingToken()).isEqualTo(7L);
    }

    @Test
    void cdcSourceIdentityRoundTripsAndIsImmutable() throws SQLException {
        CdcSourceIdentity source = new CdcSourceIdentity("stream-orders", "epoch-7",
                List.of(new CdcSourceIdentity.OffsetRange("orders", 1, 40, 50),
                        new CdcSourceIdentity.OffsetRange("orders", 0, 10, 20)));
        WriteCommitEntry entry = cdcEntry(source);

        journal.recordIntent(entry);
        WriteCommitEntry reloaded = journal.getEntry(entry.queryId()).orElseThrow();

        assertThat(reloaded.cdcSource()).isEqualTo(source);
        assertThat(reloaded.cdcSource().ranges()).containsExactly(
                new CdcSourceIdentity.OffsetRange("orders", 0, 10, 20),
                new CdcSourceIdentity.OffsetRange("orders", 1, 40, 50));
        assertThat(journal.replayPendingEntries(Set.of(entry.queryId()))).singleElement()
                .extracting(WriteCommitEntry::cdcSource).isEqualTo(source);

        assertThatThrownBy(() -> {
            try (Connection connection = dataSource.getConnection();
                    var statement = connection.prepareStatement(
                            "UPDATE write_commit_journal SET cdc_stream_id='other' WHERE query_id=?")) {
                statement.setString(1, entry.queryId());
                statement.executeUpdate();
            }
        }).hasStackTraceContaining("CDC write identity is immutable");
    }

    @Test
    void cdcSourceIdentityRequiresACompleteRangeArray() {
        CdcSourceIdentity source = new CdcSourceIdentity("stream-orders", "epoch-7",
                List.of(new CdcSourceIdentity.OffsetRange("orders", 0, 10, 20)));
        WriteCommitEntry entry = cdcEntry(source);
        journal.recordIntent(entry);

        assertThatThrownBy(() -> {
            try (Connection connection = dataSource.getConnection();
                    var statement = connection.prepareStatement(
                            "INSERT INTO write_commit_journal (query_id,catalog_name,table_name,state,"
                                    + "timestamp_ms,fragments,write_mode,idempotency_key,tenant_id,"
                                    + "base_snapshot_id,write_output_binding,commit_fingerprint,principal,"
                                    + "deferred_authorization_scope,execution_fencing_token,"
                                    + "cdc_stream_id,cdc_source_epoch,cdc_offset_ranges) "
                                    + "SELECT ?,catalog_name,table_name,state,timestamp_ms,fragments,"
                                    + "write_mode,idempotency_key,tenant_id,base_snapshot_id,"
                                    + "write_output_binding,?,principal,deferred_authorization_scope,"
                                    + "execution_fencing_token,cdc_stream_id,cdc_source_epoch,'[]' "
                                    + "FROM write_commit_journal WHERE query_id=?")) {
                statement.setString(1, "q-invalid-cdc");
                statement.setString(2, "0".repeat(64));
                statement.setString(3, entry.queryId());
                statement.executeUpdate();
            }
        }).hasStackTraceContaining("chk_write_cdc_binding");
    }

    @Test
    void markCommittedUpdatesState() {
        journal.recordIntent(entry(
                "q-1", "catalog-a", "table",
                WriteCommitState.PENDING, System.currentTimeMillis()));

        journal.markCommitted("q-1");

        Optional<WriteCommitEntry> entry = journal.getEntry("q-1");
        assertThat(entry).isPresent();
        assertThat(entry.get().state()).isEqualTo(WriteCommitState.COMMITTED);
    }

    @Test
    void isCommittedReturnsTrueAfterMarkCommitted() {
        journal.recordIntent(entry(
                "q-1", "catalog-a", "table",
                WriteCommitState.PENDING, System.currentTimeMillis()));

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
                WriteCommitState.PENDING, System.currentTimeMillis()));

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
        assertThat(journal.isCommitted("q-1")).isTrue();
    }

    @Test
    void recoveredWriteRemainsPendingUntilPublicationAndRetiresAsFinished() {
        var intent = entry("recover-committed-write", "catalog-a", "schema.table", WriteCommitState.PENDING, 1L);
        var anchors = new JdbcExecutionAnchorStore(dataSource);
        var lease = Duration.ofMinutes(1);
        var prior = anchors.tryAcquire(intent.queryId(), "original", "original-boot", lease).orElseThrow();
        var settings = new StateJournalConfig("JDBC", "jdbc:postgresql:journal-test", 20, 20, 24, 1024);
        try (var facts = new JdbcQueryStateJournal(dataSource, settings)) {
            facts.appendQueryEventSync(new QueryStateEvent(JournalEventContext.create(101L, prior.ownerId()),
                    intent.queryId(), "INSERT INTO table VALUES (1)", "{}", "RUNNING"));
            facts.appendTaskIssuancesSync(List.of(new TaskStateEvent(
                    JournalEventContext.create(102L, prior.ownerId()), intent.queryId(), "writer", "worker",
                    "SCHEDULED", 0, 0, new TaskIssuance("worker-host", 8081, prior.ownerIncarnation(),
                            prior.fencingToken(), new byte[] {1}))));
            journal.recordIntent(intent);
            assertThat(anchors.yield(prior)).isTrue();
            var successor = anchors.tryAcquire(intent.queryId(), "successor", "successor-boot", lease).orElseThrow();
            var issued = facts.pendingTaskDrains(successor, 0).get(0);
            assertThat(facts.retainTaskDrains(successor, List.of(new DrainReceipt(issued.rowId(), new byte[] {2}))))
                    .isTrue();
            assertThat(facts.confirmRecoveredTaskDrains(successor)).isEqualTo(DrainOutcome.PENDING_WRITE);
            assertThat(facts.latestQueryEvent(intent.queryId()).orElseThrow().state()).isEqualTo("RUNNING");

            assertThat(journal.recordTerminalOutcome(intent, successor.fencingToken(), WriteCommitState.COMMITTED))
                    .isTrue();
            journal.acknowledgeNoDeferredWork(intent.commitFingerprint());
            journal.purge(intent.queryId());
            assertThat(journal.getEntry(intent.queryId())).isPresent();
            assertThat(facts.confirmRecoveredTaskDrains(successor)).isEqualTo(DrainOutcome.CONFIRMED);
            var terminal = facts.latestQueryEvent(intent.queryId()).orElseThrow();
            assertThat(terminal.state()).isEqualTo("FINISHED");
            assertThat(terminal.terminalOutcome().resultDisposition())
                    .isEqualTo(org.pangrova.common.enums.ResultDisposition.NONE);
            assertThat(terminal.terminalOutcome().error()).isNull();
            assertThat(anchors.releaseDrainedQuery(successor, false)).isTrue();
            assertThat(journal.getEntry(intent.queryId())).isEmpty();
        }
    }

    @Test
    void unpublishedAbortHandoffWaitsForTheExactWorkersToDrain() throws SQLException {
        var anchors = new JdbcExecutionAnchorStore(dataSource);
        var lease = Duration.ofMinutes(1);
        var prior = anchors.tryAcquire("recover-aborted-write", "original", "original-boot", lease).orElseThrow();
        var intent = entry(prior.executionId(), "catalog-a", "schema.table", WriteCommitState.PENDING, 1L, prior);
        var settings = new StateJournalConfig("JDBC", "jdbc:postgresql:journal-test", 20, 20, 24, 1024);
        try (var facts = new JdbcQueryStateJournal(dataSource, settings)) {
            facts.appendQueryEventSync(new QueryStateEvent(JournalEventContext.create(101L, prior.ownerId()),
                    intent.queryId(), "INSERT INTO table VALUES (1)", "{}", "RUNNING"));
            facts.appendTaskIssuancesSync(List.of(new TaskStateEvent(
                    JournalEventContext.create(102L, prior.ownerId()), intent.queryId(), "writer", "worker",
                    "SCHEDULED", 0, 0, new TaskIssuance("worker-host", 8081, prior.ownerIncarnation(),
                            prior.fencingToken(), new byte[] {1}))));
            journal.recordIntent(intent);
            assertThat(stagingDrainToken(intent)).isNull();
            assertThat(anchors.yield(prior)).isTrue();
            var successor = anchors.tryAcquire(intent.queryId(), "successor", "successor-boot", lease).orElseThrow();
            assertThat(journal.abortBeforePublication(intent, successor.fencingToken())).isTrue();
            assertThat(facts.confirmRecoveredTaskDrains(prior)).isEqualTo(DrainOutcome.LOST_CLAIM);
            assertThat(facts.confirmRecoveredTaskDrains(successor)).isEqualTo(DrainOutcome.PENDING_RECEIPTS);
            assertThat(stagingDrainToken(intent)).isZero();
            assertThat(anchors.releaseDrainedQuery(successor, false)).isFalse();

            var issued = facts.pendingTaskDrains(successor, 0).get(0);
            assertThat(facts.retainTaskDrains(successor, List.of(new DrainReceipt(issued.rowId(), new byte[] {2}))))
                    .isTrue();
            assertThat(facts.confirmRecoveredTaskDrains(successor)).isEqualTo(DrainOutcome.CONFIRMED);
            assertThat(stagingDrainToken(intent)).isEqualTo(successor.fencingToken());
            assertThat(journal.abortBeforePublication(intent, successor.fencingToken())).isTrue();
            assertThat(stagingDrainToken(intent)).isEqualTo(successor.fencingToken());
            assertThat(anchors.releaseDrainedQuery(successor, false)).isTrue();
            assertThat(stagingDrainToken(intent)).isEqualTo(successor.fencingToken());
            assertThat(journal.getEntry(intent.queryId())).get().extracting(WriteCommitEntry::state)
                    .isEqualTo(WriteCommitState.ABORTED);
        }
    }

    @Test
    void localPhysicalDrainPersistsTheSameGrantAsTheAbortHandoff() throws SQLException {
        var anchors = new JdbcExecutionAnchorStore(dataSource);
        var owner = anchors.tryAcquire("local-aborted-write", "original", "original-boot", Duration.ofMinutes(1))
                .orElseThrow();
        var intent = entry(owner.executionId(), "catalog-a", "schema.table", WriteCommitState.PENDING, 1L, owner);
        journal.recordIntent(intent);
        assertThat(journal.abortBeforePublication(intent, owner.fencingToken())).isTrue();
        assertThat(stagingDrainToken(intent)).isZero();
        assertThat(anchors.releaseLocallyDrainedQuery(owner)).isTrue();
        assertThat(stagingDrainToken(intent)).isEqualTo(owner.fencingToken());
    }

    private Long stagingDrainToken(WriteCommitEntry intent) throws SQLException {
        try (var connection = dataSource.getConnection();
                var statement = connection.prepareStatement(
                        "SELECT staging_drain_token FROM write_commit_journal WHERE query_id=? AND commit_fingerprint=?")) {
            statement.setString(1, intent.queryId());
            statement.setString(2, intent.commitFingerprint());
            try (var row = statement.executeQuery()) {
                assertThat(row.next()).isTrue();
                return row.getObject(1, Long.class);
            }
        }
    }

    @Test
    void replayPendingEntriesReturnsOnlyPending() {
        journal.recordIntent(entry(
                "q-1", "catalog-a", "table1",
                WriteCommitState.PENDING, 1000L));
        journal.recordIntent(entry(
                "q-2", "catalog-a", "table2",
                WriteCommitState.PENDING, 2000L));
        journal.recordIntent(entry(
                "q-3", "catalog-b", "table3",
                WriteCommitState.PENDING, 3000L));

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
        var intent = entry("q-queue", "catalog-a", "schema.table", WriteCommitState.PENDING, 1_000L);
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
                WriteCommitState.PENDING, 1000L));
        journal.markCommitted("q-1");

        assertThat(journal.replayPendingEntries(Set.of("q-1"))).isEmpty();
    }

    @Test
    void replayPendingEntriesOrderedByTimestamp() {
        journal.recordIntent(entry(
                "q-late", "catalog-a", "table",
                WriteCommitState.PENDING, 3000L));
        journal.recordIntent(entry(
                "q-early", "catalog-a", "table",
                WriteCommitState.PENDING, 1000L));

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
                WriteCommitState.PENDING, System.currentTimeMillis()));

        assertThatThrownBy(() -> journal.recordIntent(entry(
                "q-1", "catalog-b", "other-table",
                WriteCommitState.PENDING, System.currentTimeMillis())))
                .isInstanceOf(JournalWriteException.class);
    }

    @Test
    void identicalIntentCanBeRecordedAgainAfterAmbiguousJournalResponse() {
        WriteCommitEntry intent = entry(
                "q-retry", "catalog-a", "table",
                WriteCommitState.PENDING, System.currentTimeMillis());

        journal.recordIntent(intent);
        journal.recordIntent(intent);

        WriteCommitEntry stored = journal.getEntry("q-retry").orElseThrow();
        assertThat(stored.commitFingerprint()).isEqualTo(intent.commitFingerprint());
        assertThat(stored.state()).isEqualTo(WriteCommitState.PENDING);
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

    private static WriteCommitEntry cdcEntry(CdcSourceIdentity source) {
        List<byte[]> fragments = List.of(new byte[]{1, 2, 3});
        String fingerprint = WriteCommitFingerprint.compute(
                new WriteCommitFingerprint.Intent("q-cdc", "tenant-a", "iceberg", "analytics.events",
                        "INSERT", 42L, new byte[]{42}, "alice",
                        org.pangrova.metadata.testinfra.MetadataWriteReceiptFixtures.receipt(),
                        null, source), fragments);
        return new WriteCommitEntry(
                "q-cdc", "iceberg", "analytics.events", WriteCommitState.PENDING,
                System.currentTimeMillis(), fragments, "INSERT", fingerprint, "tenant-a", "alice",
                org.pangrova.metadata.testinfra.MetadataWriteReceiptFixtures.receipt(),
                42L, new byte[]{42}, fingerprint, null, null, 0L, null, null, source);
    }

    private static WriteCommitEntry entry(
            String queryId, String catalog, String table,
            WriteCommitState state, long timestampMs) {
        return entry(queryId, catalog, table, state, timestampMs, null);
    }

    private static WriteCommitEntry entry(
            String queryId, String catalog, String table,
            WriteCommitState state, long timestampMs, ExecutionAnchor owner) {
        String mode = "INSERT";
        String tenant = "tenant-a";
        String fingerprint = WriteCommitFingerprint.compute(
                new org.pangrova.persistence.query.api.commit.WriteCommitFingerprint.Intent(
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
                owner == null ? null : owner.ownerId(),
                owner == null ? null : owner.ownerIncarnation(),
                owner == null ? 0L : owner.fencingToken(),
                null, null, null);
    }
}
