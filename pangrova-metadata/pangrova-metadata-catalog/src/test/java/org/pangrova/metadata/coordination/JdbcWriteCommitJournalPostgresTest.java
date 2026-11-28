package org.pangrova.metadata.coordination;

import org.pangrova.connector.api.enums.NativeWriteCommitOutcome;
import org.pangrova.persistence.query.api.enums.WriteCommitState;
import org.pangrova.persistence.query.postgresql.transaction.JdbcWriteCommitJournal;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.testing.testkit.postgres.SharedPostgresContainer;
import org.pangrova.persistence.query.api.commit.CdcSourceIdentity;
import org.pangrova.persistence.query.api.commit.CdcWriteCheckpoint;
import org.pangrova.persistence.query.api.commit.WriteCommitEntry;
import org.pangrova.persistence.query.api.commit.WriteCommitFingerprint;
import org.pangrova.persistence.query.api.exception.JournalWriteException;
import org.pangrova.connector.api.commit.NativeWriteFinalization;
import org.pangrova.testing.testkit.ContainerTestBase;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * PostgreSQL contract tests for the durable fenced write journal.
 */
class JdbcWriteCommitJournalPostgresTest extends ContainerTestBase {

    private JdbcWriteCommitJournal journal;
    private javax.sql.DataSource dataSource;

    @BeforeEach
    void setUp() throws Exception {
        dataSource = SharedPostgresContainer.migratedDataSource("write_commit");
        journal = new JdbcWriteCommitJournal(dataSource);
    }

    @Test
    void roundTripsFencedRedoRecordAndScopesRecoveryToClaimedExecutions() {
        journal.recordIntent(pending("claimed", 1_000L));
        journal.recordIntent(pending("live-peer", 2_000L));

        WriteCommitEntry reloaded = journal.getEntry("claimed").orElseThrow();
        assertThat(reloaded.ownerId()).isEqualTo("coordinator-a");
        assertThat(reloaded.ownerIncarnation()).isEqualTo("process-a");
        assertThat(reloaded.executionFencingToken()).isEqualTo(7L);
        assertThat(reloaded.fragments()).singleElement().isEqualTo(new byte[]{1, 2, 3});

        assertThat(journal.replayPendingEntries(Set.of("claimed")))
                .extracting(WriteCommitEntry::queryId)
                .containsExactly("claimed");
        assertThat(journal.getEntry("live-peer")).isPresent();
    }

    @Test
    void concurrentExactAdmissionKeepsTheFirstRedoAndReturnsItsState() throws Exception {
        var first = pending("admission-race", 1_000L);
        var duplicate = pending(first.queryId(), 2_000L);
        var bothReady = new java.util.concurrent.CountDownLatch(2);
        var threads = Executors.newFixedThreadPool(2);
        try {
            var left = threads.submit(() -> admitTogether(first, bothReady));
            var right = threads.submit(() -> admitTogether(duplicate, bothReady));
            assertThat(left.get(10, TimeUnit.SECONDS)).isEqualTo(WriteCommitState.PENDING);
            assertThat(right.get(10, TimeUnit.SECONDS)).isEqualTo(WriteCommitState.PENDING);
            long originalTimestamp = journal.getEntry(first.queryId()).orElseThrow().timestampMs();
            assertThat(originalTimestamp).isIn(first.timestampMs(), duplicate.timestampMs());
            assertThat(journal.recordIntent(pending(first.queryId(), 3_000L))).isEqualTo(WriteCommitState.PENDING);
            assertThat(journal.getEntry(first.queryId()).orElseThrow().timestampMs()).isEqualTo(originalTimestamp);
            journal.markCommitted(first.queryId());
            assertThat(journal.recordIntent(duplicate)).isEqualTo(WriteCommitState.COMMITTED);
        } finally {
            threads.shutdownNow();
            assertThat(threads.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    private WriteCommitState admitTogether(WriteCommitEntry intent, java.util.concurrent.CountDownLatch bothReady)
            throws InterruptedException {
        bothReady.countDown();
        if (!bothReady.await(5, TimeUnit.SECONDS)) {
            throw new IllegalStateException("Concurrent journal admission did not start");
        }
        return journal.recordIntent(intent);
    }

    @Test
    void committedStateAndRedoPayloadSurviveJdbcRoundTrip() {
        journal.recordIntent(pending("query-1", 1_000L));

        journal.markCommitted("query-1");

        WriteCommitEntry committed = journal.getEntry("query-1").orElseThrow();
        assertThat(committed.state()).isEqualTo(WriteCommitState.COMMITTED);
        assertThat(committed.fragments()).singleElement().isEqualTo(new byte[]{1, 2, 3});
        assertThat(journal.replayPendingEntries(Set.of("query-1"))).isEmpty();
    }

    @Test
    void compactTerminalReceiptSurvivesRedoRetirementAndRejectsReadmission() {
        var intent = pending("compact-receipt", 1_000L);
        journal.recordIntent(intent);
        assertThat(journal.getReceipt(intent.queryId())).isEmpty();
        var publication = new NativeWriteFinalization(NativeWriteCommitOutcome.COMMITTED, 43, new byte[32]);
        journal.recordPublication(intent.queryId(), intent.commitFingerprint(), publication);
        journal.markCommitted(intent.queryId());
        var receipt = journal.getReceipt(intent.queryId()).orElseThrow();
        assertThat(receipt.publication()).isEqualTo(publication);
        assertThat(receipt.commitFingerprint()).isEqualTo(intent.commitFingerprint());
        assertThat(receipt.tenantId()).isEqualTo(intent.tenantId());
        journal.acknowledgeNoDeferredWork(intent.commitFingerprint());
        journal.purge(intent.queryId());

        var reopened = new JdbcWriteCommitJournal(dataSource);
        assertThat(reopened.getEntry(intent.queryId())).isEmpty();
        assertThat(reopened.getReceipt(intent.queryId())).contains(receipt);
        assertThat(reopened.isCommitted(intent.queryId())).isTrue();
        reopened.markCommitted(intent.queryId());
        assertThat(reopened.recordIntent(intent)).isEqualTo(WriteCommitState.COMMITTED);
        assertThat(reopened.getEntry(intent.queryId())).isEmpty();
        assertThat(reopened.getReceipt(intent.queryId())).contains(receipt);
        assertThatThrownBy(() -> {
            try (var connection = dataSource.getConnection(); var statement = connection.prepareStatement(
                    "DELETE FROM write_commit_receipts WHERE query_id=?")) {
                statement.setString(1, intent.queryId());
                statement.executeUpdate();
            }
        }).hasMessageContaining("Write commit receipt is immutable");
    }

    @Test
    void migrationBackfillsOnlyExistingJournalIdentityAndTerminalEvidence() throws Exception {
        var upgradeSource = SharedPostgresContainer.isolatedDataSource("receipt_upgrade");
        org.flywaydb.core.Flyway.configure().dataSource(upgradeSource)
                .locations("classpath:db/migration").target("142").load().migrate();
        var pending = pending("upgrade-pending", 1_000L);
        var committed = pending("upgrade-committed", 2_000L);
        var aborted = pending("upgrade-aborted", 3_000L);
        var publication = new NativeWriteFinalization(NativeWriteCommitOutcome.COMMITTED, 43, new byte[32]);
        try (var connection = upgradeSource.getConnection(); var statement = connection.prepareStatement("""
                INSERT INTO write_commit_journal
                    (query_id, catalog_name, table_name, state, timestamp_ms, fragments, write_mode,
                     idempotency_key, execution_fencing_token, tenant_id, base_snapshot_id, commit_fingerprint,
                     write_output_binding, published_version, manifest_sha256)
                VALUES (?, ?, ?, ?, ?, 'AQID', ?, ?, 0, ?, ?, ?, 'Kg==', ?, ?)
                """)) {
            for (var intent : List.of(pending, committed, aborted)) {
                boolean published = intent == committed;
                var state = published ? WriteCommitState.COMMITTED
                        : intent == aborted ? WriteCommitState.ABORTED : WriteCommitState.PENDING;
                statement.setString(1, intent.queryId());
                statement.setString(2, intent.catalogName());
                statement.setString(3, intent.tableName());
                statement.setString(4, state.name());
                statement.setLong(5, intent.timestampMs());
                statement.setString(6, intent.writeMode());
                statement.setString(7, intent.idempotencyKey());
                statement.setString(8, intent.tenantId());
                statement.setLong(9, intent.baseSnapshotId());
                statement.setString(10, intent.commitFingerprint());
                if (published) {
                    statement.setLong(11, publication.publishedVersion());
                    statement.setBytes(12, publication.manifestSha256());
                } else {
                    statement.setNull(11, java.sql.Types.BIGINT);
                    statement.setNull(12, java.sql.Types.BINARY);
                }
                statement.addBatch();
            }
            statement.executeBatch();
        }

        org.flywaydb.core.Flyway.configure().dataSource(upgradeSource)
                .locations("classpath:db/migration").load().migrate();
        var upgraded = new JdbcWriteCommitJournal(upgradeSource);
        assertThat(upgraded.getReceipt(pending.queryId())).isEmpty();
        assertThat(upgraded.getReceipt(committed.queryId())).get().satisfies(receipt -> {
            assertThat(receipt.state()).isEqualTo(WriteCommitState.COMMITTED);
            assertThat(receipt.publication()).isEqualTo(publication);
            assertThat(receipt.commitFingerprint()).isEqualTo(committed.commitFingerprint());
        });
        assertThat(upgraded.getReceipt(aborted.queryId())).get().satisfies(receipt -> {
            assertThat(receipt.state()).isEqualTo(WriteCommitState.ABORTED);
            assertThat(receipt.commitFingerprint()).isEqualTo(aborted.commitFingerprint());
        });
        upgraded.markCommitted(pending.queryId());
        assertThat(upgraded.isCommitted(pending.queryId())).isTrue();
    }

    @Test
    void sourceTransactionCannotAttachNewEvidenceToATerminalReceiptButMayRepeatExactEvidence() throws Exception {
        var missing = pending("terminal-without-proof", 1_000L);
        var published = pending("terminal-with-proof", 2_000L);
        var publication = new NativeWriteFinalization(NativeWriteCommitOutcome.COMMITTED, 43, new byte[32]);
        journal.recordIntent(missing);
        journal.markCommitted(missing.queryId());
        journal.recordIntent(published);
        try (var connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            var terminal = JdbcWriteCommitJournal.lockIntent(connection, missing.queryId());
            assertThatThrownBy(() -> JdbcWriteCommitJournal.commitPublication(connection, terminal, publication))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("conflicts with intent");
            connection.rollback();
            var pending = JdbcWriteCommitJournal.lockIntent(connection, published.queryId());
            JdbcWriteCommitJournal.commitPublication(connection, pending, publication);
            connection.commit();
            var exact = JdbcWriteCommitJournal.lockIntent(connection, published.queryId());
            JdbcWriteCommitJournal.commitPublication(connection, exact, publication);
            connection.commit();
        }
        assertThat(journal.getEntry(missing.queryId()).orElseThrow().publication()).isNull();
        assertThat(journal.getReceipt(missing.queryId()).orElseThrow().publication()).isNull();
        assertThat(journal.getEntry(published.queryId()).orElseThrow().publication()).isEqualTo(publication);
        assertThat(journal.getReceipt(published.queryId()).orElseThrow().publication()).isEqualTo(publication);
    }

    @Test
    void receiptPublicationRollsBackWithTheSourceTransaction() throws Exception {
        var intent = pending("transaction-receipt", 1_000L);
        journal.recordIntent(intent);
        var publication = new NativeWriteFinalization(NativeWriteCommitOutcome.COMMITTED, 43, new byte[32]);
        try (var connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            var locked = JdbcWriteCommitJournal.lockIntent(connection, intent.queryId());
            JdbcWriteCommitJournal.commitPublication(connection, locked, publication);
            try (var statement = connection.prepareStatement(
                    "SELECT state FROM write_commit_receipts WHERE query_id=?")) {
                statement.setString(1, intent.queryId());
                try (var rows = statement.executeQuery()) {
                    assertThat(rows.next()).isTrue();
                    assertThat(rows.getString(1)).isEqualTo("COMMITTED");
                }
            }
            connection.rollback();
        }
        assertThat(journal.getReceipt(intent.queryId())).isEmpty();
        assertThat(journal.getEntry(intent.queryId())).get().satisfies(restored -> {
            assertThat(restored.state()).isEqualTo(WriteCommitState.PENDING);
            assertThat(restored.commitFingerprint()).isEqualTo(intent.commitFingerprint());
            assertThat(restored.fragments()).singleElement().isEqualTo(intent.fragments().get(0));
            assertThat(restored.writeOutputBinding()).isEqualTo(intent.writeOutputBinding());
            assertThat(restored.publication()).isNull();
        });
    }

    @Test
    void cdcCheckpointPrefersPendingAndIsScopedToTheExactStreamGeneration() {
        var source = new CdcSourceIdentity("orders", "stream:orders-v1",
                List.of(new CdcSourceIdentity.OffsetRange("source", 0, 0, 10)));
        var olderPending = pending("cdc-pending", 1_000L, source);
        var confirmed = pending("cdc-confirmed", 2_000L, source);
        journal.recordIntent(olderPending);
        journal.recordIntent(confirmed);
        journal.markCommitted(confirmed.queryId());
        var reopened = new JdbcWriteCommitJournal(dataSource);
        assertThat(reopened.findCdcCheckpoint("tenant-a", "orders", "stream:orders-v1"))
                .contains(new CdcWriteCheckpoint(
                        olderPending.queryId(), olderPending.commitFingerprint(), WriteCommitState.PENDING, source));
        assertThat(reopened.findCdcCheckpoint("tenant-a", "orders", "stream:orders-v2")).isEmpty();
        assertThat(reopened.findCdcCheckpoint("tenant-b", "orders", "stream:orders-v1")).isEmpty();

        journal.markAborted(olderPending.queryId(), olderPending.commitFingerprint());
        assertThat(reopened.findCdcCheckpoint("tenant-a", "orders", "stream:orders-v1"))
                .contains(new CdcWriteCheckpoint(
                        confirmed.queryId(), confirmed.commitFingerprint(), WriteCommitState.COMMITTED, source));
    }

    @Test
    void publicationSurvivesReopeningAndCatalogCommitWithoutChangingIntent() throws Exception {
        var intent = pending("published", 1_000L);
        journal.recordIntent(intent);
        byte[] digest = new byte[32];
        digest[0] = 17;
        var publication = new NativeWriteFinalization(NativeWriteCommitOutcome.COMMITTED, 43, digest);
        journal.recordPublication(intent.queryId(), intent.commitFingerprint(), publication);

        var reopened = new JdbcWriteCommitJournal(dataSource);
        var reloaded = reopened.getEntry(intent.queryId()).orElseThrow();
        assertThat(reloaded.publication()).isEqualTo(publication);
        assertThat(reloaded.commitFingerprint()).isEqualTo(intent.commitFingerprint());
        assertThat(reloaded.state()).isEqualTo(WriteCommitState.PENDING);
        reopened.recordPublication(intent.queryId(), intent.commitFingerprint(), publication);
        reopened.recordIntent(intent);
        reopened.markCommitted(intent.queryId());
        assertThat(journal.getEntry(intent.queryId()).orElseThrow().publication()).isEqualTo(publication);
    }

    @Test
    void differentVersionDigestFingerprintOrMissingIntentCannotOverwritePublication() {
        var intent = pending("publication-fence", 1_000L);
        journal.recordIntent(intent);
        var publication = new NativeWriteFinalization(
                NativeWriteCommitOutcome.COMMITTED, 43, new byte[32]);
        journal.recordPublication(intent.queryId(), intent.commitFingerprint(), publication);
        assertThatThrownBy(() -> journal.recordPublication("missing", intent.commitFingerprint(), publication))
                .isInstanceOf(JournalWriteException.class);
        assertThatThrownBy(() -> journal.recordPublication(intent.queryId(), "0".repeat(64), publication))
                .isInstanceOf(JournalWriteException.class);
        assertThatThrownBy(() -> journal.recordPublication(intent.queryId(), intent.commitFingerprint(),
                new NativeWriteFinalization(NativeWriteCommitOutcome.COMMITTED, 44, new byte[32])))
                .isInstanceOf(JournalWriteException.class);
        byte[] changed = new byte[32];
        changed[0] = 1;
        assertThatThrownBy(() -> journal.recordPublication(intent.queryId(), intent.commitFingerprint(),
                new NativeWriteFinalization(NativeWriteCommitOutcome.COMMITTED, 43, changed)))
                .isInstanceOf(JournalWriteException.class);
        assertThat(journal.getEntry(intent.queryId()).orElseThrow().publication()).isEqualTo(publication);
    }

    @Test
    void uncertainPublicationNeverBecomesDurableEvidence() {
        var intent = pending("unknown-publication", 1_000L);
        journal.recordIntent(intent);
        assertThatThrownBy(() -> journal.recordPublication(intent.queryId(), intent.commitFingerprint(),
                new NativeWriteFinalization(NativeWriteCommitOutcome.UNKNOWN, 0, new byte[0])))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(journal.getEntry(intent.queryId()).orElseThrow().publication()).isNull();
    }

    @Test
    void concurrentDifferentManifestEvidenceHasExactlyOneWinner() throws Exception {
        var intent = pending("concurrent-publication", 1_000L);
        journal.recordIntent(intent);
        var executor = java.util.concurrent.Executors.newFixedThreadPool(2);
        var start = new java.util.concurrent.CountDownLatch(1);
        try {
            var first = executor.submit(() -> publishAfter(start, intent, (byte) 1));
            var second = executor.submit(() -> publishAfter(start, intent, (byte) 2));
            start.countDown();
            boolean firstWon = first.get(10, java.util.concurrent.TimeUnit.SECONDS);
            boolean secondWon = second.get(10, java.util.concurrent.TimeUnit.SECONDS);
            assertThat(firstWon).isNotEqualTo(secondWon);
            assertThat(journal.getEntry(intent.queryId()).orElseThrow().publication().manifestSha256()[0])
                    .isEqualTo(firstWon ? (byte) 1 : (byte) 2);
        } finally {
            executor.shutdownNow();
        }
    }

    private boolean publishAfter(java.util.concurrent.CountDownLatch start, WriteCommitEntry intent, byte value)
            throws InterruptedException {
        if (!start.await(10, java.util.concurrent.TimeUnit.SECONDS)) {
            throw new IllegalStateException("Concurrent publication start timed out");
        }
        byte[] digest = new byte[32];
        digest[0] = value;
        try {
            new JdbcWriteCommitJournal(dataSource).recordPublication(intent.queryId(), intent.commitFingerprint(),
                    new NativeWriteFinalization(NativeWriteCommitOutcome.COMMITTED, 43, digest));
            return true;
        } catch (JournalWriteException conflict) {
            if (conflict.getCause() instanceof IllegalStateException) return false;
            throw conflict;
        }
    }

    @Test
    void abortBarrierSurvivesReopeningAndRejectsLateIntentPublicationOrCommit() {
        var intent = pending("aborted-barrier", 1_000L);
        journal.recordIntent(intent);
        journal.markAborted(intent.queryId(), intent.commitFingerprint());
        var reopened = new JdbcWriteCommitJournal(dataSource);
        reopened.markAborted(intent.queryId(), intent.commitFingerprint());
        var aborted = reopened.getEntry(intent.queryId()).orElseThrow();
        assertThat(aborted.state()).isEqualTo(WriteCommitState.ABORTED);
        assertThat(reopened.getReceipt(intent.queryId()).orElseThrow().state()).isEqualTo(WriteCommitState.ABORTED);
        assertThat(aborted.fragments()).singleElement().isEqualTo(new byte[]{1, 2, 3});
        assertThat(aborted.commitFingerprint()).isEqualTo(intent.commitFingerprint());
        assertThat(reopened.replayPendingEntries(Set.of(intent.queryId()))).isEmpty();
        assertThatThrownBy(() -> reopened.recordIntent(intent)).isInstanceOf(JournalWriteException.class);
        assertThatThrownBy(() -> reopened.markCommitted(intent.queryId())).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> reopened.recordPublication(intent.queryId(), intent.commitFingerprint(),
                new NativeWriteFinalization(NativeWriteCommitOutcome.COMMITTED, 43, new byte[32])))
                .isInstanceOf(JournalWriteException.class);
        reopened.purge(intent.queryId());
        assertThat(reopened.getEntry(intent.queryId())).isPresent();
    }

    @Test
    void abortCannotEraseAlreadyPublishedPendingWriteOrInventMissingEvidence() {
        var intent = pending("published-before-abort", 1_000L);
        journal.recordIntent(intent);
        var publication = new NativeWriteFinalization(
                NativeWriteCommitOutcome.COMMITTED, 43, new byte[32]);
        journal.recordPublication(intent.queryId(), intent.commitFingerprint(), publication);
        assertThatThrownBy(() -> journal.markAborted(intent.queryId(), intent.commitFingerprint()))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> journal.markAborted("missing", intent.commitFingerprint()))
                .isInstanceOf(IllegalStateException.class);
        assertThat(journal.getEntry(intent.queryId()).orElseThrow().publication()).isEqualTo(publication);
        assertThat(journal.getEntry(intent.queryId()).orElseThrow().state()).isEqualTo(WriteCommitState.PENDING);
    }

    @Test
    void concurrentAbortAndPublicationHaveExactlyOneDurableWinner() throws Exception {
        var intent = pending("abort-publication-race", 1_000L);
        journal.recordIntent(intent);
        var threads = java.util.concurrent.Executors.newFixedThreadPool(2,
                runnable -> new Thread(runnable, "write-abort-publication-race"));
        var start = new java.util.concurrent.CountDownLatch(1);
        try {
            var published = threads.submit(() -> publishAfter(start, intent, (byte) 17));
            var aborted = threads.submit(() -> abortAfter(start, intent));
            start.countDown();
            boolean publicationWon = published.get(10, java.util.concurrent.TimeUnit.SECONDS);
            boolean abortWon = aborted.get(10, java.util.concurrent.TimeUnit.SECONDS);
            assertThat(publicationWon).isNotEqualTo(abortWon);
            var stored = new JdbcWriteCommitJournal(dataSource).getEntry(intent.queryId()).orElseThrow();
            assertThat(stored.state()).isEqualTo(abortWon
                    ? WriteCommitState.ABORTED : WriteCommitState.PENDING);
            if (abortWon) assertThat(stored.publication()).isNull();
            else assertThat(stored.publication().manifestSha256()[0]).isEqualTo((byte) 17);
        } finally {
            threads.shutdownNow();
            assertThat(threads.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
        }
    }

    private boolean abortAfter(java.util.concurrent.CountDownLatch start, WriteCommitEntry intent)
            throws InterruptedException {
        if (!start.await(10, java.util.concurrent.TimeUnit.SECONDS)) {
            throw new IllegalStateException("Concurrent abort start timed out");
        }
        try {
            new JdbcWriteCommitJournal(dataSource).markAborted(intent.queryId(), intent.commitFingerprint());
            return true;
        } catch (IllegalStateException publicationWon) {
            assertThat(publicationWon).hasMessageContaining("Abort does not match");
            return false;
        }
    }

    @Test
    void fencedOutcomeRejectsPreviousOwnerAndRetainsExactReceiptAcrossRedoRetirement() {
        var leases = new org.pangrova.persistence.query.postgresql.coordination.JdbcExecutionAnchorStore(dataSource);
        var lifetime = Duration.ofMinutes(1);
        var original = leases.tryAcquire("fenced-terminal", "coordinator-a", "process-a", lifetime).orElseThrow();
        var intent = pending(original.executionId(), 1_000L, null, original.fencingToken());
        journal.recordIntent(intent);
        assertThat(leases.yield(original)).isTrue();
        var successor = leases.tryAcquire(intent.queryId(), "coordinator-b", "process-b", lifetime).orElseThrow();

        assertThat(journal.recordTerminalOutcome(intent, original.fencingToken(), WriteCommitState.COMMITTED))
                .isFalse();
        assertThat(journal.recordTerminalOutcome(intent, original.fencingToken(), WriteCommitState.ABORTED))
                .isFalse();
        assertThatThrownBy(() -> journal.markCommitted(intent.queryId())).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> journal.markAborted(intent.queryId(), intent.commitFingerprint()))
                .isInstanceOf(IllegalStateException.class);
        assertThat(journal.getEntry(intent.queryId())).get().extracting(WriteCommitEntry::state)
                .isEqualTo(WriteCommitState.PENDING);

        assertThat(journal.recordTerminalOutcome(intent, successor.fencingToken(), WriteCommitState.COMMITTED))
                .isTrue();
        assertThatThrownBy(() -> journal.recordTerminalOutcome(intent, successor.fencingToken(), WriteCommitState.ABORTED))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Conflicting terminal write outcome");
        journal.acknowledgeNoDeferredWork(intent.commitFingerprint());
        assertThat(leases.release(successor)).isTrue();
        journal.purge(intent.queryId());
        assertThat(journal.getEntry(intent.queryId())).isEmpty();
        assertThat(journal.getReceipt(intent.queryId()).orElseThrow().commitFingerprint())
                .isEqualTo(intent.commitFingerprint());
        assertThat(journal.recordTerminalOutcome(intent, successor.fencingToken(), WriteCommitState.COMMITTED))
                .isFalse();
    }

    @Test
    void releaseKeepsAnUnresolvedWriteClaimableUntilCommitIsConfirmed() {
        var leases = new org.pangrova.persistence.query.postgresql.coordination.JdbcExecutionAnchorStore(dataSource);
        var lifetime = java.time.Duration.ofMinutes(1);
        var original = leases.tryAcquire("pending-after-query", "coordinator-a", "process-a", lifetime).orElseThrow();
        journal.recordIntent(pending(original.executionId(), System.currentTimeMillis()));

        assertThat(leases.release(original)).isTrue();
        assertThat(leases.current(original.executionId())).isEmpty();
        var recovered = leases.claimExpired("coordinator-b", "process-b", lifetime, 1);
        assertThat(recovered).singleElement().satisfies(anchor -> {
            assertThat(anchor.executionId()).isEqualTo(original.executionId());
            assertThat(anchor.fencingToken()).isGreaterThan(original.fencingToken());
        });
        assertThat(leases.release(original)).isFalse();
        var intent = journal.getEntry(original.executionId()).orElseThrow();
        assertThat(journal.recordTerminalOutcome(intent, recovered.get(0).fencingToken(), WriteCommitState.COMMITTED))
                .isTrue();
        assertThat(leases.release(recovered.get(0))).isTrue();
        assertThat(leases.claimExpired("coordinator-c", "process-c", lifetime, 1)).isEmpty();
    }

    @Test
    void releaseSeesWriteIntentCommittedWhileWaitingForOwnerLock() throws Exception {
        var leases = new org.pangrova.persistence.query.postgresql.coordination.JdbcExecutionAnchorStore(dataSource);
        var lifetime = Duration.ofMinutes(1);
        var original = leases.tryAcquire("intent-during-release", "coordinator-a", "process-a", lifetime)
                .orElseThrow();
        var executor = Executors.newSingleThreadExecutor();
        try (var writer = dataSource.getConnection()) {
            writer.setAutoCommit(false);
            int writerPid;
            try (var lock = writer.prepareStatement("""
                    SELECT pg_backend_pid() FROM execution_anchor WHERE execution_id=? FOR UPDATE
                    """)) {
                lock.setString(1, original.executionId());
                try (var row = lock.executeQuery()) {
                    assertThat(row.next()).isTrue();
                    writerPid = row.getInt(1);
                }
            }
            var releasing = executor.submit(() -> leases.release(original));
            await().atMost(Duration.ofSeconds(10)).until(() -> waitsFor(writerPid));
            var intent = pending(original.executionId(), System.currentTimeMillis(), null, original.fencingToken());
            journal.recordIntent(intent);
            writer.commit();
            assertThat(releasing.get(10, TimeUnit.SECONDS)).isTrue();
            assertThat(leases.current(original.executionId())).isEmpty();
            assertThat(leases.claimExpired("coordinator-b", "process-b", lifetime, 1))
                    .singleElement().satisfies(anchor -> {
                        assertThat(anchor.executionId()).isEqualTo(intent.queryId());
                        assertThat(anchor.fencingToken()).isGreaterThan(original.fencingToken());
                    });
            assertThat(journal.replayPendingEntries(Set.of(intent.queryId())))
                    .singleElement().satisfies(entry -> {
                        assertThat(entry.commitFingerprint()).isEqualTo(intent.commitFingerprint());
                        assertThat(entry.executionFencingToken()).isEqualTo(original.fencingToken());
                    });
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    private boolean waitsFor(int holderPid) throws java.sql.SQLException {
        try (var observer = dataSource.getConnection(); var query = observer.prepareStatement("""
                SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE ?=ANY(pg_blocking_pids(pid)))
                """)) {
            query.setInt(1, holderPid);
            try (var result = query.executeQuery()) {
                return result.next() && result.getBoolean(1);
            }
        }
    }

    private static WriteCommitEntry pending(String queryId, long timestampMs) {
        return pending(queryId, timestampMs, null);
    }

    private static WriteCommitEntry pending(String queryId, long timestampMs, CdcSourceIdentity source) {
        return pending(queryId, timestampMs, source, 7L);
    }

    private static WriteCommitEntry pending(String queryId, long timestampMs, CdcSourceIdentity source, long token) {
        var fragments = List.of(new byte[]{1, 2, 3});
        String fingerprint = WriteCommitFingerprint.compute(
                new org.pangrova.persistence.query.api.commit.WriteCommitFingerprint.Intent(
                        queryId,
                        "tenant-a",
                        "iceberg",
                        "analytics.events",
                        "OVERWRITE",
                        42L,
                        new byte[]{42},
                        "alice",
                        org.pangrova.metadata.testinfra.MetadataWriteReceiptFixtures.receipt(), null, source),
                fragments);
        return new WriteCommitEntry(
                queryId,
                "iceberg",
                "analytics.events",
                WriteCommitState.PENDING,
                timestampMs,
                fragments,
                "OVERWRITE",
                fingerprint,
                "tenant-a",
                "alice",
                org.pangrova.metadata.testinfra.MetadataWriteReceiptFixtures.receipt(),
                42L,
                new byte[]{42},
                fingerprint,
                "coordinator-a",
                "process-a",
                token,
                null, null, source);
    }
}
