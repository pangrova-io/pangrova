package org.pangrova.metadata.coordination;
import org.pangrova.persistence.jdbc.query.transaction.JdbcWriteCommitJournal;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.metadata.testinfra.SharedPostgresContainer;
import org.pangrova.spi.connector.coordination.WriteCommitEntry;
import org.pangrova.spi.connector.coordination.WriteCommitFingerprint;
import org.pangrova.spi.connector.coordination.JournalWriteException;
import org.pangrova.spi.connector.coordination.NativeWriteFinalization;
import org.pangrova.testing.testkit.ContainerTestBase;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
    void committedStateAndRedoPayloadSurviveJdbcRoundTrip() {
        journal.recordIntent(pending("query-1", 1_000L));

        journal.markCommitted("query-1");

        WriteCommitEntry committed = journal.getEntry("query-1").orElseThrow();
        assertThat(committed.state()).isEqualTo(WriteCommitEntry.State.COMMITTED);
        assertThat(committed.fragments()).singleElement().isEqualTo(new byte[]{1, 2, 3});
        assertThat(journal.replayPendingEntries(Set.of("query-1"))).isEmpty();
    }

    @Test
    void publicationSurvivesReopeningAndCatalogCommitWithoutChangingIntent() throws Exception {
        var intent = pending("published", 1_000L);
        journal.recordIntent(intent);
        byte[] digest = new byte[32];
        digest[0] = 17;
        var publication = new NativeWriteFinalization(NativeWriteFinalization.Outcome.COMMITTED, 43, digest);
        journal.recordPublication(intent.queryId(), intent.commitFingerprint(), publication);

        var reopened = new JdbcWriteCommitJournal(dataSource);
        var reloaded = reopened.getEntry(intent.queryId()).orElseThrow();
        assertThat(reloaded.publication()).isEqualTo(publication);
        assertThat(reloaded.commitFingerprint()).isEqualTo(intent.commitFingerprint());
        assertThat(reloaded.state()).isEqualTo(WriteCommitEntry.State.PENDING);
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
                NativeWriteFinalization.Outcome.COMMITTED, 43, new byte[32]);
        journal.recordPublication(intent.queryId(), intent.commitFingerprint(), publication);
        assertThatThrownBy(() -> journal.recordPublication("missing", intent.commitFingerprint(), publication))
                .isInstanceOf(JournalWriteException.class);
        assertThatThrownBy(() -> journal.recordPublication(intent.queryId(), "0".repeat(64), publication))
                .isInstanceOf(JournalWriteException.class);
        assertThatThrownBy(() -> journal.recordPublication(intent.queryId(), intent.commitFingerprint(),
                new NativeWriteFinalization(NativeWriteFinalization.Outcome.COMMITTED, 44, new byte[32])))
                .isInstanceOf(JournalWriteException.class);
        byte[] changed = new byte[32];
        changed[0] = 1;
        assertThatThrownBy(() -> journal.recordPublication(intent.queryId(), intent.commitFingerprint(),
                new NativeWriteFinalization(NativeWriteFinalization.Outcome.COMMITTED, 43, changed)))
                .isInstanceOf(JournalWriteException.class);
        assertThat(journal.getEntry(intent.queryId()).orElseThrow().publication()).isEqualTo(publication);
    }

    @Test
    void uncertainPublicationNeverBecomesDurableEvidence() {
        var intent = pending("unknown-publication", 1_000L);
        journal.recordIntent(intent);
        assertThatThrownBy(() -> journal.recordPublication(intent.queryId(), intent.commitFingerprint(),
                new NativeWriteFinalization(NativeWriteFinalization.Outcome.UNKNOWN, 0, new byte[0])))
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
                    new NativeWriteFinalization(NativeWriteFinalization.Outcome.COMMITTED, 43, digest));
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
        assertThat(aborted.state()).isEqualTo(WriteCommitEntry.State.ABORTED);
        assertThat(aborted.fragments()).singleElement().isEqualTo(new byte[]{1, 2, 3});
        assertThat(aborted.commitFingerprint()).isEqualTo(intent.commitFingerprint());
        assertThat(reopened.replayPendingEntries(Set.of(intent.queryId()))).isEmpty();
        assertThatThrownBy(() -> reopened.recordIntent(intent)).isInstanceOf(JournalWriteException.class);
        assertThatThrownBy(() -> reopened.markCommitted(intent.queryId())).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> reopened.recordPublication(intent.queryId(), intent.commitFingerprint(),
                new NativeWriteFinalization(NativeWriteFinalization.Outcome.COMMITTED, 43, new byte[32])))
                .isInstanceOf(JournalWriteException.class);
        reopened.purge(intent.queryId());
        assertThat(reopened.getEntry(intent.queryId())).isPresent();
    }

    @Test
    void abortCannotEraseAlreadyPublishedPendingWriteOrInventMissingEvidence() {
        var intent = pending("published-before-abort", 1_000L);
        journal.recordIntent(intent);
        var publication = new NativeWriteFinalization(
                NativeWriteFinalization.Outcome.COMMITTED, 43, new byte[32]);
        journal.recordPublication(intent.queryId(), intent.commitFingerprint(), publication);
        assertThatThrownBy(() -> journal.markAborted(intent.queryId(), intent.commitFingerprint()))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> journal.markAborted("missing", intent.commitFingerprint()))
                .isInstanceOf(IllegalStateException.class);
        assertThat(journal.getEntry(intent.queryId()).orElseThrow().publication()).isEqualTo(publication);
        assertThat(journal.getEntry(intent.queryId()).orElseThrow().state()).isEqualTo(WriteCommitEntry.State.PENDING);
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
                    ? WriteCommitEntry.State.ABORTED : WriteCommitEntry.State.PENDING);
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

    private static WriteCommitEntry pending(String queryId, long timestampMs) {
        var fragments = List.of(new byte[]{1, 2, 3});
        String fingerprint = WriteCommitFingerprint.compute(
                new org.pangrova.spi.connector.coordination.WriteCommitFingerprint.Intent(
                        queryId,
                        "tenant-a",
                        "iceberg",
                        "analytics.events",
                        "OVERWRITE",
                        42L,
                        new byte[]{42},
                        "alice",
                        org.pangrova.metadata.testinfra.MetadataWriteReceiptFixtures.receipt(), null),
                fragments);
        return new WriteCommitEntry(
                queryId,
                "iceberg",
                "analytics.events",
                WriteCommitEntry.State.PENDING,
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
                7L,
                null, null);
    }
}
