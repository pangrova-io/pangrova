package org.pangrova.coordinator.query.task;

import org.pangrova.coordinator.coordination.inmemory.InMemoryWriteCommitJournal;
import org.pangrova.spi.coordination.WriteCommitEntry;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies {@link WriteCommitRecoveryService} — the production consumer of pending
 * write-commit intents on startup/promotion (A-COMMIT-01). Covers every terminal outcome,
 * reconciler-failure quarantine, and idempotency against already-committed intents.
 */
class WriteCommitRecoveryServiceTest {

    private static WriteCommitEntry pending(String queryId) {
        return new WriteCommitEntry(queryId, "iceberg", "analytics.events",
                WriteCommitEntry.State.PENDING, System.currentTimeMillis(),
                List.of(new byte[]{1, 2, 3}), "OVERWRITE", queryId, "coord-A", 1L);
    }

    @Test
    void committedOutcomeMarksEntryCommitted() {
        var journal = new InMemoryWriteCommitJournal();
        journal.recordIntent(pending("q1"));

        var report = new WriteCommitRecoveryService(journal,
                e -> WriteCommitReconciler.RecoveryOutcome.COMMITTED).recover();

        assertThat(report.pending()).isEqualTo(1);
        assertThat(report.committed()).isEqualTo(1);
        assertThat(journal.isCommitted("q1")).isTrue();
    }

    @Test
    void abortedOutcomePurgesEntry() {
        var journal = new InMemoryWriteCommitJournal();
        journal.recordIntent(pending("q1"));

        var report = new WriteCommitRecoveryService(journal,
                e -> WriteCommitReconciler.RecoveryOutcome.ABORTED).recover();

        assertThat(report.aborted()).isEqualTo(1);
        assertThat(journal.getEntry("q1")).isEmpty();
    }

    @Test
    void quarantinedOutcomeRetainsEntryForManualReconciliation() {
        var journal = new InMemoryWriteCommitJournal();
        journal.recordIntent(pending("q1"));

        var report = new WriteCommitRecoveryService(journal,
                e -> WriteCommitReconciler.RecoveryOutcome.QUARANTINED).recover();

        assertThat(report.quarantined()).isEqualTo(1);
        // Intent retained (still PENDING) for forensics / manual action.
        assertThat(journal.getEntry("q1")).isPresent();
        assertThat(journal.getEntry("q1").get().state())
                .isEqualTo(WriteCommitEntry.State.PENDING);
    }

    @Test
    void reconcilerExceptionIsQuarantinedNotPropagated() {
        var journal = new InMemoryWriteCommitJournal();
        journal.recordIntent(pending("q1"));

        var report = new WriteCommitRecoveryService(journal, e -> {
            throw new RuntimeException("catalog unreachable");
        }).recover();

        assertThat(report.quarantined()).isEqualTo(1);
        assertThat(journal.getEntry("q1")).isPresent();
    }

    @Test
    void alreadyCommittedEntriesAreSkippedForIdempotency() {
        var journal = new InMemoryWriteCommitJournal();
        journal.recordIntent(pending("q1"));
        journal.markCommitted("q1");   // already terminal
        var reconcileCount = new AtomicInteger();

        var report = new WriteCommitRecoveryService(journal, e -> {
            reconcileCount.incrementAndGet();
            return WriteCommitReconciler.RecoveryOutcome.COMMITTED;
        }).recover();

        // COMMITTED entries are excluded from replayPendingEntries(), so the reconciler is
        // never invoked — re-running recovery cannot double-commit.
        assertThat(reconcileCount.get()).isZero();
        assertThat(report.pending()).isZero();
    }

    @Test
    void mixedOutcomesAreTalliedAndEmptyJournalIsNoOp() {
        var empty = new InMemoryWriteCommitJournal();
        assertThat(new WriteCommitRecoveryService(empty,
                e -> WriteCommitReconciler.RecoveryOutcome.COMMITTED).recover().pending()).isZero();

        var journal = new InMemoryWriteCommitJournal();
        journal.recordIntent(pending("commit-me"));
        journal.recordIntent(pending("abort-me"));
        journal.recordIntent(pending("hold-me"));

        var report = new WriteCommitRecoveryService(journal, e -> switch (e.queryId()) {
            case "commit-me" -> WriteCommitReconciler.RecoveryOutcome.COMMITTED;
            case "abort-me" -> WriteCommitReconciler.RecoveryOutcome.ABORTED;
            default -> WriteCommitReconciler.RecoveryOutcome.QUARANTINED;
        }).recover();

        assertThat(report.pending()).isEqualTo(3);
        assertThat(report.committed()).isEqualTo(1);
        assertThat(report.aborted()).isEqualTo(1);
        assertThat(report.quarantined()).isEqualTo(1);
        assertThat(journal.isCommitted("commit-me")).isTrue();
        assertThat(journal.getEntry("abort-me")).isEmpty();
        assertThat(journal.getEntry("hold-me")).isPresent();
    }
}
