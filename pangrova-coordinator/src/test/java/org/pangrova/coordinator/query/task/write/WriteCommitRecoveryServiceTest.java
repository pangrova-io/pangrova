package org.pangrova.coordinator.query.task.write;

import org.pangrova.coordinator.coordination.inmemory.InMemoryWriteCommitJournal;
import org.pangrova.spi.coordination.WriteCommitEntry;
import org.pangrova.spi.coordination.WriteCommitFingerprint;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies {@link WriteCommitRecoveryService} — the production consumer of pending
 * write-commit intents on startup/promotion (A-COMMIT-01). Covers every terminal outcome,
 * reconciler-failure quarantine, and idempotency against already-committed intents.
 */
class WriteCommitRecoveryServiceTest {

    private static WriteCommitEntry pending(String queryId) {
        var fragments = List.of(new byte[]{1, 2, 3});
        String fingerprint = WriteCommitFingerprint.compute(
                new org.pangrova.spi.coordination.WriteCommitFingerprint.Intent(
                        queryId,
                        "tenant-a",
                        "iceberg",
                        "analytics.events",
                        "OVERWRITE",
                        19L,
                        new byte[]{42},
                        "alice",
                        org.pangrova.coordinator.testinfra.CoordinatorWriteReceiptFixtures.receipt(), null),
                fragments);
        return new WriteCommitEntry(
                queryId,
                "iceberg",
                "analytics.events",
                WriteCommitEntry.State.PENDING,
                System.currentTimeMillis(),
                fragments,
                "OVERWRITE",
                fingerprint,
                "tenant-a",
                "alice",
                org.pangrova.coordinator.testinfra.CoordinatorWriteReceiptFixtures.receipt(),
                19L,
                new byte[]{42},
                fingerprint,
                "coord-A",
                "process-A",
                1L,
                null, null);
    }

    @Test
    void committedOutcomeMarksEntryCommitted() {
        var journal = new InMemoryWriteCommitJournal();
        journal.recordIntent(pending("q1"));

        var report = recover(new WriteCommitRecoveryService(journal,
                e -> WriteCommitReconciler.RecoveryOutcome.COMMITTED), "q1");

        assertThat(report.pending()).isEqualTo(1);
        assertThat(report.committed()).isEqualTo(1);
        assertThat(journal.isCommitted("q1")).isTrue();
    }

    @Test
    void abortedOutcomeRetainsTerminalBarrier() {
        var journal = new InMemoryWriteCommitJournal();
        journal.recordIntent(pending("q1"));

        var report = recover(new WriteCommitRecoveryService(journal,
                e -> WriteCommitReconciler.RecoveryOutcome.ABORTED), "q1");

        assertThat(report.aborted()).isEqualTo(1);
        assertThat(journal.getEntry("q1")).get()
                .extracting(WriteCommitEntry::state).isEqualTo(WriteCommitEntry.State.ABORTED);
        assertThatThrownBy(() -> journal.recordIntent(pending("q1")))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void quarantinedOutcomeRetainsEntryForManualReconciliation() {
        var journal = new InMemoryWriteCommitJournal();
        journal.recordIntent(pending("q1"));

        var report = recover(new WriteCommitRecoveryService(journal,
                e -> WriteCommitReconciler.RecoveryOutcome.QUARANTINED), "q1");

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

        var report = recover(new WriteCommitRecoveryService(journal, e -> {
            throw new RuntimeException("catalog unreachable");
        }), "q1");

        assertThat(report.quarantined()).isEqualTo(1);
        assertThat(journal.getEntry("q1")).isPresent();
    }

    @Test
    void alreadyCommittedEntriesAreSkippedForIdempotency() {
        var journal = new InMemoryWriteCommitJournal();
        journal.recordIntent(pending("q1"));
        journal.markCommitted("q1");   // already terminal
        var reconcileCount = new AtomicInteger();

        var report = recover(new WriteCommitRecoveryService(journal, e -> {
            reconcileCount.incrementAndGet();
            return WriteCommitReconciler.RecoveryOutcome.COMMITTED;
        }), "q1");

        // COMMITTED entries are excluded from replayPendingEntries(), so the reconciler is
        // never invoked — re-running recovery cannot double-commit.
        assertThat(reconcileCount.get()).isZero();
        assertThat(report.pending()).isZero();
    }

    @Test
    void mixedOutcomesAreTalliedAndEmptyJournalIsNoOp() {
        var empty = new InMemoryWriteCommitJournal();
        assertThat(recover(new WriteCommitRecoveryService(empty,
                e -> WriteCommitReconciler.RecoveryOutcome.COMMITTED)).pending()).isZero();

        var journal = new InMemoryWriteCommitJournal();
        journal.recordIntent(pending("commit-me"));
        journal.recordIntent(pending("abort-me"));
        journal.recordIntent(pending("hold-me"));

        var report = recover(new WriteCommitRecoveryService(journal, e -> switch (e.queryId()) {
            case "commit-me" -> WriteCommitReconciler.RecoveryOutcome.COMMITTED;
            case "abort-me" -> WriteCommitReconciler.RecoveryOutcome.ABORTED;
            default -> WriteCommitReconciler.RecoveryOutcome.QUARANTINED;
        }), "commit-me", "abort-me", "hold-me");

        assertThat(report.pending()).isEqualTo(3);
        assertThat(report.committed()).isEqualTo(1);
        assertThat(report.aborted()).isEqualTo(1);
        assertThat(report.quarantined()).isEqualTo(1);
        assertThat(journal.isCommitted("commit-me")).isTrue();
        assertThat(journal.getEntry("abort-me")).get()
                .extracting(WriteCommitEntry::state).isEqualTo(WriteCommitEntry.State.ABORTED);
        assertThat(journal.getEntry("hold-me")).isPresent();
    }

    @Test
    void doesNotReplayAnUnclaimedExecution() {
        var journal = new InMemoryWriteCommitJournal();
        journal.recordIntent(pending("claimed"));
        journal.recordIntent(pending("live-peer"));
        var reconciled = new java.util.ArrayList<String>();

        recover(new WriteCommitRecoveryService(journal, entry -> {
            reconciled.add(entry.queryId());
            return WriteCommitReconciler.RecoveryOutcome.COMMITTED;
        }), "claimed");

        assertThat(reconciled).containsExactly("claimed");
        assertThat(journal.getEntry("live-peer")).isPresent();
    }

    @Test
    void losingDurableFenceBeforeTerminalJournalMutationLeavesIntentPending() {
        var journal = new InMemoryWriteCommitJournal();
        journal.recordIntent(pending("q1"));
        var fenceChecks = new AtomicInteger();

        var service = new WriteCommitRecoveryService(journal,
                entry -> WriteCommitReconciler.RecoveryOutcome.COMMITTED);

        assertThatThrownBy(() -> service.recover(
                Map.of("q1", 2L),
                (ignored, token) -> fenceChecks.incrementAndGet() == 1))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Lost execution anchor");
        assertThat(journal.getEntry("q1")).get()
                .extracting(WriteCommitEntry::state)
                .isEqualTo(WriteCommitEntry.State.PENDING);
    }

    private static WriteCommitRecoveryService.RecoveryReport recover(
            WriteCommitRecoveryService service, String... executionIds) {
        Map<String, Long> tokens = java.util.Arrays.stream(executionIds)
                .collect(java.util.stream.Collectors.toUnmodifiableMap(
                        id -> id, ignored -> 2L));
        return service.recover(tokens, (ignored, token) -> token == 2L);
    }
}
