package org.pangrova.coordinator.query.task.write;

import org.pangrova.coordinator.enums.WriteRecoveryOutcome;
import org.pangrova.persistence.query.api.enums.WriteCommitState;
import org.pangrova.persistence.query.memory.transaction.InMemoryWriteCommitJournal;
import org.pangrova.persistence.query.memory.transaction.InMemoryWriteCommitQuarantineStore;
import org.pangrova.query.api.write.QuarantinedWriteCommit;
import org.pangrova.persistence.query.api.transaction.WriteCommitQuarantineStore;
import org.pangrova.persistence.query.api.commit.WriteCommitEntry;
import org.pangrova.persistence.query.api.commit.WriteCommitFingerprint;
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
                new org.pangrova.persistence.query.api.commit.WriteCommitFingerprint.Intent(
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
                WriteCommitState.PENDING,
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
                null, null, null);
    }

    @Test
    void committedOutcomeMarksEntryCommitted() {
        var journal = new InMemoryWriteCommitJournal();
        journal.recordIntent(pending("q1"));

        var report = recover(service(journal,
                e -> WriteRecoveryOutcome.COMMITTED), "q1");

        assertThat(report.pending()).isEqualTo(1);
        assertThat(report.committed()).isEqualTo(1);
        assertThat(journal.isCommitted("q1")).isTrue();
    }

    @Test
    void retiredRedoRetainsExactTerminalIdentityInMemory() {
        var journal = new InMemoryWriteCommitJournal();
        var intent = pending("retired");
        journal.recordIntent(intent);
        journal.markCommitted(intent.queryId());
        var receipt = journal.getReceipt(intent.queryId()).orElseThrow();
        journal.acknowledgeNoDeferredWork(intent.commitFingerprint());
        journal.purge(intent.queryId());
        assertThat(journal.getEntry(intent.queryId())).isEmpty();
        assertThat(journal.getReceipt(intent.queryId())).contains(receipt);
        assertThat(journal.isCommitted(intent.queryId())).isTrue();
        assertThatThrownBy(() -> journal.recordIntent(intent)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void abortedOutcomeRetainsTerminalBarrier() {
        var journal = new InMemoryWriteCommitJournal();
        journal.recordIntent(pending("q1"));

        var report = recover(service(journal,
                e -> WriteRecoveryOutcome.ABORTED), "q1");

        assertThat(report.aborted()).isEqualTo(1);
        assertThat(journal.getEntry("q1")).get()
                .extracting(WriteCommitEntry::state).isEqualTo(WriteCommitState.ABORTED);
        assertThatThrownBy(() -> journal.recordIntent(pending("q1")))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> journal.markCommitted("q1")).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void deferredWriteRemainsRecoverableAndOtherIntentCompletes() {
        var journal = new InMemoryWriteCommitJournal();
        var deferred = pending("waiting-for-worker");
        journal.recordIntent(deferred);
        journal.recordIntent(pending("ready"));
        var quarantine = new InMemoryWriteCommitQuarantineStore();
        var service = new WriteCommitRecoveryService(journal, entry -> entry.queryId().equals(deferred.queryId())
                ? WriteRecoveryOutcome.DEFERRED : WriteRecoveryOutcome.COMMITTED, quarantine);

        var report = service.recover(Map.of(deferred.queryId(), 2L, "ready", 2L), (id, token) -> token == 2L);

        assertThat(report.deferredExecutionIds()).containsExactly(deferred.queryId());
        assertThat(report.quarantined()).isZero();
        assertThat(journal.getEntry(deferred.queryId())).contains(deferred);
        assertThat(quarantine.find(deferred.queryId())).isEmpty();
        assertThat(journal.isCommitted("ready")).isTrue();

        var resumed = new WriteCommitRecoveryService(journal, entry -> {
            assertThat(entry.commitFingerprint()).isEqualTo(deferred.commitFingerprint());
            return WriteRecoveryOutcome.COMMITTED;
        }, quarantine);
        assertThat(resumed.recover(Map.of(deferred.queryId(), 3L), (id, token) -> token == 3L).committed())
                .isEqualTo(1);
    }

    @Test
    void quarantinedOutcomeRetainsEntryForManualReconciliation() {
        var journal = new InMemoryWriteCommitJournal();
        journal.recordIntent(pending("q1"));
        var quarantine = new InMemoryWriteCommitQuarantineStore();

        var report = recover(new WriteCommitRecoveryService(journal,
                e -> WriteRecoveryOutcome.QUARANTINED, quarantine), "q1");

        assertThat(report.quarantined()).isEqualTo(1);
        // Intent retained (still PENDING) for forensics / manual action.
        assertThat(journal.getEntry("q1")).isPresent();
        assertThat(journal.getEntry("q1").get().state())
                .isEqualTo(WriteCommitState.PENDING);
        assertThat(quarantine.find("q1")).get()
                .satisfies(entry -> {
                    assertThat(entry.commitFingerprint())
                            .isEqualTo(journal.getEntry("q1").orElseThrow().commitFingerprint());
                    assertThat(entry.catalogName()).isEqualTo("iceberg");
                    assertThat(entry.tableName()).isEqualTo("analytics.events");
                    assertThat(entry.reason()).isNotBlank();
                    assertThat(entry.quarantinedAt()).isNotNull();
                });
    }

    @Test
    void quarantinedIntentIsNotReconciledAgain() {
        var journal = new InMemoryWriteCommitJournal();
        journal.recordIntent(pending("q1"));
        var quarantine = new InMemoryWriteCommitQuarantineStore();
        var first = new AtomicInteger();

        recover(new WriteCommitRecoveryService(journal, entry -> {
            first.incrementAndGet();
            return WriteRecoveryOutcome.QUARANTINED;
        }, quarantine), "q1");
        assertThat(first.get()).isEqualTo(1);

        var retried = new AtomicInteger();
        var second = recover(new WriteCommitRecoveryService(journal, entry -> {
            retried.incrementAndGet();
            return WriteRecoveryOutcome.COMMITTED;
        }, quarantine), "q1");

        // Manual reconciliation is required: the second pass must not touch the intent.
        assertThat(retried.get()).isZero();
        assertThat(second.quarantined()).isEqualTo(1);
        assertThat(second.committed()).isZero();
        assertThat(journal.getEntry("q1")).get()
                .extracting(WriteCommitEntry::state)
                .isEqualTo(WriteCommitState.PENDING);
    }

    @Test
    void differentFingerprintIsNotSkippedByAnOlderQuarantine() {
        var journal = new InMemoryWriteCommitJournal();
        journal.recordIntent(pending("q1"));
        var quarantine = new InMemoryWriteCommitQuarantineStore();
        quarantine.record(new QuarantinedWriteCommit("q1", "b".repeat(64), "iceberg",
                "analytics.events", "older decision", java.time.Instant.now()));
        var reconciled = new AtomicInteger();

        var report = recover(new WriteCommitRecoveryService(journal, entry -> {
            reconciled.incrementAndGet();
            return WriteRecoveryOutcome.COMMITTED;
        }, quarantine), "q1");

        assertThat(reconciled.get()).isEqualTo(1);
        assertThat(report.committed()).isEqualTo(1);
    }

    @Test
    void quarantinePersistenceFailureDoesNotAbortTheRecoveryPass() {
        var journal = new InMemoryWriteCommitJournal();
        journal.recordIntent(pending("q1"));
        var quarantine = new WriteCommitQuarantineStore() {
            @Override
            public boolean exists(String queryId, String commitFingerprint) {
                return false;
            }

            @Override
            public void record(QuarantinedWriteCommit entry) {
                throw new IllegalStateException("quarantine store unavailable");
            }

            @Override
            public java.util.Optional<QuarantinedWriteCommit> find(String queryId) {
                return java.util.Optional.empty();
            }
        };

        var report = recover(new WriteCommitRecoveryService(journal,
                e -> WriteRecoveryOutcome.QUARANTINED, quarantine), "q1");

        assertThat(report.quarantined()).isZero();
        assertThat(report.deferredExecutionIds()).containsExactly("q1");
        assertThat(journal.getEntry("q1")).isPresent();
    }

    @Test
    void reconcilerExceptionIsQuarantinedNotPropagated() {
        var journal = new InMemoryWriteCommitJournal();
        journal.recordIntent(pending("q1"));

        var report = recover(service(journal, e -> {
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

        var report = recover(service(journal, e -> {
            reconcileCount.incrementAndGet();
            return WriteRecoveryOutcome.COMMITTED;
        }), "q1");

        // COMMITTED entries are excluded from replayPendingEntries(), so the reconciler is
        // never invoked — re-running recovery cannot double-commit.
        assertThat(reconcileCount.get()).isZero();
        assertThat(report.pending()).isZero();
    }

    @Test
    void mixedOutcomesAreTalliedAndEmptyJournalIsNoOp() {
        var empty = new InMemoryWriteCommitJournal();
        assertThat(recover(service(empty,
                e -> WriteRecoveryOutcome.COMMITTED)).pending()).isZero();

        var journal = new InMemoryWriteCommitJournal();
        journal.recordIntent(pending("commit-me"));
        journal.recordIntent(pending("abort-me"));
        journal.recordIntent(pending("hold-me"));

        var report = recover(service(journal, e -> switch (e.queryId()) {
            case "commit-me" -> WriteRecoveryOutcome.COMMITTED;
            case "abort-me" -> WriteRecoveryOutcome.ABORTED;
            default -> WriteRecoveryOutcome.QUARANTINED;
        }), "commit-me", "abort-me", "hold-me");

        assertThat(report.pending()).isEqualTo(3);
        assertThat(report.committed()).isEqualTo(1);
        assertThat(report.aborted()).isEqualTo(1);
        assertThat(report.quarantined()).isEqualTo(1);
        assertThat(journal.isCommitted("commit-me")).isTrue();
        assertThat(journal.getEntry("abort-me")).get()
                .extracting(WriteCommitEntry::state).isEqualTo(WriteCommitState.ABORTED);
        assertThat(journal.getEntry("hold-me")).isPresent();
    }

    @Test
    void doesNotReplayAnUnclaimedExecution() {
        var journal = new InMemoryWriteCommitJournal();
        journal.recordIntent(pending("claimed"));
        journal.recordIntent(pending("live-peer"));
        var reconciled = new java.util.ArrayList<String>();

        recover(service(journal, entry -> {
            reconciled.add(entry.queryId());
            return WriteRecoveryOutcome.COMMITTED;
        }), "claimed");

        assertThat(reconciled).containsExactly("claimed");
        assertThat(journal.getEntry("live-peer")).isPresent();
    }

    @Test
    void losingDurableFenceBeforeTerminalJournalMutationLeavesIntentPending() {
        var journal = new InMemoryWriteCommitJournal();
        journal.recordIntent(pending("q1"));
        var fenceChecks = new AtomicInteger();

        var service = service(journal,
                entry -> WriteRecoveryOutcome.COMMITTED);

        assertThatThrownBy(() -> service.recover(
                Map.of("q1", 2L),
                (ignored, token) -> fenceChecks.incrementAndGet() == 1))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Lost execution anchor");
        assertThat(journal.getEntry("q1")).get()
                .extracting(WriteCommitEntry::state)
                .isEqualTo(WriteCommitState.PENDING);
    }

    @Test
    void losingDurableFenceBeforeQuarantineLeavesIntentPending() {
        for (boolean throwsDuringReconciliation : List.of(false, true)) {
            var journal = new InMemoryWriteCommitJournal();
            journal.recordIntent(pending("q1"));
            var quarantine = new InMemoryWriteCommitQuarantineStore();
            var fenceChecks = new AtomicInteger();
            var service = new WriteCommitRecoveryService(journal, entry -> {
                if (throwsDuringReconciliation) {
                    throw new IllegalStateException("catalog unavailable");
                }
                return WriteRecoveryOutcome.QUARANTINED;
            }, quarantine);

            assertThatThrownBy(() -> service.recover(Map.of("q1", 2L),
                    (ignored, token) -> fenceChecks.incrementAndGet() == 1))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Lost execution anchor");
            assertThat(quarantine.find("q1")).isEmpty();
            assertThat(journal.getEntry("q1")).get()
                    .extracting(WriteCommitEntry::state).isEqualTo(WriteCommitState.PENDING);
        }
    }

    private static WriteCommitRecoveryService service(
            org.pangrova.persistence.query.api.commit.WriteCommitJournal journal,
            WriteCommitReconciler reconciler) {
        return new WriteCommitRecoveryService(journal, reconciler,
                new InMemoryWriteCommitQuarantineStore());
    }

    private static WriteCommitRecoveryService.RecoveryReport recover(
            WriteCommitRecoveryService service, String... executionIds) {
        Map<String, Long> tokens = java.util.Arrays.stream(executionIds)
                .collect(java.util.stream.Collectors.toUnmodifiableMap(
                        id -> id, ignored -> 2L));
        return service.recover(tokens, (ignored, token) -> token == 2L);
    }
}
