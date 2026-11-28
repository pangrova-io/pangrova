package org.pangrova.coordinator.query.task.write;

import org.pangrova.coordinator.enums.WriteRecoveryOutcome;
import org.pangrova.persistence.query.api.commit.WriteCommitEntry;
import org.pangrova.persistence.query.api.commit.WriteCommitJournal;
import org.pangrova.persistence.query.api.enums.WriteCommitState;
import org.pangrova.query.api.write.QuarantinedWriteCommit;
import org.pangrova.persistence.query.api.transaction.WriteCommitQuarantineStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiPredicate;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Consumes durable {@code PENDING} write-commit intents for executions atomically claimed by a
 * recovering coordinator and drives each one to a terminal outcome (A-COMMIT-01).
 *
 * <p>This is the production consumer of
 * {@link WriteCommitJournal#replayPendingEntries(java.util.Set)}. It never performs an unscoped
 * journal scan: the caller must first claim each execution through the durable execution-anchor
 * store. Active executions owned by peer coordinators are therefore invisible to this pass.</p>
 *
 * <p>The pass is idempotent: entries already {@code COMMITTED} are excluded by
 * the scoped replay and additionally re-checked here, so running recovery more
 * than once produces at most one catalog commit per intent.</p>
 *
 * <p>An intent whose outcome cannot be proven is quarantined instead of retried:
 * the decision is persisted by fingerprint, so later passes skip it until an
 * operator reconciles it. Retrying an ambiguous commit could double-commit or
 * erase the evidence needed to decide.</p>
 */
public final class WriteCommitRecoveryService {

    private static final Logger LOG = LoggerFactory.getLogger(WriteCommitRecoveryService.class);

    private static final int MAX_REASON_LENGTH = 1000;
    private static final String QUARANTINE_REASON_AMBIGUOUS =
            "reconciler could not prove the commit outcome";

    private final WriteCommitJournal journal;
    private final WriteCommitReconciler reconciler;
    private final WriteCommitQuarantineStore quarantine;

    public WriteCommitRecoveryService(WriteCommitJournal journal, WriteCommitReconciler reconciler,
            WriteCommitQuarantineStore quarantine) {
        this.journal = checkNotNull(journal, "journal must not be null");
        this.reconciler = checkNotNull(reconciler, "reconciler must not be null");
        this.quarantine = checkNotNull(quarantine, "quarantine must not be null");
    }

    /**
     * Reconciles pending intents for the supplied, durably claimed executions.
     *
     * @return a summary of the recovery pass
     */
    public RecoveryReport recover(
            Map<String, Long> claimedExecutionTokens,
            BiPredicate<String, Long> durableFence) {
        checkNotNull(claimedExecutionTokens,
                "claimedExecutionTokens must not be null");
        checkNotNull(durableFence, "durableFence must not be null");
        List<WriteCommitEntry> pending = journal.replayPendingEntries(
                claimedExecutionTokens.keySet());
        if (pending.isEmpty()) {
            LOG.info("event=write_commit_recovery_noop pending=0");
            return new RecoveryReport(0, 0, 0, 0, Set.of());
        }
        LOG.warn("event=write_commit_recovery_start pending={}", pending.size());
        RecoveryCounters counters = recoverEntries(
                pending, claimedExecutionTokens, durableFence);
        LOG.warn("event=write_commit_recovery_done pending={} committed={} aborted={} quarantined={} deferred={}",
                pending.size(), counters.committed, counters.aborted, counters.quarantined, counters.deferred.size());
        return new RecoveryReport(pending.size(), counters.committed, counters.aborted,
                counters.quarantined, counters.deferred);
    }

    private RecoveryCounters recoverEntries(
            List<WriteCommitEntry> pending,
            Map<String, Long> claimedExecutionTokens,
            BiPredicate<String, Long> durableFence) {
        RecoveryCounters counters = new RecoveryCounters();
        for (WriteCommitEntry entry : pending) {
            Long claimedToken = claimedExecutionTokens.get(entry.queryId());
            requireCurrentFence(entry.queryId(), claimedToken, durableFence);
            if (quarantine.exists(entry.queryId(), entry.commitFingerprint())) {
                counters.quarantined++;
                LOG.warn("event=write_commit_recovery_skipped_quarantined queryId={} table={}",
                        entry.queryId(), entry.tableName());
                continue;
            }
            if (!journal.isCommitted(entry.queryId())) {
                reconcileAndApply(entry, claimedToken, durableFence, counters);
            }
        }
        return counters;
    }

    private void reconcileAndApply(
            WriteCommitEntry entry,
            long claimedToken,
            BiPredicate<String, Long> durableFence,
            RecoveryCounters counters) {
        WriteRecoveryOutcome outcome;
        try {
            outcome = reconciler.reconcile(entry, claimedToken);
        } catch (RuntimeException e) {
            LOG.error("event=write_recovery_alarm severity=CRITICAL "
                    + "message=\"reconciler failed; quarantining intent\" queryId={} table={} error={}",
                    entry.queryId(), entry.tableName(), e.getMessage(), e);
            quarantine(entry, "reconciler failed: " + e, claimedToken, durableFence, counters);
            return;
        }
        switch (outcome) {
            case COMMITTED -> markRecoveredCommitted(entry, claimedToken, durableFence, counters);
            case ABORTED -> markRecoveredAborted(entry, claimedToken, durableFence, counters);
            case DEFERRED -> counters.deferred.add(entry.queryId());
            case QUARANTINED -> quarantine(entry, QUARANTINE_REASON_AMBIGUOUS, claimedToken, durableFence, counters);
            default -> throw new IllegalStateException("Unhandled recovery outcome: " + outcome);
        }
    }

    private void markRecoveredCommitted(
            WriteCommitEntry entry, long token,
            BiPredicate<String, Long> durableFence, RecoveryCounters counters) {
        requireCurrentFence(entry.queryId(), token, durableFence);
        recordTerminalOutcome(entry, token, WriteCommitState.COMMITTED);
        if (!entry.authorizationReceipt().declaration().hasGeneratedColumns()) {
            journal.acknowledgeNoDeferredWork(entry.commitFingerprint());
        }
        counters.committed++;
        LOG.info("event=write_commit_recovered outcome=COMMITTED queryId={} table={}",
                entry.queryId(), entry.tableName());
    }

    private void markRecoveredAborted(
            WriteCommitEntry entry, long token,
            BiPredicate<String, Long> durableFence, RecoveryCounters counters) {
        requireCurrentFence(entry.queryId(), token, durableFence);
        recordTerminalOutcome(entry, token, WriteCommitState.ABORTED);
        counters.aborted++;
        LOG.warn("event=write_commit_recovered outcome=ABORTED queryId={} table={}",
                entry.queryId(), entry.tableName());
    }

    private void recordTerminalOutcome(WriteCommitEntry entry, long token, WriteCommitState outcome) {
        if (!journal.recordTerminalOutcome(entry, token, outcome)) {
            throw new IllegalStateException("Lost execution anchor while recovering write intent: " + entry.queryId());
        }
    }

    private void quarantine(
            WriteCommitEntry entry, String reason, long token,
            BiPredicate<String, Long> durableFence, RecoveryCounters counters) {
        requireCurrentFence(entry.queryId(), token, durableFence);
        try {
            quarantine.record(new QuarantinedWriteCommit(
                    entry.queryId(), entry.commitFingerprint(), entry.catalogName(),
                    entry.tableName(), sanitize(reason), Instant.now()));
        } catch (RuntimeException e) {
            LOG.error("event=write_commit_quarantine_persist_failed severity=CRITICAL "
                    + "message=\"quarantine decision is not durable; the next recovery pass "
                    + "will re-attempt this intent\" queryId={} table={} error={}",
                    entry.queryId(), entry.tableName(), e.getMessage(), e);
            counters.deferred.add(entry.queryId());
            return;
        }
        counters.quarantined++;
        LOG.error("event=write_recovery_alarm severity=CRITICAL "
                + "message=\"write-commit intent quarantined for manual reconciliation\" "
                + "queryId={} catalog={} table={} fragments={} reason={}",
                entry.queryId(), entry.catalogName(), entry.tableName(),
                entry.fragments().size(), reason);
    }

    private static String sanitize(String reason) {
        String value = reason == null || reason.isBlank() ? QUARANTINE_REASON_AMBIGUOUS : reason;
        return value.length() <= MAX_REASON_LENGTH
                ? value : value.substring(0, MAX_REASON_LENGTH);
    }

    private static void requireCurrentFence(
            String executionId,
            Long fencingToken,
            BiPredicate<String, Long> durableFence) {
        if (fencingToken == null || !durableFence.test(executionId, fencingToken)) {
            throw new IllegalStateException(
                    "Lost execution anchor while recovering write intent: " + executionId);
        }
    }

    /** Summary of a recovery pass; quarantined includes intents skipped from earlier passes. */
    public record RecoveryReport(int pending, int committed, int aborted, int quarantined,
            Set<String> deferredExecutionIds) {
        public RecoveryReport {
            deferredExecutionIds = Set.copyOf(deferredExecutionIds);
        }
    }

    private static final class RecoveryCounters {
        private int committed;
        private int aborted;
        private int quarantined;
        private final Set<String> deferred = new HashSet<>();
    }
}
