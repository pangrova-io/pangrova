package org.pangrova.coordinator.query.task;

import org.pangrova.spi.coordination.WriteCommitEntry;
import org.pangrova.spi.coordination.WriteCommitJournal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
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
 */
public final class WriteCommitRecoveryService {

    private static final Logger LOG = LoggerFactory.getLogger(WriteCommitRecoveryService.class);

    private final WriteCommitJournal journal;
    private final WriteCommitReconciler reconciler;

    public WriteCommitRecoveryService(WriteCommitJournal journal, WriteCommitReconciler reconciler) {
        this.journal = checkNotNull(journal, "journal must not be null");
        this.reconciler = checkNotNull(reconciler, "reconciler must not be null");
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
            return new RecoveryReport(0, 0, 0, 0);
        }
        LOG.warn("event=write_commit_recovery_start pending={}", pending.size());
        RecoveryCounters counters = recoverEntries(
                pending, claimedExecutionTokens, durableFence);
        LOG.warn("event=write_commit_recovery_done pending={} committed={} aborted={} quarantined={}",
                pending.size(), counters.committed, counters.aborted, counters.quarantined);
        return new RecoveryReport(
                pending.size(), counters.committed, counters.aborted, counters.quarantined);
    }

    private RecoveryCounters recoverEntries(
            List<WriteCommitEntry> pending,
            Map<String, Long> claimedExecutionTokens,
            BiPredicate<String, Long> durableFence) {
        RecoveryCounters counters = new RecoveryCounters();
        for (WriteCommitEntry entry : pending) {
            Long claimedToken = claimedExecutionTokens.get(entry.queryId());
            requireCurrentFence(entry.queryId(), claimedToken, durableFence);
            if (!journal.isCommitted(entry.queryId())) {
                applyOutcome(entry, claimedToken, durableFence, reconcile(entry), counters);
            }
        }
        return counters;
    }

    private WriteCommitReconciler.RecoveryOutcome reconcile(WriteCommitEntry entry) {
        try {
            return reconciler.reconcile(entry);
        } catch (RuntimeException e) {
            LOG.error("event=write_recovery_alarm severity=CRITICAL "
                    + "message=\"reconciler failed; quarantining intent\" queryId={} table={} error={}",
                    entry.queryId(), entry.tableName(), e.getMessage(), e);
            return WriteCommitReconciler.RecoveryOutcome.QUARANTINED;
        }
    }

    private void applyOutcome(
            WriteCommitEntry entry,
            long claimedToken,
            BiPredicate<String, Long> durableFence,
            WriteCommitReconciler.RecoveryOutcome outcome,
            RecoveryCounters counters) {
        switch (outcome) {
            case COMMITTED -> markRecoveredCommitted(entry, claimedToken, durableFence, counters);
            case ABORTED -> markRecoveredAborted(entry, claimedToken, durableFence, counters);
            case QUARANTINED -> quarantine(entry, counters);
            default -> throw new IllegalStateException("Unhandled recovery outcome: " + outcome);
        }
    }

    private void markRecoveredCommitted(
            WriteCommitEntry entry, long token,
            BiPredicate<String, Long> durableFence, RecoveryCounters counters) {
        requireCurrentFence(entry.queryId(), token, durableFence);
        journal.markCommitted(entry.queryId());
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
        journal.markAborted(entry.queryId(), entry.commitFingerprint());
        counters.aborted++;
        LOG.warn("event=write_commit_recovered outcome=ABORTED queryId={} table={}",
                entry.queryId(), entry.tableName());
    }

    private static void quarantine(WriteCommitEntry entry, RecoveryCounters counters) {
        counters.quarantined++;
        LOG.error("event=write_recovery_alarm severity=CRITICAL "
                + "message=\"write-commit intent quarantined for manual reconciliation\" "
                + "queryId={} catalog={} table={} fragments={}",
                entry.queryId(), entry.catalogName(), entry.tableName(), entry.fragments().size());
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

    /** Summary of a recovery pass. */
    public record RecoveryReport(int pending, int committed, int aborted, int quarantined) {
    }

    private static final class RecoveryCounters {
        private int committed;
        private int aborted;
        private int quarantined;
    }
}
