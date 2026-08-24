package org.pangrova.coordinator.query.task;

import org.pangrova.spi.coordination.WriteCommitEntry;
import org.pangrova.spi.coordination.WriteCommitJournal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Consumes durable {@code PENDING} write-commit intents on coordinator startup / leader
 * promotion and drives each one to a terminal outcome (A-COMMIT-01).
 *
 * <p>This is the production consumer of {@link WriteCommitJournal#replayPendingEntries()}:
 * before a newly-started (or newly-promoted) coordinator begins accepting conflicting writes,
 * it reconciles every intent a crashed predecessor left {@code PENDING}, so a distributed
 * write that had reached the durable-intent stage is never silently lost nor blindly retried.</p>
 *
 * <p>The pass is idempotent: entries already {@code COMMITTED} are excluded by
 * {@code replayPendingEntries()} and additionally re-checked here, so running recovery more
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
     * Reconciles all pending intents recorded in the journal.
     *
     * @return a summary of the recovery pass
     */
    public RecoveryReport recover() {
        List<WriteCommitEntry> pending = journal.replayPendingEntries();
        if (pending.isEmpty()) {
            LOG.info("event=write_commit_recovery_noop pending=0");
            return new RecoveryReport(0, 0, 0, 0);
        }
        LOG.warn("event=write_commit_recovery_start pending={}", pending.size());
        int committed = 0;
        int aborted = 0;
        int quarantined = 0;
        for (WriteCommitEntry entry : pending) {
            // Idempotency guard: a concurrent or previous pass may have already finalized it.
            if (journal.isCommitted(entry.queryId())) {
                continue;
            }
            WriteCommitReconciler.RecoveryOutcome outcome;
            try {
                outcome = reconciler.reconcile(entry);
            } catch (RuntimeException e) {
                LOG.error("event=write_recovery_alarm severity=CRITICAL "
                        + "message=\"reconciler failed; quarantining intent\" queryId={} table={} error={}",
                        entry.queryId(), entry.tableName(), e.getMessage(), e);
                outcome = WriteCommitReconciler.RecoveryOutcome.QUARANTINED;
            }
            switch (outcome) {
                case COMMITTED -> {
                    journal.markCommitted(entry.queryId());
                    committed++;
                    LOG.info("event=write_commit_recovered outcome=COMMITTED queryId={} table={}",
                            entry.queryId(), entry.tableName());
                }
                case ABORTED -> {
                    journal.purge(entry.queryId());
                    aborted++;
                    LOG.warn("event=write_commit_recovered outcome=ABORTED queryId={} table={}",
                            entry.queryId(), entry.tableName());
                }
                case QUARANTINED -> {
                    quarantined++;
                    LOG.error("event=write_recovery_alarm severity=CRITICAL "
                            + "message=\"write-commit intent quarantined for manual reconciliation\" "
                            + "queryId={} catalog={} table={} fragments={}",
                            entry.queryId(), entry.catalogName(), entry.tableName(),
                            entry.fragments().size());
                }
                default -> throw new IllegalStateException("Unhandled recovery outcome: " + outcome);
            }
        }
        LOG.warn("event=write_commit_recovery_done pending={} committed={} aborted={} quarantined={}",
                pending.size(), committed, aborted, quarantined);
        return new RecoveryReport(pending.size(), committed, aborted, quarantined);
    }

    /** Summary of a recovery pass. */
    public record RecoveryReport(int pending, int committed, int aborted, int quarantined) {
    }
}
