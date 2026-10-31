package org.pangrova.coordinator.query.task.write;

import org.pangrova.connector.spi.coordination.WriteCommitEntry;

/**
 * Decides the recovery outcome for a durable write-commit intent found {@code PENDING}
 * at coordinator startup / leader promotion (A-COMMIT-01).
 *
 * <p>A {@code PENDING} entry means a (possibly crashed) coordinator recorded a durable
 * intent together with the worker commit fragments, but never durably marked the commit
 * {@code COMMITTED}. The reconciler inspects the target catalog to decide whether the
 * commit already landed, should be retried from the persisted fragments, must be aborted,
 * or must be quarantined for manual intervention.</p>
 */
@FunctionalInterface
public interface WriteCommitReconciler {

    /**
     * Determines the terminal outcome for a single pending intent.
     *
     * @param pendingEntry the durable intent (carries catalog/table/fragments/idempotency key)
     * @return the decided outcome; must not be null
     */
    RecoveryOutcome reconcile(WriteCommitEntry pendingEntry);

    /** The terminal decision for a pending intent during recovery. */
    enum RecoveryOutcome {
        /** The commit was applied (or confirmed already applied); mark it COMMITTED. */
        COMMITTED,
        /** The write is known not to have landed and cannot be safely retried; discard the intent. */
        ABORTED,
        /** The outcome is ambiguous; retain the intent and raise an alarm for manual reconciliation. */
        QUARANTINED
    }
}
