package org.pangrova.coordinator.query.task.write;

import org.pangrova.coordinator.enums.WriteRecoveryOutcome;
import org.pangrova.persistence.query.api.commit.WriteCommitEntry;

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
     * @param claimedToken the current recovery token
     * @return the decided outcome; must not be null
     */
    WriteRecoveryOutcome reconcile(WriteCommitEntry pendingEntry, long claimedToken);

}
