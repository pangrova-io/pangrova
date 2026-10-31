package org.pangrova.query.api.transaction;

import org.pangrova.common.api.annotation.Internal;

import java.util.Optional;

/**
 * Durable record of write intents that recovery must not retry automatically.
 *
 * <p>Absence of a record means the intent is still eligible for reconciliation. A
 * record only matches the exact {@code queryId + commitFingerprint} pair, so a
 * different intent that happens to reuse a query id is never skipped by an older
 * quarantine decision.</p>
 */
@Internal
public interface WriteCommitQuarantineStore {

    /** Whether this exact intent has already been quarantined. */
    boolean exists(String queryId, String commitFingerprint);

    /** Records the quarantine decision; repeating the same decision is idempotent. */
    void record(QuarantinedWriteCommit entry);

    /** Returns the quarantine decision for a query id, if any. */
    Optional<QuarantinedWriteCommit> find(String queryId);
}
