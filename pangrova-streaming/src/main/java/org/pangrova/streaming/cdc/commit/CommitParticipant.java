package org.pangrova.streaming.cdc.commit;

/**
 * Participant in the CDC two-phase commit protocol.
 *
 * <p>Each participant represents one side of the atomic commit: typically
 * the table connector (Iceberg/Paimon data write) and the offset store
 * (CDC source offset persistence). The {@link TwoPhaseCommitCoordinator}
 * orchestrates all participants through the prepare/commit/abort lifecycle.</p>
 *
 * <h2>Contract</h2>
 * <ul>
 *   <li>{@link #prepare()} — perform the participant's work and validate readiness
 *       to commit. Must be idempotent on success (duplicate prepare is a no-op).</li>
 *   <li>{@link #commit()} — finalize the prepared work. Called only after ALL
 *       participants have successfully prepared.</li>
 *   <li>{@link #abort()} — roll back any prepared work. Called when any participant
 *       fails during prepare or commit. Must be safe to call even if prepare was
 *       never called or already committed (best-effort cleanup).</li>
 * </ul>
 *
 * <h2>Failure Semantics</h2>
 * <p>If {@code prepare()} throws, the coordinator aborts all previously prepared
 * participants and rolls back the transaction. If {@code commit()} throws, the
 * coordinator attempts to abort all participants (best-effort) and rolls back.</p>
 */
public interface CommitParticipant {

    /**
     * Returns a human-readable name for this participant (used in logging and error messages).
     */
    String name();

    /**
     * Phase 1: prepare the participant's work for commit.
     *
     * <p>For a table write participant, this performs the actual data write
     * (producing data files). For an offset participant, this validates that
     * offsets are ready to be committed.</p>
     *
     * <p><b>Recovery constraint:</b> any durable side effect produced here MUST be a
     * reclaimable orphan while the transaction is uncommitted (e.g. data files not
     * yet referenced by any snapshot). After a process crash, recovery can only
     * reclaim the commit record and CANNOT call {@link #abort()} on this in-memory
     * participant, so uncommitted side effects must be collectable by storage-layer
     * orphan/expire maintenance.</p>
     *
     * @throws Exception if preparation fails (triggers abort of all participants)
     */
    void prepare() throws Exception;

    /**
     * Phase 2: finalize the prepared work.
     *
     * <p>Called only when all participants have successfully prepared.
     * For an offset participant, this commits the source offsets.</p>
     *
     * @throws Exception if commit fails (triggers best-effort abort of all participants)
     */
    void commit() throws Exception;

    /**
     * Abort any prepared or partially committed work.
     *
     * <p>Must be idempotent and must not throw. Called on failure during
     * prepare or commit phases. Implementations should log but swallow
     * any errors during cleanup.</p>
     */
    void abort();
}
