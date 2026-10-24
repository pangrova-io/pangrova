package org.pangrova.streaming.cdc.commit;

/**
 * Prepared target publication participating in a CDC commit attempt.
 * External systems do not share an atomic transaction. A publication failure
 * leaves COMMITTING for reconciliation; it must never trigger blind rollback.
 * Source acknowledgement belongs after the durable COMMITTED transition.
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
     * (producing unpublished data files).</p>
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
     * @throws Exception if publication fails or has an ambiguous outcome
     */
    void commit() throws Exception;

    /**
     * Abort only prepared work that has not entered publication.
     *
     * <p>Must be idempotent. Called on preparation failure or when a local
     * attempt loses the publication claim. Never delete files whose publication
     * outcome is ambiguous.</p>
     */
    void abort();
}
