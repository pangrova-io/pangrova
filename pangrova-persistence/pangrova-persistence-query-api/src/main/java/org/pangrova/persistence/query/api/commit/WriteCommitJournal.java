package org.pangrova.persistence.query.api.commit;

import org.pangrova.connector.api.commit.NativeWriteFinalization;

import org.pangrova.common.api.annotation.Stable;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.pangrova.common.file.CopyFilePublication;

/**
 * Write-ahead journal for distributed write commits.
 *
 * <p>Ensures idempotent commit behavior across coordinator crash/recovery cycles.
 * Before calling {@code ConnectorMetadata.finishDistributedWrite()}, the coordinator
 * records a PENDING intent. After the commit succeeds, it marks the entry COMMITTED.
 * On recovery, already-COMMITTED entries are skipped.</p>
 *
 * <p>Implementations must guarantee that {@link #recordIntent(WriteCommitEntry)} is
 * durable before returning, so that a crash between intent-record and commit-execute
 * leaves a recoverable PENDING entry.</p>
 */
@Stable(since = "1.0")
public interface WriteCommitJournal extends WriteCommitOutbox {

    /**
     * Records a commit intent synchronously (blocks until persisted).
     * Must be called BEFORE {@code finishDistributedWrite()}.
     *
     * @param entry the commit intent with state {@link WriteCommitState#PENDING}
     * @throws JournalWriteException if persistence fails
     */
    void recordIntent(WriteCommitEntry entry);

    /**
     * Durably records a confirmed external manifest before catalog visibility is changed.
     * The fingerprint must match an existing intent; repeated identical publication
     * succeeds and different version/digest evidence is rejected.
     */
    void recordPublication(String queryId, String commitFingerprint, NativeWriteFinalization publication);

    /**
     * Durably records a COPY file's marker-verified Iceberg publication before marking
     * the write COMMITTED or retiring its redo intent. In-memory journals must reject COPY.
     */
    void recordCopyFilePublication(WriteCommitEntry entry, CopyFilePublication publication);

    /** Returns exact durable COPY evidence, including its snapshot ID; conflicting evidence fails closed. */
    Optional<CopyFilePublication> findCopyFilePublication(WriteCommitEntry entry);

    /** Whether an exact durable consumer requires confirmed publication before retiring this intent. */
    boolean requiresPublication(WriteCommitEntry intent);

    /**
     * Marks an existing intent as COMMITTED after successful commit.
     * Must be called AFTER {@code finishDistributedWrite()} succeeds.
     *
     * @param queryId the query identifier of the committed write
     * @throws JournalWriteException if the update fails
     */
    void markCommitted(String queryId);

    /**
     * Checks whether a write for the given query has already been committed.
     * Used during recovery to skip already-committed writes (idempotency guard).
     * The compact terminal receipt remains available after redo retirement.
     *
     * @param queryId the query identifier to check
     * @return true if a COMMITTED terminal receipt exists for this queryId
     */
    boolean isCommitted(String queryId);

    /**
     * Retrieves the commit entry for a given query, if one exists.
     *
     * @param queryId the query identifier
     * @return the entry, or empty if no entry exists
     */
    Optional<WriteCommitEntry> getEntry(String queryId);

    /** Retained terminal identity, independent of whether the replayable redo still exists. */
    Optional<WriteCommitReceipt> getReceipt(String queryId);

    /**
     * Finds a batch awaiting publication or the most recently confirmed checkpoint for
     * one exact CDC source generation. An unresolved batch takes priority over confirmed
     * history so another batch cannot bypass it after leadership transfer. The checkpoint
     * carries source ranges and journal identity. Fenced replay reads the redo separately.
     */
    Optional<CdcWriteCheckpoint> findCdcCheckpoint(String tenantId, String streamId, String sourceEpoch);

    /**
     * Replays PENDING entries only for executions atomically claimed by the
     * recovering process.
     * Returns entries that were recorded but not yet marked COMMITTED,
     * indicating writes that may need retry or cleanup.
     *
     * @return list of PENDING entries, ordered by timestamp ascending
     */
    List<WriteCommitEntry> replayPendingEntries(Set<String> executionIds);

    /**
     * Persists a terminal barrier after fenced recovery has confirmed the exact intent's abort.
     * Query failure or a timeout alone is not evidence that a catalog commit was aborted.
     * Repeating the same ABORTED transition is idempotent. Missing, different, committed,
     * or externally published intents are rejected; the immutable identity is never deleted.
     *
     * @param queryId execution identifier whose durable fence is held by the caller
     * @param commitFingerprint immutable fingerprint of the reconciled intent
     * @throws IllegalStateException if the intent is missing, differs or has publication evidence
     * @throws JournalWriteException if persistence fails
     */
    void markAborted(String queryId, String commitFingerprint);

    /**
     * Purges only an acknowledged committed entry with no outstanding deferred work.
     * Pending intents are retained for reconciliation, including after query failure.
     *
     * @param queryId the query identifier to purge
     */
    void purge(String queryId);
}
