package org.pangrova.spi.coordination;

import org.pangrova.spi.annotation.Stable;

import java.util.List;
import java.util.Optional;

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
public interface WriteCommitJournal {

    /**
     * Records a commit intent synchronously (blocks until persisted).
     * Must be called BEFORE {@code finishDistributedWrite()}.
     *
     * @param entry the commit intent with state {@link WriteCommitEntry.State#PENDING}
     * @throws JournalWriteException if persistence fails
     */
    void recordIntent(WriteCommitEntry entry);

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
     *
     * @param queryId the query identifier to check
     * @return true if a COMMITTED entry exists for this queryId
     */
    boolean isCommitted(String queryId);

    /**
     * Retrieves the commit entry for a given query, if one exists.
     *
     * @param queryId the query identifier
     * @return the entry, or empty if no entry exists
     */
    Optional<WriteCommitEntry> getEntry(String queryId);

    /**
     * Replays all PENDING entries for crash recovery.
     * Returns entries that were recorded but not yet marked COMMITTED,
     * indicating writes that may need retry or cleanup.
     *
     * @return list of PENDING entries, ordered by timestamp ascending
     */
    List<WriteCommitEntry> replayPendingEntries();

    /**
     * Purges the journal entry for a completed query (periodic cleanup).
     *
     * @param queryId the query identifier to purge
     */
    void purge(String queryId);
}
