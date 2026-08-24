package org.pangrova.streaming.cdc.commit;

import java.util.List;
import java.util.Map;

import org.pangrova.spi.streaming.TopicPartitionOffset;

/**
 * Orchestrates the two-phase commit protocol for CDC exactly-once semantics.
 *
 * <p>The protocol ensures that offset commits and downstream table refreshes
 * (Iceberg/Paimon manifest commits) are atomic: either both succeed or both
 * are rolled back.</p>
 *
 * <h2>Protocol Flow</h2>
 * <ol>
 *   <li>{@link #prepare} — flush refresh writer to prepared state + persist offset
 *       in prepared table. Returns a {@link PrepareToken}.</li>
 *   <li>{@link #commit} — both sides acknowledged prepared → commit refresh manifest
 *       + commit offset. Idempotent.</li>
 *   <li>{@link #rollback} — any failure → delete prepared files + revert offset.
 *       Idempotent.</li>
 * </ol>
 *
 * <h2>Atomic Commit with Participants</h2>
 * <p>{@link #executeAtomicCommit} orchestrates a list of {@link CommitParticipant}s
 * through the full 2PC protocol: prepare all participants, then commit all (or abort
 * all on any failure). The offset range is persisted in the commit state store for
 * crash-recovery deduplication.</p>
 *
 * <h2>Recovery</h2>
 * <p>On startup, {@link #recover} scans for in-flight transactions:
 * PREPARED past deadline → rollback; COMMITTING → retry commit (idempotent).</p>
 *
 * <h2>Idempotency</h2>
 * <p>The (source_id, partition, offset) triple serves as the deduplication key.
 * Duplicate prepare calls for the same triple are detected and return the
 * existing token without side effects.</p>
 */
public interface CdcCommitCoordinator {

    /**
     * Phase 1: prepare both offset and refresh for commit.
     *
     * <p>If a transaction for the same (source, partition, offset range) already
     * exists in PREPARED or COMMITTED state, this method is idempotent and returns
     * the existing token.</p>
     *
     * @param source the CDC source identifier
     * @param range  the offset range being committed
     * @return a prepare token representing the in-flight transaction
     */
    default PrepareToken prepare(SourceId source, OffsetRange range) {
        return prepare(source, range, defaultDedupKey(range));
    }

    /**
     * Prepares a commit record keyed for deduplication by an explicit {@code dedupKey}
     * (A-EOS-01: a collision-free full-batch fingerprint), rather than a single partition range.
     *
     * @param source   the CDC source identifier
     * @param range    the representative offset range recorded for offset restore
     * @param dedupKey the batch deduplication key (must be stable across crash-recovery replays)
     * @return a prepare token representing the in-flight transaction
     */
    PrepareToken prepare(SourceId source, OffsetRange range, String dedupKey);

    /**
     * A-EOS-01: default dedup key when a caller does not supply a full-batch fingerprint,
     * preserving the pre-fingerprint single-range (partition, start, end) identity.
     */
    static String defaultDedupKey(OffsetRange range) {
        return range.partition() + ":" + range.startOffset() + ":" + range.endOffset();
    }

    /**
     * Phase 2: commit both offset and refresh atomically.
     *
     * <p>Idempotent: calling commit on an already-committed token is a no-op.</p>
     *
     * @param token the prepare token from {@link #prepare}
     */
    void commit(PrepareToken token);

    /**
     * Rollback a prepared or partially committed transaction.
     *
     * <p>Idempotent: calling rollback on an already-rolled-back token is a no-op.</p>
     *
     * @param token the prepare token to rollback
     */
    void rollback(PrepareToken token);

    /**
     * Rollback with explicit participants: aborts each participant's prepared side
     * effects (delete prepared data files, revert offsets) BEFORE marking the commit
     * record rolled back. Use this on every synchronous rollback path where
     * participant handles are still available, so {@link CommitParticipant#abort()}
     * is guaranteed to run (F-STREAM-2PC).
     *
     * <p>{@link #recover} runs after a process restart where participant runtime
     * handles are gone, so it can only reclaim the commit record. Participants MUST
     * therefore ensure prepare-phase side effects are reclaimable orphans
     * (uncommitted data files collected by storage-layer orphan/expire maintenance).</p>
     *
     * @param token        the prepare token to rollback
     * @param participants participants whose prepared work must be aborted (nullable)
     */
    default void rollback(PrepareToken token, List<CommitParticipant> participants) {
        if (participants != null) {
            for (CommitParticipant participant : participants) {
                try {
                    participant.abort();
                } catch (RuntimeException ignored) {
                    // abort() contract: must not throw; each participant logs its own errors.
                }
            }
        }
        rollback(token);
    }

    /**
     * Recover in-flight transactions for the given source on startup.
     *
     * <p>Scans persisted state for PREPARED (past deadline → rollback) and
     * COMMITTING (→ retry commit) transactions.</p>
     *
     * @param source the CDC source to recover
     * @return list of tokens that were recovered (committed or rolled back)
     */
    List<PrepareToken> recover(SourceId source);

    /**
     * Executes a full two-phase commit with the given participants, using the
     * (source, range) tuple as the deduplication key for crash recovery.
     *
     * <p>Protocol:</p>
     * <ol>
     *   <li>Prepare a commit record in the state store for deduplication.</li>
     *   <li>If the same (source, range) was already committed (crash-recovery replay),
     *       skip all participants and return immediately (dedup).</li>
     *   <li>Call {@link CommitParticipant#prepare()} on each participant in order.</li>
     *   <li>If all prepare calls succeed, call {@link CommitParticipant#commit()} on each,
     *       then mark the commit record as COMMITTED.</li>
     *   <li>If any prepare or commit call fails, call {@link CommitParticipant#abort()} on
     *       all previously prepared participants and roll back the commit record.</li>
     * </ol>
     *
     * @param source       the CDC source identifier
     * @param range        the offset range being committed (dedup key)
     * @param participants ordered list of commit participants; must not be empty
     * @return {@code true} if the commit executed (new), {@code false} if deduplicated
     * @throws IllegalArgumentException if participants is null or empty
     * @throws RuntimeException         if any participant fails (after abort + rollback)
     */
    default boolean executeAtomicCommit(SourceId source, OffsetRange range,
                                        List<CommitParticipant> participants) {
        return executeAtomicCommit(source, range, defaultDedupKey(range), participants);
    }

    /**
     * Executes a full two-phase commit using an explicit {@code dedupKey} (A-EOS-01: a
     * collision-free full-batch fingerprint) as the deduplication key, rather than a single
     * partition range. The {@code range} is still recorded as the representative offset range for
     * JDBC-side offset restore.
     *
     * @param source       the CDC source identifier
     * @param range        the representative offset range recorded for offset restore
     * @param dedupKey     the batch deduplication key (stable across crash-recovery replays)
     * @param participants ordered list of commit participants; must not be empty
     * @return {@code true} if the commit executed (new), {@code false} if deduplicated
     */
    boolean executeAtomicCommit(SourceId source, OffsetRange range, String dedupKey,
                                List<CommitParticipant> participants);

    /**
     * Returns the last committed offset for each partition of the given source.
     *
     * <p>Used during pipeline startup to seek the source reader to the correct position
     * based on the JDBC-persisted state, which is the authoritative source of truth
     * for exactly-once semantics (supersedes Kafka consumer group offsets).</p>
     *
     * <p>The returned map keys are {@code TopicPartitionOffset} with the topic derived
     * from the source id. Values are {@code endOffset + 1} (the next offset to consume).</p>
     *
     * @param source the CDC source identifier
     * @return map of partition to next-offset-to-consume; empty if no committed state
     */
    Map<TopicPartitionOffset, Long> findLastCommittedOffsets(SourceId source);
}
