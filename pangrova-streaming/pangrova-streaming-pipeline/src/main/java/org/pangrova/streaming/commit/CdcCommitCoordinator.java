package org.pangrova.streaming.commit;

import org.pangrova.streaming.api.checkpoint.TopicPartitionOffset;

import java.util.List;
import java.util.Map;
import org.pangrova.streaming.api.commit.CdcBatchCheckpoint;
import org.pangrova.streaming.api.commit.SourceId;
import org.pangrova.streaming.api.commit.PrepareToken;

/**
 * Persists complete source-batch checkpoints and orchestrates catalog publication.
 * Catalog publication and the JDBC state transition are separate transactions;
 * COMMITTING requires connector reconciliation before replay or source advancement.
 */
public interface CdcCommitCoordinator {
    /** Prepare an intent identified by its complete, canonical source checkpoint. */
    default PrepareToken prepare(SourceId source, CdcBatchCheckpoint checkpoint) {
        return prepare(source, checkpoint, checkpoint.fingerprint());
    }

    /**
     * Prepare with an externally bound batch identity. Reuse of that identity with
     * a different checkpoint is rejected, including during concurrent preparation.
     */
    PrepareToken prepare(SourceId source, CdcBatchCheckpoint checkpoint, String dedupKey);

    /** Mark committed only after external publication has succeeded. */
    void commit(PrepareToken token);

    /** Roll back only an intent which has not entered publication. */
    void rollback(PrepareToken token);

    /** Claim rollback before discarding prepared participants' unpublished files. */
    void rollback(PrepareToken token, List<CommitParticipant> participants);

    /** Recover abandoned prepares; ambiguous COMMITTING records require reconciliation. */
    List<PrepareToken> recover(SourceId source);

    /** Publish participants and durably expose the checkpoint; source acknowledgement follows. */
    default boolean executeAtomicCommit(SourceId source, CdcBatchCheckpoint checkpoint,
                                       List<CommitParticipant> participants) {
        return executeAtomicCommit(source, checkpoint, checkpoint.fingerprint(), participants);
    }

    /**
     * Returns false for an already committed identical batch, true for new publication.
     * A failure after publication starts leaves COMMITTING, never a rollback claim.
     */
    boolean executeAtomicCommit(SourceId source, CdcBatchCheckpoint checkpoint, String dedupKey,
                                List<CommitParticipant> participants);

    /** Exact topic/partition next offsets from committed checkpoints, never inferred from source IDs. */
    Map<TopicPartitionOffset, Long> findLastCommittedOffsets(SourceId source);
}
