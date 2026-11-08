package org.pangrova.streaming.api.commit;

import org.pangrova.streaming.api.checkpoint.TopicPartitionOffset;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/** Durable state port for the CDC two-phase commit protocol. */
public interface CdcCommitStateStore {

    boolean insert(PrepareToken token, CdcCommitState state);

    boolean insert(PrepareToken token, CdcCommitState state, String batchKey);

    void updateState(UUID id, CdcCommitState newState);

    boolean compareAndSetState(UUID id, CdcCommitState expected, CdcCommitState next);

    Optional<PrepareToken> findDuplicate(SourceId source, CdcBatchCheckpoint checkpoint);

    Optional<PrepareToken> findDuplicate(SourceId source, CdcBatchCheckpoint checkpoint, String batchKey);

    List<PrepareToken> findInFlight(SourceId source);

    Optional<CdcCommitState> findStateById(UUID id);

    Map<TopicPartitionOffset, Long> findLastCommittedOffsets(SourceId source);
}
