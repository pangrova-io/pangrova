package org.pangrova.spi.streaming.commit;

import org.pangrova.spi.annotation.Experimental;
import org.pangrova.spi.streaming.source.SourceId;
import org.pangrova.spi.streaming.source.TopicPartitionOffset;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Backend-neutral persistence contract for CDC two-phase commit state.
 *
 * <p>JDBC/PostgreSQL is one implementation; other backends can provide their
 * own without depending on the streaming runtime. Implementations must preserve
 * these semantics:</p>
 * <ul>
 *   <li>{@link #insert} is idempotent per {@code (source, batchKey)} and returns
 *       {@code false} when the row already exists.</li>
 *   <li>{@link #compareAndSetState} is the cross-process publication claim.</li>
 *   <li>{@link #findLastCommittedOffsets} returns only COMMITTED checkpoints and
 *       exact next offsets per topic/partition.</li>
 *   <li>{@link #pruneTerminalBefore} removes only terminal rows older than the
 *       cutoff.</li>
 * </ul>
 */
@Experimental
public interface CdcCommitStateStore {

    boolean insert(PrepareToken token, CdcCommitState state, String batchKey);

    Optional<PrepareToken> findDuplicate(
            SourceId source, CdcBatchCheckpoint checkpoint, String batchKey);

    Optional<CdcCommitState> findStateById(UUID id);

    boolean compareAndSetState(UUID id, CdcCommitState expected, CdcCommitState next);

    void updateState(UUID id, CdcCommitState newState);

    List<PrepareToken> findInFlight(SourceId source);

    Map<TopicPartitionOffset, Long> findLastCommittedOffsets(SourceId source);

    /** Removes COMMITTED/ROLLED_BACK rows created before the cutoff. */
    int pruneTerminalBefore(Instant cutoff);
}
