package org.pangrova.shuffle.api.contract;

import org.pangrova.shuffle.api.commit.ShuffleCommitResult;
import org.pangrova.shuffle.api.registration.ShuffleHandle;
import org.pangrova.shuffle.api.registration.ShuffleRegistration;
/**
 * SPI for managing shuffle lifecycle in the Remote Shuffle Service.
 *
 * <p>Separated from {@link ExchangeChannelManager} per Interface Segregation Principle:
 * {@code ExchangeManager} handles data channel creation (sink/source);
 * {@code ShuffleLifecycleManager} handles shuffle registration, commit, and cleanup.
 *
 * <p>Implementations that support both (e.g., {@code RemoteShuffleExchangeManager}) implement
 * both interfaces. {@code RemoteExchangeManager} does NOT implement this interface
 * since it has no external shuffle lifecycle to manage.
 */
public interface ShuffleLifecycleManager {

    /**
     * Registers a shuffle for a stage before task dispatch.
     * The remote shuffle server allocates partition buffers and returns endpoint information.
     *
     * @param registration the shuffle descriptor
     * @return a handle with shuffle ID and server endpoints
     */
    ShuffleHandle registerShuffle(ShuffleRegistration registration);

    /**
     * Commits all partitions for a stage, making data available for downstream reads.
     * Collects per-partition statistics (row count, byte size) and returns them
     * for adaptive query execution decisions.
     *
     * @param queryId the query identifier
     * @param stageId the stage identifier
     * @return commit result with partition statistics, or {@link ShuffleCommitResult#EMPTY}
     *         if no remote shuffle is registered for this stage
     */
    ShuffleCommitResult commitShuffle(String queryId, int stageId);

    /**
     * Cleans up all shuffle data for a completed or failed query.
     *
     * @param queryId the query identifier
     */
    void cleanupShuffle(String queryId);
}
