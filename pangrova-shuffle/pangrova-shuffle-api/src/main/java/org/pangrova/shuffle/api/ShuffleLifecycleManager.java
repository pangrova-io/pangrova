package org.pangrova.shuffle.api;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Existing external-service registration, finalization and query cleanup contract.
 * This service-specific contract is not the final provider-neutral session API. Implementations
 * must reject absent or unsupported registration/publication; a local file finish or empty
 * statistics cannot prove stage visibility. The service owns its publication state and cleanup.
 */
@Experimental(reason = "Generic provider sessions and verifiable commit receipts are not yet consolidated")
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
     * @return verified committed partition statistics for the registered stage
     * @throws IllegalStateException if the stage is not registered or publication is unavailable
     */
    ShuffleCommitResult commitShuffle(String queryId, int stageId);

    /**
     * Cleans up all shuffle data for a completed or failed query.
     *
     * @param queryId the query identifier
     */
    void cleanupShuffle(String queryId);
}
