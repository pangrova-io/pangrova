package org.pangrova.shuffle.api;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.shuffle.api.capability.ShuffleCapabilityDescriptor;

/**
 * Registration, publication and cleanup for externally stored exchanges. Publication identifies
 * the exact generation and allocation and returns an immutable receipt. Implementations reject
 * unsupported operations and stale generations. Current registration and routing still use
 * external-service addresses; authorization is separate from the publication receipt.
 */
@Experimental(reason = "Generic provider sessions and verifiable commit receipts are not yet consolidated")
public interface ShuffleLifecycleManager {

    /** Capabilities enabled by this installed lifecycle, before checking per-exchange publication. */
    ShuffleCapabilityDescriptor capabilities();

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
     * @param publication exact registered generation and allocation to publish
     * @return validated publication evidence for every output partition
     * @throws IllegalStateException if the allocation is absent, replaced or publication is unavailable
     */
    ShuffleCommitReceipt commitShuffle(ShufflePublication publication);

    /**
     * Cleans up all shuffle data for a completed or failed query.
     *
     * @param queryId the query identifier
     */
    void cleanupShuffle(String queryId);
}
