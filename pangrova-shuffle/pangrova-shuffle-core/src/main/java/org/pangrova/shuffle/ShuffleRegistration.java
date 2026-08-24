package org.pangrova.shuffle;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Descriptor for registering a shuffle with the Remote Shuffle Service.
 * Sent by the Coordinator before dispatching a stage's tasks.
 *
 * @param queryId                    the owning query identifier
 * @param stageId                    the producing stage identifier
 * @param numPartitions              number of output partitions (must be positive)
 * @param estimatedBytesPerPartition estimated bytes per partition for capacity planning (non-negative)
 * @param requireReplication         whether shuffle data should be replicated for fault tolerance
 */
public record ShuffleRegistration(
        String queryId,
        int stageId,
        int numPartitions,
        long estimatedBytesPerPartition,
        boolean requireReplication
) {

    public ShuffleRegistration {
        checkNotNull(queryId, "queryId must not be null");
        if (numPartitions <= 0) {
            throw new IllegalArgumentException("numPartitions must be positive, got: " + numPartitions);
        }
        if (estimatedBytesPerPartition < 0) {
            throw new IllegalArgumentException(
                    "estimatedBytesPerPartition must be non-negative, got: " + estimatedBytesPerPartition);
        }
    }
}
