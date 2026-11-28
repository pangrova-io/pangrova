package org.pangrova.shuffle.api;

import java.util.Arrays;

import static org.pangrova.common.validation.Preconditions.checkNotNull;

/**
 * Descriptor for registering a shuffle with the Remote Shuffle Service.
 * Sent by the Coordinator before dispatching a stage's tasks.
 *
 * @param queryId                    the owning query identifier
 * @param stageId                    the producing stage identifier
 * @param numPartitions              number of output partitions (must be positive)
 * @param estimatedBytesPerPartition estimated bytes per partition for capacity planning (non-negative)
 * @param requireReplication         whether shuffle data should be replicated for fault tolerance
 * @param planEpoch                  immutable physical-plan epoch for this stage attempt
 * @param expectedProducerCount      number of logical producer partitions expected at finalization
 * @param expectedProducerSetChecksum canonical SHA-256 of the logical producer identity set
 */
public record ShuffleRegistration(
        String queryId,
        int stageId,
        int numPartitions,
        long estimatedBytesPerPartition,
        boolean requireReplication,
        long planEpoch,
        int expectedProducerCount,
        byte[] expectedProducerSetChecksum
) {

    public ShuffleRegistration {
        checkNotNull(queryId, "queryId must not be null");
        if (queryId.isBlank() || stageId < 0) {
            throw new IllegalArgumentException("shuffle registration requires a nonblank query and non-negative stage");
        }
        if (numPartitions <= 0) {
            throw new IllegalArgumentException("numPartitions must be positive, got: " + numPartitions);
        }
        if (estimatedBytesPerPartition < 0) {
            throw new IllegalArgumentException(
                    "estimatedBytesPerPartition must be non-negative, got: " + estimatedBytesPerPartition);
        }
        if (planEpoch <= 0) {
            throw new IllegalArgumentException("planEpoch must be positive, got: " + planEpoch);
        }
        if (expectedProducerCount <= 0) {
            throw new IllegalArgumentException(
                    "expectedProducerCount must be positive, got: " + expectedProducerCount);
        }
        checkNotNull(expectedProducerSetChecksum,
                "expectedProducerSetChecksum must not be null");
        if (expectedProducerSetChecksum.length != 32) {
            throw new IllegalArgumentException(
                    "expectedProducerSetChecksum must be a SHA-256 digest");
        }
        expectedProducerSetChecksum = Arrays.copyOf(
                expectedProducerSetChecksum, expectedProducerSetChecksum.length);
    }

    @Override
    public byte[] expectedProducerSetChecksum() {
        return Arrays.copyOf(
                expectedProducerSetChecksum, expectedProducerSetChecksum.length);
    }
}
