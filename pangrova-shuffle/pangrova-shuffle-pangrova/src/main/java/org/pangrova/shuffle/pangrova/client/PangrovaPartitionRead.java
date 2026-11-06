package org.pangrova.shuffle.pangrova.client;

import java.util.Arrays;
import java.util.Objects;

import org.pangrova.shuffle.api.identity.ExchangeChannelContext;

/**
 * Pangrova service read parameters for an already selected partition channel.
 * This in-process request carries expected publication evidence, not credentials or a Native handle.
 * Endpoint selection and access acquisition belong to the caller; the reader verifies the evidence
 * against the authenticated service terminal. This is not a generic Shuffle receipt or wire descriptor.
 */
public record PangrovaPartitionRead(
        String shuffleId,
        ExchangeChannelContext context,
        long planEpoch,
        int expectedProducerPartitions,
        byte[] expectedProducerSetChecksum) {

    public PangrovaPartitionRead {
        Objects.requireNonNull(context, "context must not be null");
        if (shuffleId == null || shuffleId.isBlank()) {
            throw new IllegalArgumentException("shuffleId must not be blank");
        }
        if (planEpoch <= 0 || expectedProducerPartitions <= 0) {
            throw new IllegalArgumentException("generation and producer count must be positive");
        }
        if (expectedProducerSetChecksum == null || expectedProducerSetChecksum.length != 32) {
            throw new IllegalArgumentException("producer set checksum must be SHA-256");
        }
        expectedProducerSetChecksum = expectedProducerSetChecksum.clone();
    }

    @Override
    public byte[] expectedProducerSetChecksum() {
        return expectedProducerSetChecksum.clone();
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof PangrovaPartitionRead that
                && shuffleId.equals(that.shuffleId) && context.equals(that.context)
                && planEpoch == that.planEpoch
                && expectedProducerPartitions == that.expectedProducerPartitions
                && Arrays.equals(expectedProducerSetChecksum, that.expectedProducerSetChecksum);
    }

    @Override
    public int hashCode() {
        return 31 * Objects.hash(shuffleId, context, planEpoch, expectedProducerPartitions)
                + Arrays.hashCode(expectedProducerSetChecksum);
    }
}
