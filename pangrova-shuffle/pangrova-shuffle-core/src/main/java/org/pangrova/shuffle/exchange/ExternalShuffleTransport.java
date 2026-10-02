package org.pangrova.shuffle.exchange;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.pangrova.shuffle.ShuffleAccess;
import org.pangrova.shuffle.ShuffleExecutionOwner;
import org.pangrova.shuffle.ShuffleServiceAddress;
import org.pangrova.shuffle.ShuffleServerEndpoint;

/** External shuffle service binding consumed directly by a Worker. */
public record ExternalShuffleTransport(
        String shuffleId,
        List<ShuffleServerEndpoint> endpoints,
        ShuffleServiceAddress commitAddress,
        int outputPartitionId,
        int partitionCount,
        long planEpoch,
        ShuffleExecutionOwner executionOwner,
        ExchangeTaskAttempt consumerAttempt,
        long deadlineEpochMs,
        ShuffleAccess readAccess,
        int expectedProducerPartitions,
        byte[] expectedProducerSetChecksum) implements ExchangeInputTransport {

    public ExternalShuffleTransport {
        validateReferences(shuffleId, endpoints, commitAddress,
                executionOwner, consumerAttempt, readAccess);
        validateIdentity(outputPartitionId, partitionCount, planEpoch, deadlineEpochMs,
                expectedProducerPartitions, expectedProducerSetChecksum);
        endpoints = endpoints.stream()
                .sorted(Comparator.comparingInt(ShuffleServerEndpoint::partitionStart))
                .toList();
        validateCoverage(endpoints, partitionCount);
        validateAccessExpiry(readAccess);
        expectedProducerSetChecksum = Arrays.copyOf(
                expectedProducerSetChecksum, expectedProducerSetChecksum.length);
    }

    private static void validateReferences(
            String shuffleId,
            List<ShuffleServerEndpoint> endpoints,
            ShuffleServiceAddress commitAddress,
            ShuffleExecutionOwner owner,
            ExchangeTaskAttempt attempt,
            ShuffleAccess access) {
        if (shuffleId == null || shuffleId.isBlank() || endpoints == null) {
            throw new IllegalArgumentException("external shuffle identity is required");
        }
        if (commitAddress == null || owner == null || attempt == null || access == null) {
            throw new IllegalArgumentException(
                    "external shuffle requires execution owner and read access");
        }
    }

    private static void validateIdentity(
            int outputPartitionId,
            int partitionCount,
            long planEpoch,
            long deadlineEpochMs,
            int expectedProducers,
            byte[] producerChecksum) {
        if (planEpoch <= 0 || deadlineEpochMs <= 0
                || outputPartitionId < 0 || outputPartitionId >= partitionCount) {
            throw new IllegalArgumentException(
                    "external shuffle partition and deadline are invalid");
        }
        if (expectedProducers <= 0 || producerChecksum == null
                || producerChecksum.length != 32) {
            throw new IllegalArgumentException("external shuffle producer identity is invalid");
        }
    }

    private static void validateAccessExpiry(ShuffleAccess access) {
        if (access.expiresAtEpochMs() <= System.currentTimeMillis()) {
            throw new IllegalArgumentException(
                    "shuffle read access is expired");
        }
    }

    /** Returns the endpoint whose declared range owns the consumed partition. */
    public ShuffleServerEndpoint selectedEndpoint() {
        int low = 0;
        int high = endpoints.size() - 1;
        while (low <= high) {
            int middle = (low + high) >>> 1;
            ShuffleServerEndpoint endpoint = endpoints.get(middle);
            if (outputPartitionId < endpoint.partitionStart()) {
                high = middle - 1;
            } else if (outputPartitionId >= endpoint.partitionEnd()) {
                low = middle + 1;
            } else {
                return endpoint;
            }
        }
        throw new IllegalStateException("validated endpoint coverage changed");
    }

    private static void validateCoverage(
            List<ShuffleServerEndpoint> endpoints, int partitionCount) {
        int cursor = 0;
        for (ShuffleServerEndpoint endpoint : endpoints) {
            if (endpoint.partitionStart() != cursor
                    || endpoint.partitionEnd() > partitionCount) {
                throw new IllegalArgumentException(
                        "external shuffle endpoint ranges must be exact and contiguous");
            }
            cursor = endpoint.partitionEnd();
        }
        if (cursor != partitionCount) {
            throw new IllegalArgumentException(
                    "external shuffle endpoint ranges do not cover every partition");
        }
    }

    @Override
    public byte[] expectedProducerSetChecksum() {
        return Arrays.copyOf(
                expectedProducerSetChecksum, expectedProducerSetChecksum.length);
    }

    @Override
    public List<ShuffleServerEndpoint> endpoints() {
        return List.copyOf(endpoints);
    }
}
