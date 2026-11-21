package org.pangrova.shuffle.api;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Handle returned by {@link ShuffleLifecycleManager#registerShuffle} identifying
 * a registered shuffle and the remote shuffle server endpoints holding its partitions.
 *
 * @param shuffleId the unique shuffle identifier assigned by the remote shuffle server
 * @param endpoints the remote shuffle server endpoints holding partition data
 */
public record ShuffleHandle(
        String shuffleId,
        List<ShuffleServerEndpoint> endpoints,
        ShuffleServiceAddress commitAddress,
        ShuffleAccess readAccess,
        ShuffleAccess writeAccess,
        ShuffleAccess finalizeAccess,
        ShuffleAccess cleanupAccess,
        int expectedProducerCount,
        byte[] expectedProducerSetChecksum) {

    public ShuffleHandle {
        validateIdentity(shuffleId, endpoints);
        endpoints = endpoints.stream()
                .sorted(Comparator.comparingInt(ShuffleServerEndpoint::partitionStart))
                .toList();
        validateExternal(commitAddress, readAccess, writeAccess,
                finalizeAccess, cleanupAccess, expectedProducerCount, expectedProducerSetChecksum);
        expectedProducerSetChecksum = Arrays.copyOf(
                expectedProducerSetChecksum, expectedProducerSetChecksum.length);
    }

    private static void validateIdentity(
            String shuffleId,
            List<ShuffleServerEndpoint> endpoints) {
        checkNotNull(shuffleId, "shuffleId must not be null");
        checkNotNull(endpoints, "endpoints must not be null");
        if (endpoints.isEmpty()) {
            throw new IllegalArgumentException("external shuffle handle requires partition endpoints");
        }
        if (shuffleId.isBlank()) {
            throw new IllegalArgumentException("shuffleId must not be blank");
        }
    }

    private static void validateExternal(
            ShuffleServiceAddress commitAddress,
            ShuffleAccess readAccess,
            ShuffleAccess writeAccess,
            ShuffleAccess finalizeAccess,
            ShuffleAccess cleanupAccess,
            int expectedProducerCount,
            byte[] producerChecksum) {
        validateDataCapabilities(commitAddress, readAccess, writeAccess);
        validateControlCapabilities(finalizeAccess, cleanupAccess);
        validateProducerIdentity(expectedProducerCount, producerChecksum);
    }

    private static void validateDataCapabilities(
            ShuffleServiceAddress commitAddress,
            ShuffleAccess readAccess,
            ShuffleAccess writeAccess) {
        if (commitAddress == null || readAccess == null || writeAccess == null) {
            throw new IllegalArgumentException(
                    "external shuffle handle requires data capabilities");
        }
    }

    private static void validateControlCapabilities(
            ShuffleAccess finalizeAccess,
            ShuffleAccess cleanupAccess) {
        if (finalizeAccess == null || cleanupAccess == null) {
            throw new IllegalArgumentException(
                    "external shuffle handle requires control capabilities");
        }
    }

    private static void validateProducerIdentity(
            int expectedProducerCount,
            byte[] producerChecksum) {
        if (expectedProducerCount <= 0 || producerChecksum == null
                || producerChecksum.length != 32) {
            throw new IllegalArgumentException(
                    "external shuffle handle requires producer identity");
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

    /** Returns the sole endpoint owning {@code partitionId}. */
    public ShuffleServerEndpoint endpointForPartition(int partitionId) {
        if (partitionId < 0) {
            throw new IllegalArgumentException("partitionId must be non-negative");
        }
        int low = 0;
        int high = endpoints.size() - 1;
        while (low <= high) {
            int middle = (low + high) >>> 1;
            ShuffleServerEndpoint endpoint = endpoints.get(middle);
            if (partitionId < endpoint.partitionStart()) {
                high = middle - 1;
            } else if (partitionId >= endpoint.partitionEnd()) {
                low = middle + 1;
            } else {
                return endpoint;
            }
        }
        throw new IllegalArgumentException(
                "no shuffle endpoint owns partition " + partitionId);
    }

    /** Validates exact, non-overlapping coverage of {@code [0, partitionCount)}. */
    public void validatePartitionCoverage(int partitionCount) {
        if (partitionCount <= 0) {
            throw new IllegalArgumentException("partitionCount must be positive");
        }
        int cursor = 0;
        for (ShuffleServerEndpoint endpoint : endpoints) {
            if (endpoint.partitionStart() != cursor
                    || endpoint.partitionEnd() > partitionCount) {
                throw new IllegalArgumentException(
                        "shuffle endpoint ranges must exactly cover [0, partitionCount)");
            }
            cursor = endpoint.partitionEnd();
        }
        if (cursor != partitionCount) {
            throw new IllegalArgumentException(
                    "shuffle endpoint ranges must exactly cover [0, partitionCount)");
        }
    }
}
