package org.pangrova.shuffle.api.registration;

import org.pangrova.shuffle.api.capability.ShuffleAccess;
import org.pangrova.shuffle.api.ownership.ShuffleExecutionOwner;
import static java.util.Objects.requireNonNull;

import java.util.List;

/** Coordinator-allocated external shuffle binding installed on a Worker. */
public record ShuffleChannelBinding(
        String queryId,
        int stageId,
        long planEpoch,
        int numPartitions,
        ShuffleExecutionOwner executionOwner,
        String shuffleId,
        List<ShuffleServerEndpoint> endpoints,
        ShuffleServiceAddress commitAddress,
        ShuffleAccess writeAccess) {

    public ShuffleChannelBinding {
        validateReferences(queryId, executionOwner, endpoints, commitAddress, writeAccess);
        endpoints = List.copyOf(endpoints);
        validateGeneration(queryId, stageId, planEpoch, numPartitions);
        if (shuffleId == null || shuffleId.isBlank() || endpoints.isEmpty()) {
            throw new IllegalArgumentException(
                    "external shuffle binding must have an id and endpoints");
        }
        validateCoverage(endpoints, numPartitions);
    }

    private static void validateReferences(
            String queryId,
            ShuffleExecutionOwner owner,
            List<ShuffleServerEndpoint> endpoints,
            ShuffleServiceAddress commitAddress,
            ShuffleAccess writeAccess) {
        requireNonNull(queryId, "queryId must not be null");
        requireNonNull(owner, "executionOwner must not be null");
        requireNonNull(endpoints, "endpoints must not be null");
        requireNonNull(commitAddress, "commitAddress must not be null");
        requireNonNull(writeAccess, "writeAccess must not be null");
    }

    private static void validateGeneration(
            String queryId,
            int stageId,
            long planEpoch,
            int numPartitions) {
        if (queryId.isBlank() || stageId < 0 || planEpoch <= 0 || numPartitions <= 0) {
            throw new IllegalArgumentException("shuffle generation identity is invalid");
        }
    }

    private static void validateCoverage(
            List<ShuffleServerEndpoint> endpoints,
            int numPartitions) {
        int cursor = 0;
        for (ShuffleServerEndpoint endpoint : endpoints.stream()
                .sorted(java.util.Comparator.comparingInt(
                        ShuffleServerEndpoint::partitionStart)).toList()) {
            if (endpoint.partitionStart() != cursor
                    || endpoint.partitionEnd() > numPartitions) {
                throw new IllegalArgumentException(
                        "shuffle endpoint ranges must exactly cover every partition");
            }
            cursor = endpoint.partitionEnd();
        }
        if (cursor != numPartitions) {
            throw new IllegalArgumentException(
                    "shuffle endpoint ranges must exactly cover every partition");
        }
    }
}
