package org.pangrova.shuffle.api.exchange;

import static java.util.Objects.requireNonNull;

import org.pangrova.shuffle.api.ownership.ShuffleExecutionOwner;

/** Runtime-neutral identity for one exchange producer or consumer attempt. */
public record ExchangeTaskAttempt(
        String queryId,
        int stageId,
        int taskPartitionId,
        String taskId,
        long controlEpoch,
        ExchangeAttemptFence fence,
        ShuffleExecutionOwner executionOwner,
        String workerAdmissionDomainId,
        String workerIncarnationId) {

    public ExchangeTaskAttempt {
        requireNonNull(queryId, "queryId must not be null");
        requireNonNull(taskId, "taskId must not be null");
        requireNonNull(fence, "fence must not be null");
        requireNonNull(executionOwner, "executionOwner must not be null");
        requireCanonicalUuid(workerAdmissionDomainId, "workerAdmissionDomainId");
        requireCanonicalUuid(workerIncarnationId, "workerIncarnationId");
        if (queryId.isBlank()) {
            throw new IllegalArgumentException("queryId must not be blank");
        }
        if (stageId < 0) {
            throw new IllegalArgumentException("stageId must not be negative");
        }
        if (taskPartitionId < 0) {
            throw new IllegalArgumentException("taskPartitionId must not be negative");
        }
        if (taskId.isBlank()) {
            throw new IllegalArgumentException("taskId must not be blank");
        }
        if (controlEpoch <= 0) {
            throw new IllegalArgumentException("controlEpoch must be positive");
        }
    }

    private static void requireCanonicalUuid(String value, String name) {
        requireNonNull(value, name + " must not be null");
        if (!java.util.UUID.fromString(value).toString().equals(value)) {
            throw new IllegalArgumentException(name + " must be a canonical UUID");
        }
    }
}
