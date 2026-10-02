package org.pangrova.shuffle.exchange;

import static java.util.Objects.requireNonNull;

/**
 * Fenced identity and deadline for one producer or consumer exchange channel.
 *
 * <p>This is the runtime-neutral exchange contract. Protocol adapters translate it
 * to the canonical v2 task attempt message; shuffle providers must not invent a
 * provider-specific attempt identity.</p>
 */
public record ExchangeChannelContext(
        ExchangeTaskAttempt taskAttempt,
        int exchangePartitionId,
        long deadlineEpochMillis) {

    public ExchangeChannelContext {
        requireNonNull(taskAttempt, "taskAttempt must not be null");
        if (exchangePartitionId < 0) {
            throw new IllegalArgumentException("exchangePartitionId must not be negative");
        }
        if (deadlineEpochMillis <= 0) {
            throw new IllegalArgumentException("deadlineEpochMillis must be positive");
        }
    }

    public String queryId() {
        return taskAttempt.queryId();
    }

    public int stageId() {
        return taskAttempt.stageId();
    }

    public int taskPartitionId() {
        return taskAttempt.taskPartitionId();
    }

    public long planEpoch() {
        return taskAttempt.fence().planEpoch();
    }

    public int attemptId() {
        return taskAttempt.fence().attemptId();
    }

    public long fencingToken() {
        return taskAttempt.fence().fencingToken();
    }

    public String taskId() {
        return taskAttempt.taskId();
    }

    public long controlEpoch() {
        return taskAttempt.controlEpoch();
    }

    public org.pangrova.shuffle.ShuffleExecutionOwner executionOwner() {
        return taskAttempt.executionOwner();
    }
}
