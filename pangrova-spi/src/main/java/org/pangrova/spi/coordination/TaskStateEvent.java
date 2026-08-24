package org.pangrova.spi.coordination;

import java.util.Objects;

/**
 * Immutable journal event for task state transitions.
 * Captures the essential state needed to rebuild task tracking on HA recovery.
 *
 * @param queryId        owning query identifier (required)
 * @param taskId         unique task identifier (required)
 * @param workerId       the Worker executing this task (nullable for pre-dispatch events)
 * @param state          the task state at this transition point (required)
 * @param stageId        the stage this task belongs to
 * @param reportSequence monotonically increasing sequence for deduplication
 * @param timestampMs    epoch millis when the transition occurred
 * @param coordinatorId  the coordinator that owns this task (nullable for legacy events)
 */
public record TaskStateEvent(
        String queryId,
        String taskId,
        String workerId,
        String state,
        int stageId,
        long reportSequence,
        long timestampMs,
        String coordinatorId
) {
    public TaskStateEvent {
        Objects.requireNonNull(queryId, "queryId must not be null");
        Objects.requireNonNull(taskId, "taskId must not be null");
        Objects.requireNonNull(state, "state must not be null");
        if (timestampMs < 0) {
            throw new IllegalArgumentException("timestampMs must be non-negative");
        }
    }

    /** Backward-compatible constructor without coordinatorId. */
    public TaskStateEvent(String queryId, String taskId, String workerId,
                          String state, int stageId, long reportSequence,
                          long timestampMs) {
        this(queryId, taskId, workerId, state, stageId, reportSequence, timestampMs, null);
    }
}
