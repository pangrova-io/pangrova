package org.pangrova.query.api.journal;

import org.pangrova.common.api.annotation.Experimental;

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
 * @param context        immutable identity, original time and issuing owner
 */
@Experimental
public record TaskStateEvent(
        JournalEventContext context,
        String queryId,
        String taskId,
        String workerId,
        String state,
        int stageId,
        long reportSequence
) {
    public TaskStateEvent {
        Objects.requireNonNull(context, "context must not be null");
        Objects.requireNonNull(queryId, "queryId must not be null");
        Objects.requireNonNull(taskId, "taskId must not be null");
        Objects.requireNonNull(state, "state must not be null");
    }
}
