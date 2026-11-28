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
 * @param issuance       committed pre-dispatch authority for an initial scheduled fact
 * @param reportAuthority exact execution grant used by a Worker status report
 */
@Experimental
public record TaskStateEvent(
        JournalEventContext context,
        String queryId,
        String taskId,
        String workerId,
        String state,
        int stageId,
        long reportSequence,
        TaskIssuance issuance,
        ReportAuthority reportAuthority
) {
    public TaskStateEvent(JournalEventContext context, String queryId, String taskId, String workerId,
            String state, int stageId, long reportSequence) {
        this(context, queryId, taskId, workerId, state, stageId, reportSequence, null, null);
    }

    public TaskStateEvent(JournalEventContext context, String queryId, String taskId, String workerId,
            String state, int stageId, long reportSequence, TaskIssuance issuance) {
        this(context, queryId, taskId, workerId, state, stageId, reportSequence, issuance, null);
    }

    public TaskStateEvent {
        Objects.requireNonNull(context, "context must not be null");
        Objects.requireNonNull(queryId, "queryId must not be null");
        Objects.requireNonNull(taskId, "taskId must not be null");
        Objects.requireNonNull(state, "state must not be null");
        if (issuance != null && (!"SCHEDULED".equals(state) || reportSequence != 0 || workerId == null
                || context.coordinatorId() == null)) {
            throw new IllegalArgumentException("Only an exact scheduled event may carry task issuance");
        }
        if (reportAuthority != null && (issuance != null || workerId == null || context.coordinatorId() == null)) {
            throw new IllegalArgumentException("A Worker report requires its exact issuing owner");
        }
    }

    /** Original issuing grant carried by the validated task attempt. */
    public record ReportAuthority(String ownerIncarnation, long fencingToken) {
        public ReportAuthority {
            if (Objects.requireNonNull(ownerIncarnation, "ownerIncarnation").isBlank() || fencingToken <= 0) {
                throw new IllegalArgumentException("Invalid Worker report execution grant");
            }
        }
    }
}
