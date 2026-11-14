package org.pangrova.worker.task;

import org.pangrova.protocol.enums.ReportedTaskState;
import org.pangrova.protocol.v2.TaskState;

/**
 * Projects Worker lifecycle states into public execution and wire states.
 * Exhaustive switch (no default) ensures compile-time safety on enum changes.
 *
 * <p>See docs/architecture/jni-runtime.md §1 for TaskState alignment.</p>
 */
final class TaskStateMapper {

    private TaskStateMapper() {}

    /**
     * Maps the worker-local state into its publicly observable form.
     * The Worker retains finer grained startup and cancellation states.
     */
    public static ReportedTaskState toReportedState(org.pangrova.worker.enums.TaskState state) {
        return switch (state) {
            case RECEIVED -> ReportedTaskState.PENDING;
            case INITIALIZING -> ReportedTaskState.PENDING;
            case RUNNING -> ReportedTaskState.RUNNING;
            case FINISHING -> ReportedTaskState.FLUSHING;
            case CANCELLING -> ReportedTaskState.RUNNING;
            case FINISHED -> ReportedTaskState.FINISHED;
            case FAILED -> ReportedTaskState.FAILED;
            case CANCELLED -> ReportedTaskState.CANCELLED;
        };
    }

    /** Converts the publicly observable state to its generated wire value. */
    public static TaskState toProtoState(org.pangrova.worker.enums.TaskState state) {
        return switch (toReportedState(state)) {
            case PENDING -> TaskState.TASK_STATE_PENDING;
            case RUNNING -> TaskState.TASK_STATE_RUNNING;
            case FLUSHING -> TaskState.TASK_STATE_FLUSHING;
            case FINISHED -> TaskState.TASK_STATE_FINISHED;
            case FAILED -> TaskState.TASK_STATE_FAILED;
            case CANCELLED -> TaskState.TASK_STATE_CANCELLED;
        };
    }

    /**
     * Parses a REST task report state.
     * Used by Coordinator-side code to interpret Worker status reports.
     */
    public static ReportedTaskState fromRestState(String restState) {
        return ReportedTaskState.valueOf(restState);
    }
}
