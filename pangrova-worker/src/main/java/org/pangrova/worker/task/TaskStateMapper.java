package org.pangrova.worker.task;

import org.pangrova.protocol.v2.TaskState;
import org.pangrova.protocol.execution.TaskStatus;

/**
 * Maps Worker TaskState to SPI TaskState and Protobuf TaskState.
 * Exhaustive switch (no default) ensures compile-time safety on enum changes.
 *
 * <p>See docs/architecture/jni-runtime.md §1 for TaskState alignment.</p>
 */
final class TaskStateMapper {

    private TaskStateMapper() {}

    /**
     * Maps Worker TaskState to SPI TaskStatus.TaskState.
     * Mapped from the canonical Protobuf TaskState generated for both runtimes.
     */
    public static TaskStatus.TaskState toSpiState(org.pangrova.worker.enums.TaskState state) {
        return switch (state) {
            case RECEIVED -> TaskStatus.TaskState.PENDING;
            case INITIALIZING -> TaskStatus.TaskState.PENDING;
            case RUNNING -> TaskStatus.TaskState.RUNNING;
            case FINISHING -> TaskStatus.TaskState.FLUSHING;
            case CANCELLING -> TaskStatus.TaskState.RUNNING;
            case FINISHED -> TaskStatus.TaskState.FINISHED;
            case FAILED -> TaskStatus.TaskState.FAILED;
            case CANCELLED -> TaskStatus.TaskState.CANCELLED;
        };
    }

    /**
     * Maps Worker TaskState to Protobuf TaskState enum.
     * Values match the Protobuf {@code TaskState} enum exactly.
     */
    public static TaskState toProtoState(org.pangrova.worker.enums.TaskState state) {
        return switch (state) {
            case RECEIVED -> TaskState.TASK_STATE_PENDING;
            case INITIALIZING -> TaskState.TASK_STATE_PENDING;
            case RUNNING -> TaskState.TASK_STATE_RUNNING;
            case FINISHING -> TaskState.TASK_STATE_FLUSHING;
            case CANCELLING -> TaskState.TASK_STATE_RUNNING;
            case FINISHED -> TaskState.TASK_STATE_FINISHED;
            case FAILED -> TaskState.TASK_STATE_FAILED;
            case CANCELLED -> TaskState.TASK_STATE_CANCELLED;
        };
    }

    /**
     * Maps Worker TaskState to Protobuf TaskState string name.
     * Used for REST DTO serialization and logging.
     */
    public static String toProtoStateName(org.pangrova.worker.enums.TaskState state) {
        return toProtoState(state).name();
    }

    /**
     * Maps a REST DTO state string (from TaskStatusReport) to SPI TaskStatus.TaskState.
     * Used by Coordinator-side code to interpret Worker status reports.
     */
    public static TaskStatus.TaskState fromRestState(String restState) {
        return switch (restState) {
            case "PENDING" -> TaskStatus.TaskState.PENDING;
            case "RUNNING" -> TaskStatus.TaskState.RUNNING;
            case "FLUSHING" -> TaskStatus.TaskState.FLUSHING;
            case "FINISHED" -> TaskStatus.TaskState.FINISHED;
            case "FAILED" -> TaskStatus.TaskState.FAILED;
            case "CANCELLED" -> TaskStatus.TaskState.CANCELLED;
            default -> throw new IllegalArgumentException("Unknown REST task state: " + restState);
        };
    }
}
