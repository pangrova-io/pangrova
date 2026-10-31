package org.pangrova.protocol.execution;

/**
 * SPI representation of task execution status.
 *
 * @param state the current task state
 */
public record TaskStatus(TaskState state) {

    /**
     * Factory method for creating a TaskStatus from a state.
     */
    public static TaskStatus of(TaskState state) {
        return new TaskStatus(state);
    }

    /**
     * Task lifecycle states as seen by the SPI layer.
     * Aligned 1:1 with Protobuf {@code TaskState} in pangrova_native_protocol.proto.
     */
    public enum TaskState {
        PENDING, RUNNING, FLUSHING, FINISHED, FAILED, CANCELLED
    }
}
