package org.pangrova.worker.enums;

/** Worker-local task states governed by {@link org.pangrova.worker.task.state.TaskStateTransitions}. */
public enum TaskState {
    RECEIVED, INITIALIZING, RUNNING, FINISHING, CANCELLING,
    FINISHED, FAILED, CANCELLED
}
