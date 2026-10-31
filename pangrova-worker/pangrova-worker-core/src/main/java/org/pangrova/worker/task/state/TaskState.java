package org.pangrova.worker.task.state;

/** Worker-local task states governed by {@link TaskStateTransitions}. */
public enum TaskState {
    RECEIVED, INITIALIZING, RUNNING, FINISHING, CANCELLING,
    FINISHED, FAILED, CANCELLED
}
