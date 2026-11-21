package org.pangrova.worker.enums;

/** Worker classification used for task failure reporting. */
public enum NativeTaskFailureCategory {
    /** C++ process crashed (SIGSEGV, abort). Fail-fast, no retry. */
    NATIVE_CRASHED,
    /** Native executor unresponsive. Escalate as heartbeat timeout. */
    NATIVE_HUNG,
    /** Native task reached FAILED terminal state. Fail-fast, no retry. */
    NATIVE_TASK_FAILED,
    /** Transient IO error. Backoff retry up to max attempts. */
    TRANSIENT
}
