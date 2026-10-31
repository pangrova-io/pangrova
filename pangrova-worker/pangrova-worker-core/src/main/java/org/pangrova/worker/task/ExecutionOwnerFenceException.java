package org.pangrova.worker.task;

/** Rejection raised when a task carries a stale or inconsistent execution owner. */
public final class ExecutionOwnerFenceException extends IllegalStateException {

    ExecutionOwnerFenceException(String message) {
        super(message);
    }
}
