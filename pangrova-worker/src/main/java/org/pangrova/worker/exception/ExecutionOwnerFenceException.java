package org.pangrova.worker.exception;

/** Rejection raised when a task carries a stale or inconsistent execution owner. */
public final class ExecutionOwnerFenceException extends IllegalStateException {

    public ExecutionOwnerFenceException(String message) {
        super(message);
    }
}
