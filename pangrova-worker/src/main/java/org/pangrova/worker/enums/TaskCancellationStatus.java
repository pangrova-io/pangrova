package org.pangrova.worker.enums;

/** Outcome of a Worker task cancellation request. */
public enum TaskCancellationStatus {
    CANCELLED,
    NOT_FOUND,
    NOT_CANCELLABLE
}
