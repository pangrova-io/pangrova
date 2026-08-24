// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

package org.pangrova.coordinator.scheduler;

/**
 * Structured result of a task cancellation attempt.
 *
 * @param status       outcome of the cancel operation
 * @param errorMessage human-readable error detail (null on success)
 * @param retryable    whether the caller may retry the cancel
 */
public record SchedulerCancelResult(Status status, String errorMessage, boolean retryable) {

    public enum Status {
        CANCELLED,
        NOT_FOUND,
        FAILED
    }

    public static SchedulerCancelResult cancelled() {
        return new SchedulerCancelResult(Status.CANCELLED, null, false);
    }

    public static SchedulerCancelResult notFound(String taskId) {
        return new SchedulerCancelResult(Status.NOT_FOUND, "Task not found: " + taskId, false);
    }

    public static SchedulerCancelResult failed(String message, boolean retryable) {
        return new SchedulerCancelResult(Status.FAILED, message, retryable);
    }

    public boolean isSuccess() {
        return status == Status.CANCELLED;
    }
}
