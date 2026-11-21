// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

package org.pangrova.coordinator.scheduler;

import org.pangrova.coordinator.enums.SchedulerCancellationStatus;

/**
 * Structured result of a task cancellation attempt.
 *
 * @param status       outcome of the cancel operation
 * @param errorMessage human-readable error detail (null on success)
 * @param retryable    whether the caller may retry the cancel
 */
public record SchedulerCancelResult(SchedulerCancellationStatus status, String errorMessage, boolean retryable,
        org.pangrova.protocol.v2.TaskReport executionDrainReceipt) {

    public SchedulerCancelResult {
        if (status == SchedulerCancellationStatus.DRAINED && executionDrainReceipt == null
                || status != SchedulerCancellationStatus.DRAINED && executionDrainReceipt != null) {
            throw new IllegalArgumentException("Only actual drained cancellation carries its canonical receipt");
        }
    }

    public static SchedulerCancelResult drained(org.pangrova.protocol.v2.TaskReport receipt) {
        return new SchedulerCancelResult(SchedulerCancellationStatus.DRAINED, null, false, receipt);
    }

    public static SchedulerCancelResult pending() {
        return new SchedulerCancelResult(SchedulerCancellationStatus.PENDING, "Execution owner has not drained",
                true, null);
    }

    public static SchedulerCancelResult failed(String message, boolean retryable) {
        return new SchedulerCancelResult(SchedulerCancellationStatus.FAILED, message, retryable, null);
    }

    public boolean isSuccess() {
        return status == SchedulerCancellationStatus.DRAINED;
    }
}
