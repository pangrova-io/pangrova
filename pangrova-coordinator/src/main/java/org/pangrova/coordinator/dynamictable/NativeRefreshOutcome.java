package org.pangrova.coordinator.dynamictable;

import java.util.Objects;

/**
 * Outcome of a native Dynamic Table refresh dispatch.
 *
 * <p>Returned by {@link DynamicTableRefreshDispatchFunction#apply}, this record
 * carries durable-commit confirmation from the worker. The caller MUST advance
 * the DT checkpoint only when {@link #success()} is {@code true}.
 *
 * <p>This contract closes F-COORD-FEAT-04: previously the dispatch path was
 * fire-and-forget — the coordinator advanced the checkpoint at dispatch time,
 * before knowing whether the worker had actually written and committed the
 * data. If the worker failed or partially wrote after dispatch, the next
 * refresh started after the now-advanced checkpoint and the delta was
 * permanently skipped (silent materialization gap). With this contract, the
 * dispatch function blocks until the task reaches a terminal state and reports
 * the actual row/byte counts written; the coordinator advances the checkpoint
 * only on confirmed success.
 *
 * @param success            whether the worker task finished and committed successfully
 * @param taskId             dispatched worker task identifier (for tracing/logs)
 * @param processedRows      number of rows the worker actually wrote (0 on failure)
 * @param bytesWritten       number of bytes the worker actually wrote (0 on failure)
 * @param committedCheckpoint the checkpoint value the worker confirmed it advanced past;
 *                           equals the input checkpoint on success, may be null on failure
 * @param errorMessage       failure description when {@link #success()} is {@code false}; null on success
 */
public record NativeRefreshOutcome(
        boolean success,
        String taskId,
        long processedRows,
        long bytesWritten,
        String committedCheckpoint,
        String errorMessage
) {

    public NativeRefreshOutcome {
        Objects.requireNonNull(taskId, "taskId must not be null");
        if (taskId.isBlank()) {
            throw new IllegalArgumentException("taskId must not be blank");
        }
        if (processedRows < 0) {
            throw new IllegalArgumentException("processedRows must be non-negative");
        }
        if (bytesWritten < 0) {
            throw new IllegalArgumentException("bytesWritten must be non-negative");
        }
        if (success && errorMessage != null) {
            throw new IllegalArgumentException("success outcomes must not carry an errorMessage");
        }
        if (success && (committedCheckpoint == null || committedCheckpoint.isBlank())) {
            throw new IllegalArgumentException("success outcomes require a committed checkpoint");
        }
        if (!success && errorMessage == null) {
            throw new IllegalArgumentException("failure outcomes must carry an errorMessage");
        }
        if (!success && committedCheckpoint != null) {
            throw new IllegalArgumentException("failure outcomes must not carry a committed checkpoint");
        }
    }

    /** Rejects a missing, failed, or mismatched durable commit before checkpoint publication. */
    public NativeRefreshOutcome requireCommittedCheckpoint(String expectedCheckpoint) {
        if (expectedCheckpoint == null || expectedCheckpoint.isBlank()) {
            throw new IllegalStateException("Native refresh source checkpoint is missing");
        }
        if (!success) {
            throw new IllegalStateException("Native refresh task " + taskId + " failed: " + errorMessage);
        }
        if (!expectedCheckpoint.equals(committedCheckpoint)) {
            throw new IllegalStateException("Native refresh committed a different source checkpoint");
        }
        return this;
    }

    /**
     * Builds a successful outcome confirming the worker committed the dispatched plan.
     */
    public static NativeRefreshOutcome success(String taskId,
                                               long processedRows,
                                               long bytesWritten,
                                               String committedCheckpoint) {
        return new NativeRefreshOutcome(true, taskId, processedRows, bytesWritten,
                committedCheckpoint, null);
    }

    /**
     * Builds a failure outcome. The caller MUST NOT advance the DT checkpoint.
     */
    public static NativeRefreshOutcome failure(String taskId, String errorMessage) {
        return new NativeRefreshOutcome(false, taskId, 0L, 0L, null,
                Objects.requireNonNull(errorMessage, "errorMessage must not be null"));
    }
}
