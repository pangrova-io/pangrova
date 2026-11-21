package org.pangrova.worker.task;

import org.pangrova.worker.enums.TaskCancellationStatus;

/**
 * Structured result of a worker-side task cancellation attempt.
 * Replaces the previous boolean return from {@link TaskManager#cancelTask(String)}.
 *
 * @param status       outcome of the cancel operation
 * @param errorMessage human-readable error detail (null on success)
 */
public record TaskCancelResult(TaskCancellationStatus status, String errorMessage) {

    public static TaskCancelResult cancelled() {
        return new TaskCancelResult(TaskCancellationStatus.CANCELLED, null);
    }

    public static TaskCancelResult notFound(String taskId) {
        return new TaskCancelResult(TaskCancellationStatus.NOT_FOUND, "Task not found: " + taskId);
    }

    public static TaskCancelResult notCancellable(String taskId, String currentState) {
        return new TaskCancelResult(TaskCancellationStatus.NOT_CANCELLABLE,
                "Task " + taskId + " in state " + currentState + " cannot be cancelled");
    }

    public boolean isSuccess() {
        return status == TaskCancellationStatus.CANCELLED;
    }
}
