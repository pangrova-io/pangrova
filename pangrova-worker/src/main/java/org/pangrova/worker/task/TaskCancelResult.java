package org.pangrova.worker.task;

/**
 * Structured result of a worker-side task cancellation attempt.
 * Replaces the previous boolean return from {@link TaskManager#cancelTask(String)}.
 *
 * @param status       outcome of the cancel operation
 * @param errorMessage human-readable error detail (null on success)
 */
public record TaskCancelResult(Status status, String errorMessage) {

    public enum Status {
        CANCELLED,
        NOT_FOUND,
        NOT_CANCELLABLE
    }

    public static TaskCancelResult cancelled() {
        return new TaskCancelResult(Status.CANCELLED, null);
    }

    public static TaskCancelResult notFound(String taskId) {
        return new TaskCancelResult(Status.NOT_FOUND, "Task not found: " + taskId);
    }

    public static TaskCancelResult notCancellable(String taskId, String currentState) {
        return new TaskCancelResult(Status.NOT_CANCELLABLE,
                "Task " + taskId + " in state " + currentState + " cannot be cancelled");
    }

    public boolean isSuccess() {
        return status == Status.CANCELLED;
    }
}
