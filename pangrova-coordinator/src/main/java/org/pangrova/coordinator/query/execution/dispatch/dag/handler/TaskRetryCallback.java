package org.pangrova.coordinator.query.execution.dispatch.dag.handler;

/**
 * Callback for task-level retry dispatch.
 * Invoked when a task fails with a retryable error and has not exceeded
 * the per-task retry limit.
 */
@FunctionalInterface
public interface TaskRetryCallback {

    /**
     * Re-dispatches a failed task on a different worker.
     *
     * @param taskId          the failed task identifier
     * @param stageId         the owning stage identifier
     * @param generation      the stage generation at dispatch time
     * @param excludeWorkerId the worker that failed (excluded from selection)
     */
    void retryTask(String taskId, int stageId, int generation,
                   String excludeWorkerId);
}
