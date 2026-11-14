package org.pangrova.query.api.task;

import org.pangrova.common.api.annotation.Internal;

/**
 * Callback for task-level retry dispatch.
 * Invoked when a task fails with a retryable error and has not exceeded
 * the per-task retry limit. Invocation may occur on task completion threads;
 * implementations must protect their own admission state and fence stale generations.
 * Returning may mean asynchronous dispatch has been admitted, not that the replacement
 * task completed. The caller borrows this callback and does not close its resources.
 */
@FunctionalInterface
@Internal
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
