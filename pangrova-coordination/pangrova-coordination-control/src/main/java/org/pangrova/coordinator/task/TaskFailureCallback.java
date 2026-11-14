package org.pangrova.coordinator.task;

/**
 * Callback invoked when a task transitions to {@link TaskStateMachine.State#FAILED}.
 * Registered by the query lifecycle layer (e.g. QueryManager / DAGDispatchEngine)
 * to propagate task failure to stage-level and query-level failure handling.
 *
 * <p>Implementations must be thread-safe — the callback may be invoked from any
 * thread that calls {@link CoordinatorTaskTracker#updateTaskState}.</p>
 */
@FunctionalInterface
public interface TaskFailureCallback {

    /**
     * Called after a task has transitioned to FAILED.
     *
     * @param queryId the owning query identifier
     * @param taskId  the failed task identifier
     */
    void onTaskFailed(String queryId, String taskId);
}
