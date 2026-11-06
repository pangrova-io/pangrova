package org.pangrova.coordinator.query.task;

import java.util.concurrent.ConcurrentHashMap;
import java.util.Set;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.coordinator.query.execution.dispatch.dag.StageCompletionCallback;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.query.api.error.QueryFailure;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Logical query completion after required task/write effects; physical cleanup remains in QueryManager. */
@ThreadSafe
final class TaskQueryCompletion {
    private static final Logger LOG = LoggerFactory.getLogger(TaskQueryCompletion.class);
    private final QueryManager queryManager;
    private final CoordinatorTaskTracker taskTracker;
    private final StageCompletionCallback stageCompletionCallback;
    private final NativeTaskWriteCompletion writes;
    private final TaskReportSequences sequences;
    private final ConcurrentHashMap<String, Boolean> finalizationGuard = new ConcurrentHashMap<>();
    private final Set<String> deferredQueries = ConcurrentHashMap.newKeySet();

    TaskQueryCompletion(TaskStatusReceiverContext deps, NativeTaskWriteCompletion writes,
            TaskReportSequences sequences) {
        queryManager = deps.queryManager();
        taskTracker = deps.taskTracker();
        stageCompletionCallback = deps.stageCompletionCallback();
        this.writes = writes;
        this.sequences = sequences;
    }

    void evaluateQueryTransitions(String queryId, TaskStatusReport report) {
        boolean allFinished = taskTracker.allTasksFinished(queryId);
        boolean hasFailed = taskTracker.hasFailedTask(queryId);

        if (allFinished) {
            // When DAG dispatch is active, taskTracker may report allTasksFinished
            // before downstream stage tasks are registered (async dispatch race).
            // Consult the DAG engine to confirm all stages have truly completed.
            if (stageCompletionCallback != null
                    && !stageCompletionCallback.isQueryComplete(queryId)) {
                LOG.debug("event=query_finalization_deferred queryId={}"
                        + " reason=dag_stages_pending", queryId);
                // The final task report can precede an asynchronous stage
                // transition. Observe the DAG's terminal event so completion
                // cannot be lost between the two owners.
                observeDeferredCompletion(queryId);
                return;
            }
            finalizeCompletedQuery(queryId);
        } else if (hasFailed) {
            writes.cleanupWriteStateOnFailure(queryId);
            clearQuerySequences(queryId);
            queryManager.onTaskFailed(queryId, TaskReportQueryAdapter.failure(report));
        }
    }

    /** Keeps the query's metadata operation alive until its DAG reports a terminal outcome. */
    private void observeDeferredCompletion(String queryId) {
        if (!deferredQueries.add(queryId)) {
            return;
        }
        org.pangrova.query.api.lifecycle.QueryOperation operation;
        try {
            operation = taskTracker.enterQueryMetadataOperation(queryId);
        } catch (RuntimeException | Error failure) {
            deferredQueries.remove(queryId);
            throw failure;
        }
        try {
            stageCompletionCallback.onQueryTerminal(queryId, successful -> {
                try {
                    if (successful && mayFinalize(queryId) && taskTracker.allTasksFinished(queryId)) {
                        LOG.info("event=query_finalization_dag_complete queryId={}", queryId);
                        finalizeCompletedQuery(queryId);
                    }
                } finally {
                    deferredQueries.remove(queryId);
                    operation.close();
                }
            });
        } catch (RuntimeException | Error failure) {
            deferredQueries.remove(queryId);
            operation.close();
            throw failure;
        }
    }

    private boolean mayFinalize(String queryId) {
        if (taskTracker.hasFailedTask(queryId)) {
            return false;
        }
        return queryManager.getQueryState(queryId).filter(state -> switch (state) {
            case FINISHED, FAILED, CANCELLED -> false;
            default -> true;
        }).isPresent();
    }

    private void finalizeCompletedQuery(String queryId) {
        // Single-finalizer guard: only one thread may finalize a given query.
        // Prevents concurrent finalization race where a non-winner thread drives
        // the query to FINISHED while the commit-winner is still in flight.
        if (finalizationGuard.putIfAbsent(queryId, Boolean.TRUE) != null) {
            LOG.debug("event=finalization_skipped_concurrent queryId={}", queryId);
            return;
        }
        try {
            try {
                if (!writes.commitDistributedWriteIfNeeded(queryId)) {
                    LOG.warn("event=query_finalization_deferred queryId={} "
                            + "reason=execution_owner_fenced", queryId);
                    return;
                }
            } catch (Exception e) {
                LOG.error("event=distributed_write_commit_failed queryId={} error={}",
                        queryId, e.getMessage(), e);
                writes.cleanupWriteStateAfterCommitFailure(queryId);
                clearQuerySequences(queryId);
                QueryFailure error = e instanceof org.pangrova.common.exception.PangrovaException pangrova
                        ? new QueryFailure(pangrova.getErrorCode().getCode(),
                                e.getClass().getSimpleName(), e.getMessage())
                        : new QueryFailure("WRITE_COMMIT_FAILED", e.getClass().getSimpleName(),
                                "Distributed write commit failed: " + e.getMessage());
                queryManager.onTaskFailed(queryId, error);
                return;
            }
            clearQuerySequences(queryId);
            queryManager.onAllTasksFinished(queryId);
            // Complete waiters only after any deferred distributed write commit
            // is visible to the next sequential DML phase.
            for (String taskId : taskTracker.getTaskIds(queryId)) {
                taskTracker.signalTaskTerminal(queryId, taskId);
            }
        } finally {
            finalizationGuard.remove(queryId);
        }
    }

    /**
     * Removes all sequence tracking entries for tasks belonging to the given query.
     * Prevents unbounded growth of the report-sequence gate map.
     */
    private void clearQuerySequences(String queryId) {
        for (String taskId : taskTracker.getTaskIds(queryId)) {
            sequences.clearTaskSequence(taskId);
        }
    }

}
