package org.pangrova.coordinator.query.task;

import java.util.concurrent.ConcurrentHashMap;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.coordinator.query.execution.dispatch.dag.StageCompletionCallback;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
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
    private final org.pangrova.governance.lineage.LineageEventEmitter lineageEmitter;
    private final NativeTaskWriteCompletion writes;
    private final TaskReportSequences sequences;
    private final SpeculativeTaskReportCompletion speculation;
    private final ConcurrentHashMap<String, Boolean> finalizationGuard = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Thread> rechecks = new ConcurrentHashMap<>();

    TaskQueryCompletion(TaskStatusReceiverContext deps, NativeTaskWriteCompletion writes,
            TaskReportSequences sequences, SpeculativeTaskReportCompletion speculation) {
        queryManager = deps.queryManager(); taskTracker = deps.taskTracker();
        stageCompletionCallback = deps.stageCompletionCallback(); lineageEmitter = deps.lineageEmitter();
        this.writes = writes; this.sequences = sequences; this.speculation = speculation;
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
                // Schedule a deferred re-check. The DAG engine may complete
                // the stage during collectResults (early terminal replay),
                // after which isQueryComplete returns true. Without this
                // retry, the query hangs because no one re-triggers
                // evaluateQueryTransitions after the DAG completes.
                scheduleQueryCompletionRecheck(queryId);
                return;
            }
            finalizeCompletedQuery(queryId);
        } else if (hasFailed) {
            writes.cleanupWriteStateOnFailure(queryId);
            clearQuerySequences(queryId);
            queryManager.onTaskFailed(queryId, report.error());
        }
    }

    /**
     * Schedules a deferred re-check of query completion after DAG stages finish.
     * Polls isQueryComplete at short intervals until true, then finalizes.
     */
    private void scheduleQueryCompletionRecheck(String queryId) {
        var operation = taskTracker.enterQueryMetadataOperation(queryId);
        Thread thread;
        try {
            thread = new Thread(() -> {
                try { pollUntilQueryComplete(queryId); }
                finally { rechecks.remove(queryId, Thread.currentThread()); operation.close(); }
            }, "query-recheck-" + queryId);
            thread.setDaemon(true);
            if (rechecks.putIfAbsent(queryId, thread) != null) { operation.close(); return; }
        } catch (RuntimeException | Error failure) { operation.close(); throw failure; }
        try { thread.start(); }
        catch (RuntimeException | Error failure) {
            rechecks.remove(queryId, thread);
            operation.close();
            throw failure;
        }
    }

    /** Polls query completion at 50ms intervals, finalizing when all tasks finish. */
    private void pollUntilQueryComplete(String queryId) {
        for (int i = 0; i < 600; i++) {
            var state = queryManager.getQueryState(queryId);
            if (state.isEmpty() || switch (state.orElseThrow()) {
                case FINISHED, FAILED, CANCELLED -> true;
                default -> false;
            }) return;
            if (!sleepQuietly()) {
                return;
            }
            if (stageCompletionCallback == null
                    || stageCompletionCallback.isQueryComplete(queryId)) {
                if (taskTracker.allTasksFinished(queryId)) {
                    LOG.info("event=query_finalization_recheck_success queryId={} attempts={}",
                            queryId, i + 1);
                    finalizeCompletedQuery(queryId);
                }
                return;
            }
        }
        LOG.warn("event=query_finalization_recheck_timeout queryId={}", queryId);
    }

    /** Sleeps 50ms; returns false if interrupted. */
    private static boolean sleepQuietly() {
        try {
            Thread.sleep(50);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
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
                queryManager.onTaskFailed(queryId,
                        new org.pangrova.protocol.rest.error.ErrorDetail(
                                "WRITE_COMMIT_FAILED", e.getClass().getSimpleName(),
                                "Distributed write commit failed: " + e.getMessage()));
                return;
            }
            emitTaskCompletedLineage(queryId);
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
        speculation.clearQuery(queryId);
    }

    private void emitTaskCompletedLineage(String queryId) {
        if (lineageEmitter == null) {
            return;
        }
        try {
            lineageEmitter.emitCompleteEvent(queryId);
        } catch (Exception e) {
            LOG.debug("event=lineage_task_completed_failed queryId={} error={}",
                    queryId, e.getMessage());
        }
    }
}
