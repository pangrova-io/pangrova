package org.pangrova.coordinator.query.execution.dispatch.dag.handler;

import java.util.List;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.coordinator.query.execution.dispatch.DispatchResult;
import org.pangrova.coordinator.query.execution.dispatch.dag.DAGDispatchEngine;
import org.pangrova.coordinator.scheduler.TaskDispatcher;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.task.TaskCancelRetrier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles cancellation of dispatched tasks and task registration bookkeeping.
 * Extracted from DAGDispatchEngine to isolate cancel/registration concerns.
 *
 * <p>BUG-D12 fix: delegates individual cancel calls to {@link TaskCancelRetrier}
 * which logs at WARN on failure and enqueues for periodic retry, instead of
 * silently swallowing failures at DEBUG level.</p>
 */
public final class DispatchCanceller {

    private static final Logger LOG = LoggerFactory.getLogger(DispatchCanceller.class);

    private final CoordinatorTaskTracker taskTracker;
    private final TaskCancelRetrier cancelRetrier;

    public DispatchCanceller(TaskDispatcher dispatcher,
                      CoordinatorTaskTracker taskTracker) {
        this.taskTracker = taskTracker;
        this.cancelRetrier = new TaskCancelRetrier(dispatcher, taskTracker);
    }

    public DispatchCanceller(TaskDispatcher dispatcher,
                      CoordinatorTaskTracker taskTracker,
                      TaskCancelRetrier cancelRetrier) {
        this.taskTracker = taskTracker;
        this.cancelRetrier = cancelRetrier;
    }

    /**
     * Cancels all dispatched tasks for a failed query.
     * BUG-D12 fix: delegates to {@link TaskCancelRetrier#cancelWithRetry}
     * so failures are logged at WARN and enqueued for retry.
     */
    public void cancelAllTasks(String queryId,
                        List<DAGDispatchEngine.StageDispatchRecord> dispatched) {
        LOG.warn("event=cancel_all_tasks queryId={} taskCount={}", queryId, dispatched.size());
        for (DAGDispatchEngine.StageDispatchRecord rec : dispatched) {
            cancelRetrier.cancelWithRetry(queryId, rec.result().taskId(),
                    rec.result().workerHost(), rec.workerHttpPort());
        }
    }

    /**
     * Returns the cancel retrier for periodic retry integration.
     */
    public TaskCancelRetrier cancelRetrier() {
        return cancelRetrier;
    }

    /** Cancels through the retained exact request, not a raw task-id network command. */
    public void cancelTask(String queryId, String taskId, String workerHost, int workerHttpPort) {
        cancelRetrier.cancelWithRetry(queryId, taskId, workerHost, workerHttpPort);
    }

    /**
     * Registers a dispatched task in the task tracker with scheduling state and deadline.
     */
    public void registerDispatchedTask(String queryId, DispatchResult result, String workerId) {
        String taskId = result.taskId();
        // The state machine was installed before HTTP dispatch. A Worker may
        // already have reported RUNNING or a terminal state by this point.
        taskTracker.getTaskState(queryId, taskId);
        taskTracker.assignWorker(taskId, workerId);
    }

    /** Registers the immutable task identity before any bytes are sent to a Worker. */
    public void prepareTaskDispatch(
            org.pangrova.protocol.rest.task.SubmitTaskRequest request,
            org.pangrova.coordinator.worker.WorkerInfo worker) {
        var attempt = request.taskRequest().getAttempt();
        String taskId = attempt.getTaskId();
        String queryId = attempt.getQueryOrJobId();
        taskTracker.registerTask(queryId, taskId);
        try {
            taskTracker.registerTaskRequest(taskId, request);
            taskTracker.bindDispatchTarget(request, worker);
            taskTracker.markScheduled(queryId, taskId);
            taskTracker.assignWorker(taskId, worker.workerId());
            taskTracker.setDeadline(taskId, java.time.Instant.ofEpochMilli(request.taskRequest().getDeadlineEpochMs()));
        } catch (RuntimeException failure) {
            taskTracker.unregisterUndispatchedTask(queryId, taskId);
            throw failure;
        }
    }

    public void enterTaskDispatch(org.pangrova.protocol.rest.task.SubmitTaskRequest request) {
        taskTracker.enterTaskDispatch(request);
    }

    /** Journals the same frozen fact retained by the sole issued-attempt authority. */
    public void journalScheduledTask(org.pangrova.query.api.state.QueryStateJournal journal, String taskId) {
        journal.appendTaskEvent(taskTracker.scheduledJournalEvent(taskId));
    }

    /** Rolls back a prepared task only when dispatch did not start. */
    public void discardPreparedTask(
            org.pangrova.protocol.rest.task.SubmitTaskRequest request) {
        var attempt = request.taskRequest().getAttempt();
        taskTracker.unregisterUndispatchedTask(
                attempt.getQueryOrJobId(), attempt.getTaskId());
    }

    /**
     * Wraps a checked exception into a RuntimeException preserving PangrovaException identity.
     */
    public static RuntimeException wrapIfNeeded(Exception e, String queryId) {
        if (e instanceof PangrovaException) {
            return (PangrovaException) e;
        }
        if (e instanceof RuntimeException re) {
            return re;
        }
        return new PangrovaException(PangrovaErrorCode.RUNTIME_TASK_FAILED,
                "Query execution failed [queryId=" + queryId + "]", e);
    }
}
