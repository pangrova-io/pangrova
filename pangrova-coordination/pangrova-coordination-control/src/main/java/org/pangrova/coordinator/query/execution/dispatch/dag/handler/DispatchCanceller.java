package org.pangrova.coordinator.query.execution.dispatch.dag.handler;

import org.pangrova.execution.task.StageDispatchRecord;

import java.util.List;

import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.execution.task.DispatchResult;
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
                        List<StageDispatchRecord> dispatched) {
        LOG.warn("event=cancel_all_tasks queryId={} taskCount={}", queryId, dispatched.size());
        for (StageDispatchRecord rec : dispatched) {
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

    /** Captures the original prepared event and the exact Worker address and binary seal. */
    public org.pangrova.query.api.journal.TaskStateEvent issuanceFact(
            org.pangrova.protocol.rest.task.SubmitTaskRequest request,
            org.pangrova.coordinator.worker.WorkerInfo worker) {
        var seal = org.pangrova.protocol.task.CanonicalTaskAdmissionSeals.create(request.taskRequest());
        var owner = seal.getAttempt().getExecutionOwner();
        var fact = taskTracker.scheduledJournalEvent(seal.getAttempt().getTaskId());
        var issuance = new org.pangrova.query.api.journal.TaskIssuance(
                worker.host(), worker.httpPort(), owner.getOwnerIncarnation(), owner.getFencingToken(),
                seal.toByteArray());
        return new org.pangrova.query.api.journal.TaskStateEvent(fact.context(), fact.queryId(), fact.taskId(),
                fact.workerId(), fact.state(), fact.stageId(), fact.reportSequence(), issuance);
    }

    /** Rolls back a prepared task only when dispatch did not start. */
    public void discardPreparedTask(
            org.pangrova.protocol.rest.task.SubmitTaskRequest request) {
        var attempt = request.taskRequest().getAttempt();
        taskTracker.unregisterUndispatchedTask(
                attempt.getQueryOrJobId(), attempt.getTaskId());
    }

    /**
     * Converts a dispatch failure into a query exception while retaining the original cause.
     */
    public static RuntimeException wrapIfNeeded(Throwable failure, String queryId) {
        if (failure instanceof RuntimeException runtime) {
            return runtime;
        }
        if (failure instanceof LinkageError linkage) {
            return new PangrovaException(PangrovaErrorCode.COORDINATOR_DISPATCH_FAILED,
                    "DAG dispatch could not link a runtime class [queryId=" + queryId + "]: " + linkage, linkage);
        }
        if (!(failure instanceof Exception)) {
            return new RuntimeException(failure);
        }
        return new PangrovaException(PangrovaErrorCode.RUNTIME_TASK_FAILED,
                "Query execution failed [queryId=" + queryId + "]", failure);
    }
}
