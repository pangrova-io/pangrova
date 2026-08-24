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
    private final long taskTimeoutSeconds;
    private final TaskCancelRetrier cancelRetrier;

    public DispatchCanceller(TaskDispatcher dispatcher,
                      CoordinatorTaskTracker taskTracker,
                      long taskTimeoutSeconds) {
        this.taskTracker = taskTracker;
        this.taskTimeoutSeconds = taskTimeoutSeconds;
        this.cancelRetrier = new TaskCancelRetrier(dispatcher, taskTracker);
    }

    public DispatchCanceller(TaskDispatcher dispatcher,
                      CoordinatorTaskTracker taskTracker,
                      long taskTimeoutSeconds,
                      TaskCancelRetrier cancelRetrier) {
        this.taskTracker = taskTracker;
        this.taskTimeoutSeconds = taskTimeoutSeconds;
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

    /**
     * Registers a dispatched task in the task tracker with scheduling state and deadline.
     */
    public void registerDispatchedTask(String queryId, DispatchResult result, String workerId) {
        String taskId = result.taskId();
        try {
            taskTracker.registerTask(queryId, taskId);
            taskTracker.markScheduled(queryId, taskId);
        } catch (IllegalStateException e) {
            // Task already registered by TaskStatusReceiver (early arrival race)
        }
        taskTracker.assignWorker(taskId, workerId);
        if (taskTimeoutSeconds > 0) {
            taskTracker.setDeadline(taskId,
                    java.time.Instant.now().plusSeconds(taskTimeoutSeconds));
        }
    }

    /**
     * Stores the original task request in the task tracker for speculative cloning.
     */
    public void storeTaskRequest(String taskId, org.pangrova.protocol.rest.task.SubmitTaskRequest request) {
        if (request != null) {
            taskTracker.registerTaskRequest(taskId, request);
        }
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
