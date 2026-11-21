package org.pangrova.coordinator.task;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.pangrova.coordinator.scheduler.SchedulerCancelResult;
import org.pangrova.coordinator.scheduler.TaskDispatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Tracks exact task cancellations across query cleanup, DAG failures and speculative attempts.
 * The coordinator periodically retries retained targets until the Worker seal or an independently
 * validated execution-drain receipt confirms the actual exit.
 */
public final class TaskCancelRetrier {

    private static final Logger LOG = LoggerFactory.getLogger(TaskCancelRetrier.class);

    private final TaskDispatcher dispatcher;
    private final CoordinatorTaskTracker taskTracker;

    /**
     * Tracks tasks whose cancel RPC failed and need retry.
     * Key: taskId, Value: cancel target metadata.
     */
    private final ConcurrentHashMap<String, CancelTarget> pendingCancels =
            new ConcurrentHashMap<>();

    /**
     * Immutable snapshot of the information needed to retry a cancel RPC.
     */
    public record CancelTarget(String queryId, String workerHost, int httpPort) {}

    public TaskCancelRetrier(TaskDispatcher dispatcher,
                             CoordinatorTaskTracker taskTracker) {
        this.dispatcher = checkNotNull(dispatcher, "dispatcher must not be null");
        this.taskTracker = checkNotNull(taskTracker, "taskTracker must not be null");
    }

    /**
     * Attempts to cancel a task on its assigned worker. On failure, logs at
     * WARN and keeps the exact target for periodic retry via {@link #retryPendingCancels()}.
     *
     * @param queryId    the owning query identifier
     * @param taskId     the task to cancel
     * @param workerHost the worker host running the task
     * @param httpPort   the worker HTTP port
     */
    public void cancelWithRetry(String queryId, String taskId,
                                String workerHost, int httpPort) {
        var target = new CancelTarget(queryId, workerHost, httpPort);
        pendingCancels.put(taskId, target);
        retrySingleCancel(taskId, target);
    }

    /**
     * Retries cancellation for all CANCEL_PENDING tasks. Intended to be
     * called from the existing periodic {@code checkTaskTimeouts} scheduler.
     *
     * <p>Successfully cancelled tasks are removed from the pending set.
     * Tasks that still fail remain pending for the next retry cycle.</p>
     *
     * @return the number of tasks successfully cancelled in this cycle
     */
    public int retryPendingCancels() {
        if (pendingCancels.isEmpty()) {
            return 0;
        }
        int cancelled = 0;
        for (Map.Entry<String, CancelTarget> entry : pendingCancels.entrySet()) {
            if (retrySingleCancel(entry.getKey(), entry.getValue())) {
                cancelled++;
            }
        }
        if (!pendingCancels.isEmpty()) {
            LOG.warn("event=cancel_pending_remaining count={}", pendingCancels.size());
        }
        return cancelled;
    }

    private boolean retrySingleCancel(String taskId, CancelTarget target) {
        try {
            var retained = taskTracker.executionDrainReceipt(taskId);
            if (retained.filter(report -> report.getAttempt().getQueryOrJobId().equals(target.queryId())).isPresent()) {
                return pendingCancels.remove(taskId, target);
            }
            var request = requireIssuedRequest(taskId);
            if (!request.taskRequest().getAttempt().getQueryOrJobId().equals(target.queryId())) {
                throw new IllegalStateException("Cancellation belongs to another query");
            }
            SchedulerCancelResult result = dispatcher.cancelTask(request, target.workerHost(), target.httpPort());
            if (result.isSuccess()) {
                taskTracker.recordExecutionDrain(request, result.executionDrainReceipt());
                pendingCancels.remove(taskId, target);
                LOG.info("event=cancel_retry_success queryId={} taskId={}", target.queryId(), taskId);
                return true;
            }
            LOG.warn("event=cancel_retry_failed queryId={} taskId={} status={} error={}",
                    target.queryId(), taskId, result.status(), result.errorMessage());
        } catch (Exception e) {
            LOG.warn("event=cancel_retry_exception queryId={} taskId={} error={}",
                    target.queryId(), taskId, e.getMessage());
        }
        return false;
    }

    private org.pangrova.protocol.rest.task.SubmitTaskRequest requireIssuedRequest(String taskId) {
        var request = taskTracker.getTaskRequest(taskId);
        if (request == null) throw new IllegalStateException("Cannot seal an unissued exact task: " + taskId);
        return request;
    }

    /**
     * Returns the number of tasks currently awaiting cancel retry.
     * Exposed for testing and monitoring.
     */
    public int pendingCount() {
        return pendingCancels.size();
    }

    /**
     * Returns an unmodifiable snapshot of pending task IDs.
     * Exposed for testing.
     */
    public Set<String> pendingTaskIds() {
        return Set.copyOf(pendingCancels.keySet());
    }
}
