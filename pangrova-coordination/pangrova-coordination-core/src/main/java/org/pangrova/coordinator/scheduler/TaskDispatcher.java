package org.pangrova.coordinator.scheduler;

import java.util.concurrent.CompletableFuture;

import org.pangrova.query.core.task.DispatchResult;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;

/**
 * Dispatches a task to a target worker.
 *
 * <p>Implementation: {@link RemoteTaskDispatcher} — sends a real HTTP POST
 * to the Worker's {@code /v1/task} endpoint via JDK HttpClient.</p>
 */
public interface TaskDispatcher {

    /** Binds the selected physical Worker before task intent or frozen COPY authority is persisted. */
    default SubmitTaskRequest bindWorkerIdentity(SubmitTaskRequest request, WorkerInfo worker) {
        var expected = worker.workerExecutionIdentity().toProto();
        var attempt = request.taskRequest().getAttempt();
        if (attempt.hasWorkerExecutionIdentity()) {
            if (!attempt.getWorkerExecutionIdentity().equals(expected)) {
                throw new IllegalStateException("Task target differs from the registered Worker incarnation");
            }
            return request;
        }
        return request.withBoundTask(request.taskRequest().toBuilder()
                .setAttempt(attempt.toBuilder().setWorkerExecutionIdentity(expected)).build());
    }

    /**
     * Binds coordinator-owned control identity before a request is registered
     * and sent. Implementations that do not add transport identity return the
     * immutable request unchanged.
     */
    default SubmitTaskRequest bindControlIdentity(SubmitTaskRequest request) {
        return request;
    }

    /** Binds private frozen inputs after control/access/output binding, before admission seals are registered. */
    default SubmitTaskRequest bindSourceInput(SubmitTaskRequest request) {
        return request;
    }

    DispatchResult dispatch(SubmitTaskRequest request, WorkerInfo worker);

    /**
     * Asynchronously dispatches a task to a target worker.
     * Default implementation wraps the synchronous dispatch in a CompletableFuture.
     */
    default CompletableFuture<DispatchResult> dispatchAsync(SubmitTaskRequest request, WorkerInfo worker) {
        return CompletableFuture.supplyAsync(() -> dispatch(request, worker));
    }

    /**
     * Injects a dynamic filter into a running task on the specified worker.
     * Default implementation is a no-op for dispatchers that do not support dynamic filters.
     *
     * @param taskId      the probe-side task identifier
     * @param planNodeId  the TableScan plan node to receive the filter
     * @param filterProto serialized filter expression (protobuf bytes)
     * @param workerId    the worker hosting the task
     */
    default void injectDynamicFilter(String taskId, String planNodeId,
                                     byte[] filterProto, String workerId) {
        // No-op — concrete dispatchers override when dynamic filter support is available.
    }

    /**
     * Cancels a running task on the specified worker.
     * Default implementation returns a FAILED result.
     *
     * @param taskId     the task to cancel
     * @param workerHost the host of the worker running the task
     * @param httpPort   the HTTP port of the worker
     * @return structured cancel result with status, error detail, and retryability
     */
    default SchedulerCancelResult cancelTask(SubmitTaskRequest request, String workerHost, int httpPort) {
        return SchedulerCancelResult.failed("Cancel not supported by this dispatcher", false);
    }
    /**
     * Notifies a downstream Worker that an upstream stage has committed its output.
     * Enables BLOCKING exchange sources to begin delivering data.
     *
     * @param workerHost       the downstream Worker host
     * @param httpPort         the downstream Worker HTTP port
     * @param queryId          the query identifier
     * @param upstreamStageId  the upstream stage that committed
     */
    default void notifyUpstreamCommit(String workerHost, int httpPort,
                                      String queryId, int upstreamStageId) {
        // No-op — concrete dispatchers override when commit notification is supported.
    }

    /**
     * Notifies a downstream Worker that an upstream stage has committed its output,
     * with retry and a failure callback invoked when all retries are exhausted.
     *
     * @param workerHost           the downstream Worker host
     * @param httpPort             the downstream Worker HTTP port
     * @param queryId              the query identifier
     * @param upstreamStageId      the upstream stage that committed
     * @param stageFailureCallback invoked when all retry attempts fail; may be null
     */
    default void notifyUpstreamCommit(String workerHost, int httpPort,
                                      String queryId, int upstreamStageId,
                                      Runnable stageFailureCallback) {
        notifyUpstreamCommit(workerHost, httpPort, queryId, upstreamStageId);
    }

}
