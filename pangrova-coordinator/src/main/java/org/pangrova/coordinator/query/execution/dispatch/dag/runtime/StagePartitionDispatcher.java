package org.pangrova.coordinator.query.execution.dispatch.dag.runtime;

import org.pangrova.coordinator.query.execution.dispatch.DispatchResult;
import org.pangrova.coordinator.query.execution.dispatch.dag.handler.DispatchCanceller;
import org.pangrova.coordinator.scheduler.TaskDispatcher;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.spi.annotation.Internal;

import java.util.concurrent.CompletableFuture;
import java.util.function.UnaryOperator;

/** The single registration/control-binding/HTTP dispatch boundary for ordinary and durable leaf partitions. */
@Internal
public final class StagePartitionDispatcher {
    private final TaskDispatcher dispatcher;
    private final DispatchCanceller canceller;
    private final UnaryOperator<SubmitTaskRequest> accessBinder;

    public StagePartitionDispatcher(TaskDispatcher dispatcher, DispatchCanceller canceller,
            UnaryOperator<SubmitTaskRequest> accessBinder) {
        this.dispatcher = java.util.Objects.requireNonNull(dispatcher, "dispatcher");
        this.canceller = java.util.Objects.requireNonNull(canceller, "canceller");
        this.accessBinder = java.util.Objects.requireNonNull(accessBinder, "accessBinder");
    }

    /** Returns the actual bound request and HTTP acknowledgement; no result or owner is synthesized. */
    public Issued dispatch(SubmitTaskRequest request, WorkerInfo worker) {
        var bound = accessBinder.apply(dispatcher.bindControlIdentity(request));
        requirePreservedAttempt(request, bound);
        long remaining = Math.subtractExact(bound.taskRequest().getDeadlineEpochMs(), System.currentTimeMillis());
        if (remaining <= 0) {
            throw new IllegalStateException("Actual task deadline expired before HTTP dispatch");
        }
        canceller.prepareTaskDispatch(bound, worker.workerId());
        try {
            var response = dispatcher.dispatchAsync(bound, worker)
                    .orTimeout(remaining, java.util.concurrent.TimeUnit.MILLISECONDS);
            var acknowledged = response.thenApply(result -> {
                if (result == null || !bound.taskRequest().getAttempt().getTaskId().equals(result.taskId())) {
                    throw new IllegalStateException("Worker acknowledgement changed the actual issued task identity");
                }
                canceller.registerDispatchedTask(bound.taskRequest().getAttempt().getQueryOrJobId(),
                        result, worker.workerId());
                return result;
            });
            return new Issued(bound, acknowledged);
        } catch (RuntimeException failure) {
            canceller.discardPreparedTask(bound);
            throw failure;
        }
    }

    /** Only actual HTTP-dispatched partitions have this value; restored winners do not. */
    public record Issued(SubmitTaskRequest request, CompletableFuture<DispatchResult> acknowledgement) {}

    private static void requirePreservedAttempt(SubmitTaskRequest request, SubmitTaskRequest bound) {
        var original = request.taskRequest().getAttempt();
        var actual = bound.taskRequest().getAttempt();
        if (!original.toBuilder().clearExecutionOwner().clearControlEpoch().build()
                .equals(actual.toBuilder().clearExecutionOwner().clearControlEpoch().build())
                || original.hasExecutionOwner() && !original.getExecutionOwner().equals(actual.getExecutionOwner())
                || original.getControlEpoch() > 0 && original.getControlEpoch() != actual.getControlEpoch()) {
            throw new IllegalStateException("Control or access binding changed the issued partition identity");
        }
    }
}
