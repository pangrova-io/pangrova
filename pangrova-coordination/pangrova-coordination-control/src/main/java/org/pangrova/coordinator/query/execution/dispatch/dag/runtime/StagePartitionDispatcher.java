package org.pangrova.coordinator.query.execution.dispatch.dag.runtime;

import org.pangrova.execution.task.DispatchResult;
import org.pangrova.coordinator.query.execution.dispatch.dag.handler.DispatchCanceller;
import org.pangrova.coordinator.scheduler.TaskDispatcher;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.persistence.query.api.state.QueryStateJournal;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.common.api.annotation.Internal;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.UnaryOperator;

/** The single registration/control-binding/HTTP dispatch boundary for ordinary and durable leaf partitions. */
@Internal
public final class StagePartitionDispatcher {
    private final TaskDispatcher dispatcher;
    private final DispatchCanceller canceller;
    private final UnaryOperator<SubmitTaskRequest> accessBinder;
    private final QueryStateJournal journal;

    public StagePartitionDispatcher(TaskDispatcher dispatcher, DispatchCanceller canceller,
            UnaryOperator<SubmitTaskRequest> accessBinder) {
        this(dispatcher, canceller, accessBinder, null);
    }

    /** Shares the task journal with the DAG owner and its Worker reports. */
    public StagePartitionDispatcher(TaskDispatcher dispatcher, DispatchCanceller canceller,
            UnaryOperator<SubmitTaskRequest> accessBinder, QueryStateJournal journal) {
        this.dispatcher = java.util.Objects.requireNonNull(dispatcher, "dispatcher");
        this.canceller = java.util.Objects.requireNonNull(canceller, "canceller");
        this.accessBinder = java.util.Objects.requireNonNull(accessBinder, "accessBinder");
        this.journal = journal;
    }

    /** Returns the actual bound request and HTTP acknowledgement; no result or owner is synthesized. */
    public Issued dispatch(SubmitTaskRequest request, WorkerInfo worker) {
        return dispatchCohort(List.of(new Candidate(request, worker))).get(0);
    }

    /** Freezes and durably records a bounded cohort before any Worker receives a request. */
    public List<Issued> dispatchCohort(List<Candidate> candidates) {
        if (candidates.size() > QueryStateJournal.MAX_TASK_ISSUANCE_BATCH) {
            throw new IllegalArgumentException("Task issuance cohort exceeds its bound");
        }
        List<Candidate> bound = new ArrayList<>(candidates.size());
        try {
            for (var candidate : candidates) bound.add(prepare(candidate));
            if (journal != null && !bound.isEmpty()) {
                journal.appendTaskIssuancesSync(bound.stream()
                        .map(candidate -> canceller.issuanceFact(candidate.request(), candidate.worker())).toList());
            }
        } catch (RuntimeException | Error failure) {
            for (var candidate : bound) {
                try {
                    canceller.discardPreparedTask(candidate.request());
                } catch (RuntimeException cleanup) {
                    failure.addSuppressed(cleanup);
                }
            }
            throw failure;
        }
        return bound.stream().map(this::send).toList();
    }

    public record Candidate(SubmitTaskRequest request, WorkerInfo worker) { }

    private Candidate prepare(Candidate candidate) {
        var request = candidate.request();
        var worker = candidate.worker();
        var targetBound = dispatcher.bindWorkerIdentity(request, worker);
        var bound = dispatcher.bindSourceInput(accessBinder.apply(dispatcher.bindControlIdentity(targetBound)));
        requirePreservedAttempt(request, bound);
        remainingMillis(bound);
        canceller.prepareTaskDispatch(bound, worker);
        return new Candidate(bound, worker);
    }

    private Issued send(Candidate candidate) {
        var bound = candidate.request();
        var worker = candidate.worker();
        try {
            long remaining = remainingMillis(bound);
            canceller.enterTaskDispatch(bound);
            var response = dispatcher.dispatchBoundAsync(bound, worker)
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
            // dispatchAsync may already have sent bytes before throwing. Keep
            // the exact issued identity and surface the actual failure through
            // the same acknowledgement lifecycle; cancellation must seal it.
            return new Issued(bound, CompletableFuture.failedFuture(failure));
        }
    }

    private static long remainingMillis(SubmitTaskRequest request) {
        long remaining = Math.subtractExact(request.taskRequest().getDeadlineEpochMs(), System.currentTimeMillis());
        if (remaining <= 0) {
            throw new IllegalStateException("Actual task deadline expired before HTTP dispatch");
        }
        return remaining;
    }

    /** Only actual HTTP-dispatched partitions have this value; restored winners do not. */
    public record Issued(SubmitTaskRequest request, CompletableFuture<DispatchResult> acknowledgement) {}

    private static void requirePreservedAttempt(SubmitTaskRequest request, SubmitTaskRequest bound) {
        var original = request.taskRequest().getAttempt();
        var actual = bound.taskRequest().getAttempt();
        if (!original.toBuilder().clearExecutionOwner().clearControlEpoch().clearWorkerExecutionIdentity().build()
                .equals(actual.toBuilder().clearExecutionOwner().clearControlEpoch()
                        .clearWorkerExecutionIdentity().build())
                || original.hasWorkerExecutionIdentity()
                    && !original.getWorkerExecutionIdentity().equals(actual.getWorkerExecutionIdentity())
                || original.hasExecutionOwner() && !original.getExecutionOwner().equals(actual.getExecutionOwner())
                || original.getControlEpoch() > 0 && original.getControlEpoch() != actual.getControlEpoch()) {
            throw new IllegalStateException("Control or access binding changed the issued partition identity");
        }
    }
}
