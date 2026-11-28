package org.pangrova.coordinator.query.execution.dispatch.dag;

import org.pangrova.common.concurrency.ExecutorDrain;
import org.pangrova.common.concurrency.ManagedExecutorFactory;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.coordinator.query.execution.dispatch.dag.runtime.DAGExecutionState;
import org.pangrova.coordinator.query.task.QueryStageObservations;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.query.profiling.enums.PreparationOutcome;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

/** Dispatches live read preparation without occupying Worker report or HTTP acknowledgement threads. */
final class DownstreamStageScheduler implements AutoCloseable {
    private static final int QUEUED_QUERIES = 256;
    private static final long DRAIN_TIMEOUT_MILLIS = 30_000;

    private final CoordinatorTaskTracker tracker;
    private final ExecutorService executor;

    DownstreamStageScheduler(CoordinatorTaskTracker tracker) {
        this(tracker, ManagedExecutorFactory.newBoundedThreadPool("dag-read-preparation",
                Math.max(2, Math.min(8, Runtime.getRuntime().availableProcessors() / 2)), QUEUED_QUERIES));
    }

    DownstreamStageScheduler(CoordinatorTaskTracker tracker, ExecutorService executor) {
        this.tracker = tracker;
        this.executor = executor;
    }

    void request(DAGExecutionState state, QueryStageObservations observations, Runnable prepareReadyStages) {
        if (!active(state) || state.downstreamWakeups().getAndIncrement() != 0) return;
        // Hold issuance through queueing, planning and actual split-source close.
        var operation = tracker.enterQueryOperation(state.queryId());
        var preparation = observations == null ? null : observations.submitPreparation();
        try {
            executor.execute(() -> ExecutorDrain.runOwned(executor, () -> {
                try (operation) {
                    runPreparation(state, preparation, prepareReadyStages);
                }
            }));
        } catch (RejectedExecutionException failure) {
            if (preparation != null) preparation.finish(PreparationOutcome.REJECTED);
            operation.close();
            throw new PangrovaException(PangrovaErrorCode.RUNTIME_RESOURCE_EXHAUSTED,
                    "Downstream read preparation queue is full", failure);
        }
    }

    private static void runPreparation(DAGExecutionState state, QueryStageObservations.Preparation preparation,
            Runnable prepareReadyStages) {
        if (preparation != null) preparation.start();
        try {
            boolean ran = drain(state, prepareReadyStages);
            if (preparation != null) {
                preparation.finish(ran ? PreparationOutcome.COMPLETED : PreparationOutcome.SKIPPED);
            }
        } catch (RuntimeException | LinkageError failure) {
            if (preparation != null) preparation.finish(PreparationOutcome.FAILED);
            state.cancelled().set(true);
            state.completion().completeExceptionally(failure);
        }
    }

    private static boolean drain(DAGExecutionState state, Runnable prepareReadyStages) {
        boolean ran = false;
        int wakeups = 1;
        do {
            if (active(state)) {
                prepareReadyStages.run();
                ran = true;
            }
            wakeups = state.downstreamWakeups().addAndGet(-wakeups);
        } while (wakeups != 0);
        return ran;
    }

    private static boolean active(DAGExecutionState state) {
        return !state.cancelled().get() && !state.completion().isDone();
    }

    @Override
    public void close() {
        ExecutorDrain.shutdownAndAwait(executor, DRAIN_TIMEOUT_MILLIS);
    }
}
