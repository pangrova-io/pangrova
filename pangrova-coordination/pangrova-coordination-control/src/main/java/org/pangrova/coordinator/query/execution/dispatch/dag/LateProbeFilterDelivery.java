package org.pangrova.coordinator.query.execution.dispatch.dag;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.coordinator.query.execution.dispatch.dag.runtime.BuildFilterGate;
import org.pangrova.coordinator.query.execution.dispatch.dag.runtime.DAGExecutionState;
import org.pangrova.coordinator.scheduler.TaskDispatcher;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Delivers complete late predicates over the sole exact Worker control path. */
final class LateProbeFilterDelivery implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(LateProbeFilterDelivery.class);
    private static final int MAX_IN_FLIGHT = 64;
    private static final int MAX_QUEUED_COHORTS = 128;

    private final TaskDispatcher dispatcher;
    private final CoordinatorTaskTracker tasks;
    private final Semaphore inFlight = new Semaphore(MAX_IN_FLIGHT);
    private final ExecutorService cohorts = ManagedExecutorFactory.newBoundedThreadPool(
            "dag-late-filter", 2, MAX_QUEUED_COHORTS);

    LateProbeFilterDelivery(TaskDispatcher dispatcher, CoordinatorTaskTracker tasks) {
        this.dispatcher = dispatcher;
        this.tasks = tasks;
    }

    void buildPublished(DAGExecutionState state, BuildFilterGate gate) {
        if (!active(state) || !gate.lateDeliveryReady()) return;
        try {
            cohorts.execute(() -> tasks.visitRunningTasks(state.queryId(), taskId -> offer(state, gate, taskId)));
        } catch (RejectedExecutionException overloaded) {
            LOG.debug("event=late_runtime_filter_cohort_skipped queryId={} stageId={}",
                    state.queryId(), gate.plan().probeStageId());
        }
    }

    void offer(DAGExecutionState state, BuildFilterGate gate, String taskId) {
        if (!active(state) || !gate.lateDeliveryReady()) return;
        var issued = tasks.runningReadTarget(state.queryId(), taskId);
        if (issued == null) return;
        var attempt = issued.request().taskRequest().getAttempt();
        if (!gate.isProbe(attempt.getStageId())) return;
        if (!inFlight.tryAcquire()) {
            LOG.debug("event=late_runtime_filter_delivery_skipped taskId={} reason=capacity", taskId);
            return;
        }
        boolean handedOff = false;
        try {
            var update = gate.claimLateBinding(attempt);
            if (update.isEmpty() || !active(state)) return;
            dispatcher.injectDynamicFilter(attempt, update.orElseThrow(), issued.worker())
                    .whenComplete((accepted, failure) -> {
                        inFlight.release();
                        LOG.debug("event=late_runtime_filter_delivery taskId={} stageId={} accepted={} failure={}",
                                taskId, attempt.getStageId(), accepted, failure == null ? null : failure.toString());
                    });
            handedOff = true;
        } catch (IllegalStateException | IllegalArgumentException failed) {
            LOG.debug("event=late_runtime_filter_delivery_failed taskId={}", taskId, failed);
        } finally {
            if (!handedOff) inFlight.release();
        }
    }

    private static boolean active(DAGExecutionState state) {
        return !state.cancelled().get() && !state.completion().isDone();
    }

    @Override
    public void close() {
        cohorts.shutdownNow();
    }
}
