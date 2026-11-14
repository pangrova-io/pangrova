package org.pangrova.scheduler.api.control;

import org.pangrova.scheduler.api.enums.ControlLoopAction;

import java.util.Objects;

import java.time.Instant;
import java.util.List;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Immutable point-in-time snapshot of the entire cluster state, provided to
 * the {@link org.pangrova.scheduler.api.control.SchedulerControlLoop} for evaluation.
 *
 * <p>The coordinator assembles a {@code ClusterSnapshot} on every control-loop
 * tick by aggregating the latest heartbeat data from all registered workers.
 * The snapshot is intentionally a read-only value object — it cannot be mutated
 * by the control loop, which must instead emit {@link ControlLoopAction}s for
 * the scheduler executor to carry out.</p>
 *
 * @param workers        immutable list of per-worker snapshots, one per registered worker
 * @param activeQueries  number of queries currently executing across the cluster
 * @param queuedQueries  number of queries waiting in the admission queue
 * @param timestamp      instant when this snapshot was captured
 */
@Experimental(reason = "G-SCHED-08: cluster snapshot for coordinator control-loop evaluation")
public record ClusterSnapshot(
        List<SchedulingWorkerSnapshot> workers,
        int activeQueries,
        int queuedQueries,
        Instant timestamp) {

    public ClusterSnapshot {
        Objects.requireNonNull(workers, "workers");
        Objects.requireNonNull(timestamp, "timestamp");
        workers = List.copyOf(workers);
        if (activeQueries < 0) {
            throw new IllegalArgumentException(
                    "activeQueries must be non-negative, got: " + activeQueries);
        }
        if (queuedQueries < 0) {
            throw new IllegalArgumentException(
                    "queuedQueries must be non-negative, got: " + queuedQueries);
        }
    }
}
