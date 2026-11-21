package org.pangrova.scheduler.api.control;

import org.pangrova.scheduler.api.enums.ControlLoopAction;

import java.util.List;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Evaluates one cluster observation and returns ordered scheduling actions.
 * Implementations must be thread-safe and own any state kept between evaluations.
 * The caller is responsible for providing snapshots and executing actions.
 */
@Experimental(reason = "G-SCHED-08: unified coordinator control loop for reactive cluster management")
public interface SchedulerControlLoop {

    /**
     * Evaluates the current cluster snapshot and returns zero or more actions
     * for the scheduler executor to carry out.
     *
     * <p>An empty list means no action is needed — the cluster is in a
     * satisfactory state. Multiple actions may be returned when several
     * conditions are detected simultaneously (e.g., rebalance + scale-up).</p>
     *
     * @param snapshot point-in-time cluster state assembled from heartbeats
     * @return an ordered list of actions to execute; empty if no action needed
     */
    List<ControlLoopAction> evaluate(ClusterSnapshot snapshot);

    /**
     * Returns the human-readable name of this control loop implementation
     * (e.g., "default", "cost-aware", "latency-optimised").
     */
    String name();
}
