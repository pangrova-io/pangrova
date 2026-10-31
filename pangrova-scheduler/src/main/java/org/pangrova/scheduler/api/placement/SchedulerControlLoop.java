package org.pangrova.scheduler.api.placement;

import java.util.List;

import org.pangrova.common.api.annotation.Experimental;

/**
 * SPI for a unified coordinator scheduling control loop that monitors cluster
 * state and makes reactive scheduling decisions.
 *
 * <p>The control loop follows the <b>observe-decide-act</b> pattern common in
 * distributed systems (Kubernetes controllers, Spark's ExecutorAllocationManager,
 * Trino's ClusterMemoryManager). On each tick the coordinator:</p>
 * <ol>
 *   <li><b>Observe:</b> assembles a {@link ClusterSnapshot} from the latest
 *       worker heartbeats and query queue state.</li>
 *   <li><b>Decide:</b> calls {@link #evaluate(ClusterSnapshot)} which returns
 *       zero or more {@link ControlLoopAction}s.</li>
 *   <li><b>Act:</b> the scheduler executor carries out the emitted actions
 *       (rebalance tasks, request scale-up, kill queries, etc.).</li>
 * </ol>
 *
 * <h3>Design rationale</h3>
 * <p>Without a unified control loop, the coordinator's scheduling decisions
 * are spread across ad-hoc per-query threads with busy-polling loops and
 * disconnected worker-selection logic. This SPI centralises those decisions
 * into a single, testable, pluggable evaluation function that runs on a
 * configurable interval.</p>
 *
 * <h3>Industry context</h3>
 * <ul>
 *   <li><b>Kubernetes:</b> Every controller (Deployment, HPA, Node) runs
 *       a reconciliation loop that observes actual state and drives toward
 *       desired state.</li>
 *   <li><b>Spark:</b> {@code ExecutorAllocationManager} is a periodic
 *       control loop that scales executors based on pending task backlog.</li>
 *   <li><b>Trino:</b> {@code ClusterMemoryManager} periodically checks
 *       cluster memory and kills the largest query under pressure.</li>
 *   <li><b>Snowflake:</b> Internal auto-suspend / auto-resume / auto-scale
 *       are driven by control-loop evaluations against warehouse metrics.</li>
 * </ul>
 *
 * <p>Implementations must be stateless and thread-safe. State between
 * evaluations (e.g., hysteresis counters, cooldown timers) should be
 * encapsulated inside the implementation, not shared externally.</p>
 *
 * @see ClusterSnapshot
 * @see ControlLoopAction
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
