package org.pangrova.scheduler.api.enums;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Actions that the the scheduler control loop can emit in response to
 * observed cluster state.
 *
 * <p>Each action represents a discrete, atomic scheduling decision. The
 * coordinator's scheduler evaluates the cluster snapshot on every control-loop
 * tick and produces zero or more actions, which the scheduler executor then
 * carries out in order.</p>
 *
 * <h3>Industry context</h3>
 * <ul>
 *   <li><b>Kubernetes:</b> The control-loop / reconciliation-loop pattern is
 *       fundamental — controllers observe desired vs. actual state and emit
 *       actions (scale, migrate, evict).</li>
 *   <li><b>Spark:</b> The {@code ExecutorAllocationManager} periodically
 *       evaluates pending tasks vs. available executors and requests scale-up
 *       or scale-down from the cluster manager.</li>
 *   <li><b>Trino:</b> The {@code ClusterMemoryManager} monitors per-node memory
 *       usage and kills the largest query when the cluster is under memory
 *       pressure — analogous to {@link #KILL_QUERY}.</li>
 *   <li><b>Snowflake/Databricks:</b> Auto-scaling and workload rebalancing are
 *       driven by internal control loops that monitor queue depth and resource
 *       utilisation.</li>
 * </ul>
 */
@Experimental(reason = "G-SCHED-08: coordinator control-loop actions for reactive cluster management")
public enum ControlLoopAction {

    /**
     * Redistribute running or queued tasks across workers to balance load.
     *
     * <p>Triggered when the control loop detects significant imbalance in
     * CPU or memory utilisation across workers. The scheduler moves pending
     * tasks from overloaded workers to underloaded ones, or reschedules
     * stages that haven't started yet.</p>
     */
    REBALANCE,

    /**
     * Request additional worker capacity from the underlying cluster manager.
     *
     * <p>Triggered when all workers exceed a utilisation threshold and the
     * query queue is growing. The coordinator signals the external resource
     * manager (YARN, Kubernetes, or a cloud auto-scaler) to provision new
     * worker instances.</p>
     */
    SCALE_UP,

    /**
     * Release excess worker capacity back to the cluster manager.
     *
     * <p>Triggered when worker utilisation drops below a low-watermark for
     * a sustained period. The coordinator gracefully drains idle workers and
     * signals the resource manager to reclaim them, reducing cost.</p>
     */
    SCALE_DOWN,

    /**
     * Migrate a running task from one worker to another.
     *
     * <p>Triggered when a specific worker is about to be decommissioned,
     * shows signs of degraded performance (e.g., disk errors, network
     * congestion), or when data-locality analysis indicates a better
     * placement exists for an in-progress task.</p>
     */
    MIGRATE,

    /**
     * Terminate a running query to relieve cluster-wide resource pressure.
     *
     * <p>Triggered as a last-resort action when the cluster is critically
     * low on memory or other resources and no rebalancing or scaling can
     * resolve the pressure in time. The control loop selects the query to
     * kill based on configurable policy (largest memory consumer, longest
     * running, lowest priority, etc.).</p>
     */
    KILL_QUERY
}
