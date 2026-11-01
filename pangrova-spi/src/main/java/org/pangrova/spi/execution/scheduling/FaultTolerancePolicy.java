package org.pangrova.spi.execution.scheduling;

import org.pangrova.spi.annotation.Experimental;

/**
 * Declares how the scheduler reacts when a task or stage fails.
 *
 * <p>The policy is consulted by the DAG scheduler's failure handling path.
 * Retry-based policies ({@link #TASK_RETRY}, {@link #STAGE_RETRY}) are only
 * effective when the active {@link org.pangrova.shuffle.api.capability.ShuffleCapabilities}
 * supports it — i.e., {@code shuffleCapabilities.supportsTaskRetry()} returns
 * {@code true} (requires at least {@code LOCAL_SPILL} durability).
 *
 * <h3>Industry context</h3>
 * <ul>
 *   <li><b>Trino/Presto:</b> Trino FTE uses persistent exchange (spooled data)
 *       with task-granularity retry; mirrors {@code TASK_RETRY}.</li>
 *   <li><b>Spark:</b> Stage-level retry via DAGScheduler; lost shuffle output
 *       triggers full upstream stage re-computation; mirrors {@code STAGE_RETRY}.</li>
 *   <li><b>Snowflake/Databricks:</b> Internal retry at both task and stage
 *       granularity, transparent to the user.</li>
 * </ul>
 */
@Experimental(reason = "G-SCHED-07 fault-tolerant execution; evolving alongside FTE scheduler")
public enum FaultTolerancePolicy {

    /**
     * Current default: any task failure immediately fails the entire query.
     * No retry is attempted. Suitable for low-latency interactive workloads
     * where retry latency is unacceptable.
     */
    NONE,

    /**
     * Retry the failed task up to the configured {@link RetryBudget} limit.
     * Requires that the shuffle provider supports task retry
     * ({@link org.pangrova.shuffle.api.capability.ShuffleCapabilities#supportsTaskRetry()}).
     * The scheduler re-reads committed partitions from a surviving shuffle store
     * rather than re-running upstream stages.
     *
     * <p>Analogous to Trino FTE task-level retry.</p>
     */
    TASK_RETRY,

    /**
     * When a task fails and its upstream shuffle output is lost (e.g., the
     * producing worker crashed and data was {@code EPHEMERAL}), recompute the
     * entire upstream stage. The retry budget's stage-level limit governs
     * how many times a stage may be re-executed per query.
     *
     * <p>Analogous to Spark's DAGScheduler stage-level retry.</p>
     */
    STAGE_RETRY
}
