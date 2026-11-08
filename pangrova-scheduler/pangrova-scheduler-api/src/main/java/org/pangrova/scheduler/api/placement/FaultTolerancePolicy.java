package org.pangrova.scheduler.api.placement;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Declares how the scheduler reacts when a task or stage fails.
 *
 * <p>Task retry against unchanged input requires an explicit effective CONSUMER_RETRY guarantee.
 * Storage medium alone is insufficient. These policy values are used by the FTE policy helper;
 * production DAG retry currently has its own Stage owner and is not configured by this enum.
 */
@Experimental(reason = "G-SCHED-07 fault-tolerant execution; evolving alongside FTE scheduler")
public enum FaultTolerancePolicy {

    /**
     * Any task failure immediately fails the query under this policy.
     * No retry is attempted. Suitable for low-latency interactive workloads
     * where retry latency is unacceptable.
     */
    NONE,

    /**
     * Retry the failed task up to the configured {@link RetryBudget} limit.
     * Requires that the shuffle provider supports task retry
     * ({@link org.pangrova.shuffle.api.capability.ShuffleCapabilityDescriptor
 * #supports(org.pangrova.shuffle.api.capability.ShuffleCapability)}).
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
