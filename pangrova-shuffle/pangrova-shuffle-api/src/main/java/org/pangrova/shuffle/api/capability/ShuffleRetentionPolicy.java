package org.pangrova.shuffle.api.capability;

import org.pangrova.spi.annotation.Experimental;

/**
 * Retention policy for committed shuffle data, orthogonal to {@link ShuffleDurability}.
 *
 * <p>While {@code ShuffleDurability} describes <em>where</em> data lives (memory, local disk, external),
 * {@code ShuffleRetentionPolicy} describes <em>how long</em> data outlives its producing worker process.
 * This distinction matters for safe scale-down, preemption, and spill-based elasticity:
 *
 * <ul>
 *   <li>{@link #EPHEMERAL} — data is tied to the worker process lifetime. Scaling down or
 *       preempting the worker loses all shuffle data, forcing upstream re-execution.</li>
 *   <li>{@link #DETACHED} — data survives worker restart or graceful decommission (e.g.,
 *       local-spill to NVMe that persists across process restarts, or data migrated to a peer
 *       before shutdown). Safe for scale-down if drain completes.</li>
 *   <li>{@link #PERSISTENT} — data is in an external durable store (object store, RSS) that is
 *       fully independent of any worker. Safe for scale-down and preemption without migration.</li>
 * </ul>
 *
 * <p>The scheduler consults retention policy via {@link ShuffleCapabilities#retentionPolicy()}
 * before decommissioning a worker. For {@code EPHEMERAL} workers with live shuffle data, the
 * scheduler must either migrate data, mark affected stages for re-execution, or block scale-down.
 *
 * @see ShuffleDurability
 * @see ShuffleCapabilities#retentionPolicy()
 */
@Experimental(reason = "G-RES-08: persistent/detached shuffle for safe scale-down and preemption")
public enum ShuffleRetentionPolicy {

    /**
     * Shuffle data is tied to the worker process. Lost on shutdown, crash, or preemption.
     * Scale-down of a worker with unreferenced EPHEMERAL shuffle data requires re-execution
     * of upstream stages.
     */
    EPHEMERAL,

    /**
     * Shuffle data survives worker restart or graceful decommission. The data may reside on
     * local persistent storage (NVMe) accessible after process restart, or may be migrated
     * to a peer worker during a drain window before shutdown.
     *
     * <p>Safe for planned scale-down when a drain period is allowed. Not safe for abrupt
     * preemption unless the storage is independently accessible (e.g., shared filesystem).</p>
     */
    DETACHED,

    /**
     * Shuffle data resides in an external durable store (S3, OSS, HDFS, Celeborn) that is
     * fully independent of any worker process. Safe for immediate scale-down and preemption
     * without any migration or drain.
     */
    PERSISTENT
}
