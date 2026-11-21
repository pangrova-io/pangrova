package org.pangrova.coordinator.query.execution.dispatch.dag;

/**
 * Configuration for unified pipeline execution.
 *
 * @param enabled             whether pipeline scheduling is active (upstream RUNNING → schedule downstream)
 * @param maxConcurrentStages maximum concurrent RUNNING stages (0 = unlimited)
 * @param commitNotificationTimeoutMs timeout for upstream commit notifications to Workers
 */
public record PipelineConfig(
        boolean enabled,
        int maxConcurrentStages,
        long commitNotificationTimeoutMs,
        boolean localShuffle
) {
    /** Pipeline disabled: barrier-only scheduling (current behavior). */
    public static final PipelineConfig DISABLED = new PipelineConfig(false, 0, 5000);

    /** Disk segments are consumed only after each producer stage finishes on the selected Worker. */
    public static final PipelineConfig LOCAL = new PipelineConfig(false, 0, 5000, true);

    public PipelineConfig(boolean enabled, int maxConcurrentStages, long commitNotificationTimeoutMs) {
        this(enabled, maxConcurrentStages, commitNotificationTimeoutMs, false);
    }

    /** Pipeline enabled with defaults: unlimited concurrent stages, 5s commit timeout. */
    public static final PipelineConfig ENABLED = new PipelineConfig(true, 0, 5000);

    public PipelineConfig {
        if (localShuffle && enabled) throw new IllegalArgumentException("Local shuffle requires blocking stages");
        if (maxConcurrentStages < 0) {
            throw new IllegalArgumentException(
                    "maxConcurrentStages must be non-negative, got: " + maxConcurrentStages);
        }
        if (commitNotificationTimeoutMs <= 0) {
            throw new IllegalArgumentException(
                    "commitNotificationTimeoutMs must be positive, got: " + commitNotificationTimeoutMs);
        }
    }
}
