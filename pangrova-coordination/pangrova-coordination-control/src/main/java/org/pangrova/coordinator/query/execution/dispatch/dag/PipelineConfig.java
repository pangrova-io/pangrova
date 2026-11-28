package org.pangrova.coordinator.query.execution.dispatch.dag;

/**
 * Configuration for unified pipeline execution.
 *
 * @param enabled             whether pipeline scheduling is active (upstream RUNNING → schedule downstream)
 * @param maxConcurrentStages maximum concurrent RUNNING stages (0 = unlimited)
 * @param localShuffle whether the entire query uses Worker-local shuffle
 */
public record PipelineConfig(
        boolean enabled,
        int maxConcurrentStages,
        boolean localShuffle
) {
    /** Pipeline disabled: barrier-only scheduling (current behavior). */
    public static final PipelineConfig DISABLED = new PipelineConfig(false, 0);

    /** Disk segments are consumed only after each producer stage finishes on the selected Worker. */
    public static final PipelineConfig LOCAL = new PipelineConfig(false, 0, true);

    public PipelineConfig(boolean enabled, int maxConcurrentStages) {
        this(enabled, maxConcurrentStages, false);
    }

    /** Pipeline enabled with defaults: unlimited concurrent stages. */
    public static final PipelineConfig ENABLED = new PipelineConfig(true, 0);

    public PipelineConfig {
        if (localShuffle && enabled) throw new IllegalArgumentException("Local shuffle requires blocking stages");
        if (maxConcurrentStages < 0) {
            throw new IllegalArgumentException(
                    "maxConcurrentStages must be non-negative, got: " + maxConcurrentStages);
        }
    }
}
