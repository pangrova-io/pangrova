package org.pangrova.planner.adaptive;

/**
 * Configuration for Adaptive Query Execution (AQE).
 *
 * <p>Controls partition coalescing, skew detection, and broadcast join thresholds.
 * All size fields use bytes; ratio fields are dimensionless multipliers.</p>
 *
 * @param enabled                      whether AQE re-optimization is enabled
 * @param targetPartitionBytes         target coalesced partition size (default 256 MB)
 * @param skewPartitionThresholdRatio  partition is skewed when size exceeds median × this ratio (default 5.0)
 * @param skewPartitionMinBytes        minimum absolute size to consider a partition skewed (default 256 MB)
 * @param minPartitionsForCoalesce     minimum partitions required before coalescing is attempted (default 2)
 * @param broadcastThresholdBytes      maximum build-side size for broadcast hash join switch (default 10 MB)
 */
public record AdaptiveQueryConfig(
        boolean enabled,
        long targetPartitionBytes,
        double skewPartitionThresholdRatio,
        long skewPartitionMinBytes,
        int minPartitionsForCoalesce,
        long broadcastThresholdBytes) {

    private static final long DEFAULT_TARGET_PARTITION_BYTES = 256L * 1024 * 1024;
    private static final double DEFAULT_SKEW_THRESHOLD_RATIO = 5.0;
    private static final long DEFAULT_SKEW_MIN_BYTES = 256L * 1024 * 1024;
    private static final int DEFAULT_MIN_PARTITIONS = 2;
    private static final long DEFAULT_BROADCAST_THRESHOLD_BYTES = 10L * 1024 * 1024;

    /** AQE disabled with default thresholds. */
    public static final AdaptiveQueryConfig DISABLED = new AdaptiveQueryConfig(false);

    /** AQE enabled with default thresholds. */
    public static final AdaptiveQueryConfig ENABLED = new AdaptiveQueryConfig(true);

    public AdaptiveQueryConfig {
        if (targetPartitionBytes <= 0) {
            throw new IllegalArgumentException("targetPartitionBytes must be positive, got: " + targetPartitionBytes);
        }
        if (!Double.isFinite(skewPartitionThresholdRatio) || skewPartitionThresholdRatio <= 1.0) {
            throw new IllegalArgumentException("skewPartitionThresholdRatio must be finite and > 1.0, got: "
                    + skewPartitionThresholdRatio);
        }
        if (skewPartitionMinBytes < 0) {
            throw new IllegalArgumentException("skewPartitionMinBytes must be non-negative, got: "
                    + skewPartitionMinBytes);
        }
        if (minPartitionsForCoalesce < 1) {
            throw new IllegalArgumentException("minPartitionsForCoalesce must be at least 1, got: "
                    + minPartitionsForCoalesce);
        }
        if (broadcastThresholdBytes <= 0) {
            throw new IllegalArgumentException("broadcastThresholdBytes must be positive, got: "
                    + broadcastThresholdBytes);
        }
    }

    /**
     * Backward-compatible constructor: enabled flag only, all thresholds use defaults.
     */
    public AdaptiveQueryConfig(boolean enabled) {
        this(enabled, DEFAULT_TARGET_PARTITION_BYTES, DEFAULT_SKEW_THRESHOLD_RATIO,
                DEFAULT_SKEW_MIN_BYTES, DEFAULT_MIN_PARTITIONS, DEFAULT_BROADCAST_THRESHOLD_BYTES);
    }

    /**
     * Backward-compatible constructor without broadcast threshold.
     */
    public AdaptiveQueryConfig(boolean enabled, long targetPartitionBytes,
                               double skewPartitionThresholdRatio,
                               long skewPartitionMinBytes,
                               int minPartitionsForCoalesce) {
        this(enabled, targetPartitionBytes, skewPartitionThresholdRatio,
                skewPartitionMinBytes, minPartitionsForCoalesce, DEFAULT_BROADCAST_THRESHOLD_BYTES);
    }
}
