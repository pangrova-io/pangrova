package org.pangrova.planner.adaptivequery;

import java.util.Objects;

import org.pangrova.planner.api.adaptivequery.SkewResult;
import org.pangrova.shuffle.api.SkewDetector;

/** Adapts configured shuffle skew detection to planner decisions. */
public final class SkewJoinHandler {
    private final SkewDetector detector;

    /** Borrows a stateless, thread-safe detector selected by process assembly. */
    public SkewJoinHandler(SkewDetector detector) {
        this.detector = Objects.requireNonNull(detector, "detector");
    }

    /**
     * Reports hot partition identities from the actual stage statistics.
     * Split recommendations are advisory; range splitting requires a separate
     * execution plan and backend capability and is not implied by this result.
     */
    public SkewResult detectSkew(ShuffleStageStats stats, AdaptiveQueryConfig config) {
        var result = detector.detect(stats.partitionStats(), new SkewDetector.SkewDetectionConfig(
                config.skewPartitionThresholdRatio(), config.skewPartitionMinBytes(),
                config.targetPartitionBytes()));
        if (!result.hasSkew()) {
            return SkewResult.NO_SKEW;
        }
        return new SkewResult(result.skewedPartitions().stream()
                .map(SkewDetector.SkewedPartition::partitionId).toList(),
                result.medianBytes(), result.maxBytes());
    }
}
