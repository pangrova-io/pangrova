package org.pangrova.planner.adaptive;

import java.util.Map;
import java.util.Objects;

import org.pangrova.shuffle.api.commit.ShufflePartitionStats;

/**
 * Statistics collected after a shuffle stage completes.
 *
 * <p>Carries both aggregate stats (actual vs estimated) and optional
 * per-partition stats for partition-aware AQE decisions (coalescing,
 * skew detection).</p>
 *
 * @param stageId           unique identifier for the shuffle stage
 * @param actualDataSize    actual data size in bytes after shuffle
 * @param actualRowCount    actual row count after shuffle
 * @param estimatedDataSize estimated data size in bytes from static planning
 * @param estimatedRowCount estimated row count from static planning
 * @param partitionStats    per-partition statistics keyed by partition id;
 *                          empty map when partition-level data is unavailable
 */
public record ShuffleStageStats(
        String stageId,
        long actualDataSize,
        long actualRowCount,
        long estimatedDataSize,
        long estimatedRowCount,
        Map<Integer, ShufflePartitionStats> partitionStats) {

    public ShuffleStageStats {
        Objects.requireNonNull(stageId, "stageId must not be null");
        partitionStats = partitionStats != null ? Map.copyOf(partitionStats) : Map.of();
    }

    /**
     * Backward-compatible constructor without partition stats.
     * Delegates to the canonical constructor with an empty partition map.
     */
    public ShuffleStageStats(String stageId, long actualDataSize, long actualRowCount,
                             long estimatedDataSize, long estimatedRowCount) {
        this(stageId, actualDataSize, actualRowCount, estimatedDataSize, estimatedRowCount, Map.of());
    }
}
