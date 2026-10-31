package org.pangrova.planner.adaptive;

import java.util.List;
import java.util.Map;

import org.pangrova.shuffle.api.ShufflePartitionStats;

/**
 * Detects skewed partitions for dynamic skew join handling.
 *
 * <p>A partition is skewed when its size exceeds both:
 * <ul>
 *   <li>median partition size × {@link AdaptiveQueryConfig#skewPartitionThresholdRatio()}</li>
 *   <li>{@link AdaptiveQueryConfig#skewPartitionMinBytes()}</li>
 * </ul>
 */
public class SkewJoinHandler {

    /**
     * Partition-aware skew detection using real per-partition stats.
     *
     * <p>Returns {@link SkewResult#NO_SKEW} when fewer than 2 partitions
     * are available (median is meaningless with a single partition).</p>
     */
    public SkewResult detectSkew(ShuffleStageStats stats, AdaptiveQueryConfig config) {
        Map<Integer, ShufflePartitionStats> partitionStats = stats.partitionStats();
        if (partitionStats.size() < 2) {
            return SkewResult.NO_SKEW;
        }
        long[] sizes = sortedPartitionSizes(partitionStats);
        long medianBytes = medianOf(sizes);
        long maxBytes = sizes[sizes.length - 1];
        long[] rowCounts = sortedPartitionRowCounts(partitionStats);
        long medianRows = medianOf(rowCounts);
        List<Integer> skewed = findSkewedPartitions(
                partitionStats, medianBytes, medianRows, config);
        if (skewed.isEmpty()) {
            return SkewResult.NO_SKEW;
        }
        return new SkewResult(skewed, medianBytes, maxBytes);
    }

    private long[] sortedPartitionSizes(Map<Integer, ShufflePartitionStats> partitionStats) {
        return partitionStats.values().stream()
                .mapToLong(ShufflePartitionStats::dataBytes)
                .sorted()
                .toArray();
    }

    private long[] sortedPartitionRowCounts(Map<Integer, ShufflePartitionStats> partitionStats) {
        return partitionStats.values().stream()
                .mapToLong(ShufflePartitionStats::rowCount)
                .sorted()
                .toArray();
    }

    private List<Integer> findSkewedPartitions(Map<Integer, ShufflePartitionStats> partitionStats,
                                               long medianBytes, long medianRows,
                                               AdaptiveQueryConfig config) {
        double threshold = config.skewPartitionThresholdRatio();
        long minSkewBytes = config.skewPartitionMinBytes();
        return partitionStats.entrySet().stream()
                .filter(e -> isPartitionSkewed(e.getValue(), medianBytes, medianRows,
                        threshold, minSkewBytes))
                .map(Map.Entry::getKey)
                .toList();
    }

    private boolean isPartitionSkewed(ShufflePartitionStats stats, long medianBytes,
                                      long medianRows, double threshold, long minSkewBytes) {
        if (stats.dataBytes() <= minSkewBytes) {
            return false;
        }
        boolean bytesSkewed = stats.dataBytes() > medianBytes * threshold;
        boolean rowCountSkewed = medianRows > 0
                && stats.rowCount() > medianRows * threshold;
        return bytesSkewed || rowCountSkewed;
    }

    /**
     * Computes the median of a pre-sorted array.
     * For even-length arrays, returns the average of the two middle values.
     */
    private static long medianOf(long[] sorted) {
        int n = sorted.length;
        if (n % 2 == 1) {
            return sorted[n / 2];
        }
        return (sorted[n / 2 - 1] + sorted[n / 2]) / 2;
    }

}
