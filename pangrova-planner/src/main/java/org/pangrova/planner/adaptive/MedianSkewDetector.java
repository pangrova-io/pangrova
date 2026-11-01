package org.pangrova.planner.adaptive;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.pangrova.shuffle.api.commit.ShufflePartitionStats;
import org.pangrova.shuffle.api.adaptive.SkewDetector;

/**
 * Default {@link SkewDetector} implementation using median-based detection.
 *
 * <p>Mirrors Spark AQE's {@code OptimizeSkewedJoin} algorithm: a partition is
 * skewed when its byte size exceeds both {@code median * skewThresholdRatio}
 * and an absolute minimum ({@code minSkewBytes}). This avoids false positives
 * on uniformly large or uniformly small shuffles.</p>
 *
 * <p>For each skewed partition, the detector recommends splitting into
 * {@code ceil(partitionBytes / targetPartitionBytes)} sub-ranges, capped at 64
 * to prevent excessive parallelism from a single hot partition.</p>
 *
 * <p>Stateless and thread-safe.</p>
 *
 * @see SkewDetector
 * @see SkewJoinHandler
 */
public final class MedianSkewDetector implements SkewDetector {

    private static final int MAX_SPLITS = 64;

    @Override
    public SkewDetectionResult detect(Map<Integer, ShufflePartitionStats> partitionStats,
                                      SkewDetectionConfig config) {
        if (partitionStats == null || partitionStats.size() < 2) {
            return SkewDetectionResult.NO_SKEW;
        }

        long[] sizes = partitionStats.values().stream()
                .mapToLong(ShufflePartitionStats::dataBytes)
                .sorted()
                .toArray();

        long[] rowCounts = partitionStats.values().stream()
                .mapToLong(ShufflePartitionStats::rowCount)
                .sorted()
                .toArray();

        long medianBytes = medianOf(sizes);
        long medianRows = medianOf(rowCounts);
        long maxBytes = sizes[sizes.length - 1];
        long bytesSkewThreshold = (long) (medianBytes * config.skewThresholdRatio());
        long rowsSkewThreshold = medianRows > 0
                ? (long) (medianRows * config.skewThresholdRatio()) : Long.MAX_VALUE;

        List<SkewedPartition> skewed = new ArrayList<>();
        for (Map.Entry<Integer, ShufflePartitionStats> entry : partitionStats.entrySet()) {
            ShufflePartitionStats stats = entry.getValue();
            if (stats.dataBytes() <= config.minSkewBytes()) {
                continue;
            }
            boolean byteSkewed = stats.dataBytes() > bytesSkewThreshold;
            boolean rowSkewed = medianRows > 0
                    && stats.rowCount() > rowsSkewThreshold;
            if (byteSkewed || rowSkewed) {
                int splits = computeSplitCount(stats.dataBytes(), config.targetPartitionBytes());
                if (rowSkewed && !byteSkewed) {
                    splits = Math.max(splits,
                            computeSplitCount(stats.rowCount(), config.targetPartitionBytes() / 128));
                }
                skewed.add(new SkewedPartition(
                        entry.getKey(), stats.dataBytes(), stats.rowCount(), splits));
            }
        }

        if (skewed.isEmpty()) {
            return SkewDetectionResult.NO_SKEW;
        }
        return new SkewDetectionResult(skewed, medianBytes, maxBytes);
    }

    private static int computeSplitCount(long partitionBytes, long targetBytes) {
        if (targetBytes <= 0 || partitionBytes <= targetBytes) {
            return 1;
        }
        int splits = (int) Math.ceil((double) partitionBytes / targetBytes);
        return Math.min(splits, MAX_SPLITS);
    }

    private static long medianOf(long[] sorted) {
        int n = sorted.length;
        if (n % 2 == 1) {
            return sorted[n / 2];
        }
        return (sorted[n / 2 - 1] + sorted[n / 2]) / 2;
    }
}
