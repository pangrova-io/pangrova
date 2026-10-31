package org.pangrova.shuffle.api;

import java.util.List;
import java.util.Map;

import org.pangrova.common.api.annotation.Experimental;

/**
 * SPI for detecting partition skew in shuffle data and recommending mitigation actions.
 *
 * <p>The coordinator's AQE (Adaptive Query Execution) engine invokes a {@code SkewDetector}
 * after a shuffle stage commits and per-partition statistics are available via
 * {@link org.pangrova.shuffle.api.ShuffleCommitResult}. The detector analyzes the
 * partition size distribution and identifies hot partitions that would cause
 * downstream stragglers.</p>
 *
 * <h3>Two-layer skew mitigation (ADR-002 D2 + Task 124)</h3>
 * <p>This SPI is the <em>sole data source</em> for partition-level skew statistics.
 * The per-partition stats produced by {@link #detect(Map, SkewDetectionConfig)} feed
 * both layers of skew handling:
 * <ol>
 *   <li><b>Shuffle-plane split</b> — the shuffle service uses range-read primitives
 *       ({@code createSource(..., startMapIndex, endMapIndex, attemptNumber, ...)}) to
 *       serve sub-ranges of a skewed partition to multiple reduce tasks in parallel.</li>
 *   <li><b>Planner-level re-optimization</b> — {@code PartitionCoalescer} merges small
 *       partitions, while {@code SkewJoinHandler} splits large partitions into sub-tasks,
 *       both driven by the same {@link ShufflePartitionStats} this detector analyzes.</li>
 * </ol>
 *
 * <h3>Contract</h3>
 * <ul>
 *   <li>Implementations must be stateless and thread-safe.</li>
 *   <li>{@link #detect} must return {@link SkewDetectionResult#NO_SKEW} when
 *       insufficient data is available (fewer than 2 partitions).</li>
 *   <li>The default implementation uses median-based detection matching
 *       Spark AQE's {@code OptimizeSkewedJoin} algorithm.</li>
 * </ul>
 *
 * <p>Industry references: Spark AQE {@code OptimizeSkewedJoin},
 * Trino {@code ScaleWritersPartitioning}, Celeborn skew-partition-split.</p>
 *
 * @see ShufflePartitionStats
 * @see org.pangrova.shuffle.api.ShuffleCommitResult
 */
@Experimental(reason = "ADR-002 D2 + G-SHUF-05: shuffle-plane skew detection for AQE")
public interface SkewDetector {

    /**
     * Analyzes per-partition statistics and identifies skewed partitions.
     *
     * @param partitionStats per-partition statistics keyed by partition ID;
     *                       must contain at least 2 entries for meaningful detection
     * @param config         skew detection thresholds and parameters
     * @return detection result with identified skewed partitions and recommended actions
     */
    SkewDetectionResult detect(Map<Integer, ShufflePartitionStats> partitionStats,
                               SkewDetectionConfig config);

    /**
     * Configuration for skew detection thresholds.
     *
     * @param skewThresholdRatio  a partition is skewed when its size exceeds
     *                            median * skewThresholdRatio (must be > 1.0)
     * @param minSkewBytes        minimum absolute size for a partition to be considered
     *                            skewed, regardless of the ratio (must be non-negative)
     * @param targetPartitionBytes target partition size for split recommendations
     *                             (must be positive)
     */
    record SkewDetectionConfig(
            double skewThresholdRatio,
            long minSkewBytes,
            long targetPartitionBytes) {

        /** Default config: 5x median, 256 MB min, 128 MB target (Spark AQE defaults). */
        public static final SkewDetectionConfig DEFAULT =
                new SkewDetectionConfig(5.0, 256L * 1024 * 1024, 128L * 1024 * 1024);

        public SkewDetectionConfig {
            if (skewThresholdRatio <= 1.0) {
                throw new IllegalArgumentException(
                        "skewThresholdRatio must be > 1.0, got: " + skewThresholdRatio);
            }
            if (minSkewBytes < 0) {
                throw new IllegalArgumentException(
                        "minSkewBytes must be non-negative, got: " + minSkewBytes);
            }
            if (targetPartitionBytes <= 0) {
                throw new IllegalArgumentException(
                        "targetPartitionBytes must be positive, got: " + targetPartitionBytes);
            }
        }
    }

    /**
     * Result of skew detection, containing identified skewed partitions and
     * recommended split counts for each.
     *
     * @param skewedPartitions partitions identified as skewed, with recommended split counts
     * @param medianBytes      median partition size used as the baseline for detection
     * @param maxBytes         maximum partition size observed
     */
    record SkewDetectionResult(
            List<SkewedPartition> skewedPartitions,
            long medianBytes,
            long maxBytes) {

        /** No skew detected — all partitions within normal distribution. */
        public static final SkewDetectionResult NO_SKEW =
                new SkewDetectionResult(List.of(), 0, 0);

        public SkewDetectionResult {
            skewedPartitions = skewedPartitions != null
                    ? List.copyOf(skewedPartitions) : List.of();
        }

        /** Returns true if at least one partition is skewed. */
        public boolean hasSkew() {
            return !skewedPartitions.isEmpty();
        }
    }

    /**
     * A single skewed partition with its statistics and recommended split count.
     *
     * @param partitionId        the skewed partition identifier
     * @param dataBytes          actual size of the partition in bytes
     * @param rowCount           actual row count in the partition
     * @param recommendedSplits  recommended number of sub-ranges to split this partition
     *                           into for parallel processing (always >= 2 for skewed partitions)
     */
    record SkewedPartition(int partitionId, long dataBytes, long rowCount,
                           int recommendedSplits) {

        public SkewedPartition {
            if (dataBytes < 0) {
                throw new IllegalArgumentException(
                        "dataBytes must be non-negative, got: " + dataBytes);
            }
            if (rowCount < 0) {
                throw new IllegalArgumentException(
                        "rowCount must be non-negative, got: " + rowCount);
            }
            if (recommendedSplits < 1) {
                throw new IllegalArgumentException(
                        "recommendedSplits must be >= 1, got: " + recommendedSplits);
            }
        }
    }
}
