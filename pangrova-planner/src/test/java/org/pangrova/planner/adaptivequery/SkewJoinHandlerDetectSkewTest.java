package org.pangrova.planner.adaptivequery;

import org.pangrova.planner.api.adaptivequery.SkewResult;

import java.util.Map;

import org.pangrova.shuffle.api.ShufflePartitionStats;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies partition-aware skew detection via {@link SkewJoinHandler#detectSkew}.
 *
 * <p><b>Validates: Requirements REQ-3, REQ-6</b></p>
 */
class SkewJoinHandlerDetectSkewTest {

    private final SkewJoinHandler handler = new SkewJoinHandler(
            new org.pangrova.shuffle.skew.MedianSkewDetector());

    @Test
    void emptyPartitions_returnsNoSkew() {
        ShuffleStageStats stats = new ShuffleStageStats("s1", 0, 0, 0, 0, Map.of());
        SkewResult result = handler.detectSkew(stats, AdaptiveQueryConfig.ENABLED);
        assertThat(result).isEqualTo(SkewResult.NO_SKEW);
        assertThat(result.hasSkew()).isFalse();
    }

    @Test
    void singlePartition_returnsNoSkew() {
        ShuffleStageStats stats = new ShuffleStageStats("s1", 1024, 10, 0, 0,
                Map.of(0, new ShufflePartitionStats(0, 1024, 10)));
        SkewResult result = handler.detectSkew(stats, AdaptiveQueryConfig.ENABLED);
        assertThat(result).isEqualTo(SkewResult.NO_SKEW);
    }

    @Test
    void uniformPartitions_noSkew() {
        long mb = 1024L * 1024;
        Map<Integer, ShufflePartitionStats> partitions = Map.of(
                0, new ShufflePartitionStats(0, 100 * mb, 1000),
                1, new ShufflePartitionStats(1, 100 * mb, 1000),
                2, new ShufflePartitionStats(2, 100 * mb, 1000),
                3, new ShufflePartitionStats(3, 100 * mb, 1000));
        ShuffleStageStats stats = new ShuffleStageStats("s1", 400 * mb, 4000, 0, 0, partitions);

        SkewResult result = handler.detectSkew(stats, AdaptiveQueryConfig.ENABLED);
        assertThat(result.hasSkew()).isFalse();
    }

    @Test
    void skewedPartition_detected() {
        long mb = 1024L * 1024;
        // Median ~ 50MB, partition 0 = 2GB → 2048/50 = 40.96x > 5.0x threshold
        // 2GB > 256MB min → skewed
        Map<Integer, ShufflePartitionStats> partitions = Map.of(
                0, new ShufflePartitionStats(0, 2048 * mb, 100_000),
                1, new ShufflePartitionStats(1, 50 * mb, 500),
                2, new ShufflePartitionStats(2, 50 * mb, 500),
                3, new ShufflePartitionStats(3, 50 * mb, 500),
                4, new ShufflePartitionStats(4, 50 * mb, 500));
        ShuffleStageStats stats = new ShuffleStageStats("s1", 2248 * mb, 102_000, 0, 0, partitions);

        SkewResult result = handler.detectSkew(stats, AdaptiveQueryConfig.ENABLED);

        assertThat(result.hasSkew()).isTrue();
        assertThat(result.skewedPartitions()).containsExactly(0);
        assertThat(result.medianBytes()).isEqualTo(50 * mb);
        assertThat(result.maxBytes()).isEqualTo(2048 * mb);
    }

    @Test
    void partitionExceedsRatioButBelowMinBytes_notSkewed() {
        // Median = 1KB, partition 0 = 10KB → 10x > 5x threshold
        // But 10KB < 256MB min → NOT skewed
        Map<Integer, ShufflePartitionStats> partitions = Map.of(
                0, new ShufflePartitionStats(0, 10_000, 10),
                1, new ShufflePartitionStats(1, 1_000, 1),
                2, new ShufflePartitionStats(2, 1_000, 1),
                3, new ShufflePartitionStats(3, 1_000, 1));
        ShuffleStageStats stats = new ShuffleStageStats("s1", 13_000, 13, 0, 0, partitions);

        SkewResult result = handler.detectSkew(stats, AdaptiveQueryConfig.ENABLED);
        assertThat(result.hasSkew()).isFalse();
    }

    @Test
    void customThresholds_respected() {
        long mb = 1024L * 1024;
        // Custom: ratio=2.0, minBytes=10MB
        AdaptiveQueryConfig config = new AdaptiveQueryConfig(true, 256 * mb, 2.0, 10 * mb, 2);
        // Median = 50MB, partition 0 = 120MB → 120/50 = 2.4x > 2.0x, 120MB > 10MB → skewed
        Map<Integer, ShufflePartitionStats> partitions = Map.of(
                0, new ShufflePartitionStats(0, 120 * mb, 1200),
                1, new ShufflePartitionStats(1, 50 * mb, 500),
                2, new ShufflePartitionStats(2, 50 * mb, 500));
        ShuffleStageStats stats = new ShuffleStageStats("s1", 220 * mb, 2200, 0, 0, partitions);

        SkewResult result = handler.detectSkew(stats, config);
        assertThat(result.hasSkew()).isTrue();
        assertThat(result.skewedPartitions()).containsExactly(0);
    }
}
