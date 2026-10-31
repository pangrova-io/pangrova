package org.pangrova.planner.adaptive;

import java.util.Map;

import org.pangrova.shuffle.api.ShufflePartitionStats;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link SkewJoinHandler#detectSkew(ShuffleStageStats, AdaptiveQueryConfig)}.
 */
class SkewJoinHandlerTest {

    private static final long MB = 1024L * 1024;
    private final SkewJoinHandler handler = new SkewJoinHandler();

    @Test
    void emptyPartitions_returnsNoSkew() {
        var stats = statsWithPartitions(Map.of());
        assertThat(handler.detectSkew(stats, AdaptiveQueryConfig.ENABLED)).isEqualTo(SkewResult.NO_SKEW);
    }

    @Test
    void singlePartition_returnsNoSkew() {
        var stats = statsWithPartitions(Map.of(0, new ShufflePartitionStats(0, 1000 * MB, 10000)));
        assertThat(handler.detectSkew(stats, AdaptiveQueryConfig.ENABLED)).isEqualTo(SkewResult.NO_SKEW);
    }

    @Test
    void allEqualSizes_noSkew() {
        var partitions = Map.of(
                0, new ShufflePartitionStats(0, 100 * MB, 1000),
                1, new ShufflePartitionStats(1, 100 * MB, 1000),
                2, new ShufflePartitionStats(2, 100 * MB, 1000),
                3, new ShufflePartitionStats(3, 100 * MB, 1000),
                4, new ShufflePartitionStats(4, 100 * MB, 1000));
        var stats = statsWithPartitions(partitions);
        var result = handler.detectSkew(stats, AdaptiveQueryConfig.ENABLED);

        assertThat(result.hasSkew()).isFalse();
    }

    @Test
    void skewedPartition_detected() {
        long skewedSize = 2L * 1024 * MB;
        long normalSize = 50 * MB;
        var partitions = Map.of(
                0, new ShufflePartitionStats(0, skewedSize, 100_000),
                1, new ShufflePartitionStats(1, normalSize, 500),
                2, new ShufflePartitionStats(2, normalSize, 500),
                3, new ShufflePartitionStats(3, normalSize, 500),
                4, new ShufflePartitionStats(4, normalSize, 500));
        var stats = statsWithPartitions(partitions);
        var result = handler.detectSkew(stats, AdaptiveQueryConfig.ENABLED);

        assertThat(result.hasSkew()).isTrue();
        assertThat(result.skewedPartitions()).contains(0);
        assertThat(result.maxBytes()).isEqualTo(skewedSize);
    }

    @Test
    void aboveThresholdRatio_butBelowMinBytes_notSkewed() {
        var partitions = Map.of(
                0, new ShufflePartitionStats(0, 10 * MB, 100),
                1, new ShufflePartitionStats(1, 10 * MB, 100),
                2, new ShufflePartitionStats(2, 60 * MB, 100));
        var stats = statsWithPartitions(partitions);
        var result = handler.detectSkew(stats, AdaptiveQueryConfig.ENABLED);

        assertThat(result.hasSkew()).isFalse();
    }

    @Test
    void aboveMinBytes_butBelowThresholdRatio_notSkewed() {
        var partitions = Map.of(
                0, new ShufflePartitionStats(0, 200 * MB, 1000),
                1, new ShufflePartitionStats(1, 200 * MB, 1000),
                2, new ShufflePartitionStats(2, 300 * MB, 1000));
        var stats = statsWithPartitions(partitions);
        var result = handler.detectSkew(stats, AdaptiveQueryConfig.ENABLED);

        assertThat(result.hasSkew()).isFalse();
    }

    @Test
    void customConfig_lowerThresholds_detectsMoreSkew() {
        var config = new AdaptiveQueryConfig(true, 256 * MB, 2.0, 10 * MB, 2);
        var partitions = Map.of(
                0, new ShufflePartitionStats(0, 50 * MB, 500),
                1, new ShufflePartitionStats(1, 50 * MB, 500),
                2, new ShufflePartitionStats(2, 150 * MB, 1500));
        var stats = statsWithPartitions(partitions);
        var result = handler.detectSkew(stats, config);

        assertThat(result.hasSkew()).isTrue();
        assertThat(result.skewedPartitions()).contains(2);
    }

    private static ShuffleStageStats statsWithPartitions(Map<Integer, ShufflePartitionStats> partitions) {
        long totalBytes = partitions.values().stream().mapToLong(ShufflePartitionStats::dataBytes).sum();
        long totalRows = partitions.values().stream().mapToLong(ShufflePartitionStats::rowCount).sum();
        return new ShuffleStageStats("test-stage", totalBytes, totalRows, totalBytes, totalRows, partitions);
    }
}
