package org.pangrova.planner.adaptive;

import java.util.Map;

import org.pangrova.shuffle.ShufflePartitionStats;
import org.pangrova.shuffle.SkewDetector;
import org.pangrova.shuffle.SkewDetector.SkewDetectionConfig;
import org.pangrova.shuffle.SkewDetector.SkewDetectionResult;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link MedianSkewDetector}, the default {@link SkewDetector} implementation.
 */
class MedianSkewDetectorTest {

    private final MedianSkewDetector detector = new MedianSkewDetector();

    // Use small thresholds for testing: 2x median, 100 bytes min, 50 bytes target
    private final SkewDetectionConfig config = new SkewDetectionConfig(2.0, 100, 50);

    @Test
    void nullPartitionStatsReturnsNoSkew() {
        SkewDetectionResult result = detector.detect(null, config);
        assertThat(result).isEqualTo(SkewDetectionResult.NO_SKEW);
    }

    @Test
    void singlePartitionReturnsNoSkew() {
        Map<Integer, ShufflePartitionStats> stats = Map.of(
                0, new ShufflePartitionStats(0, 1000, 100));
        SkewDetectionResult result = detector.detect(stats, config);
        assertThat(result).isEqualTo(SkewDetectionResult.NO_SKEW);
    }

    @Test
    void uniformPartitionsReturnsNoSkew() {
        Map<Integer, ShufflePartitionStats> stats = Map.of(
                0, new ShufflePartitionStats(0, 200, 20),
                1, new ShufflePartitionStats(1, 200, 20),
                2, new ShufflePartitionStats(2, 200, 20));
        SkewDetectionResult result = detector.detect(stats, config);
        assertThat(result.hasSkew()).isFalse();
    }

    @Test
    void detectsSkewedPartition() {
        // Partition 2 is 10x the median (200) = 2000 > 2.0 * 200 = 400, and > 100 min
        Map<Integer, ShufflePartitionStats> stats = Map.of(
                0, new ShufflePartitionStats(0, 200, 20),
                1, new ShufflePartitionStats(1, 200, 20),
                2, new ShufflePartitionStats(2, 2000, 200));

        SkewDetectionResult result = detector.detect(stats, config);

        assertThat(result.hasSkew()).isTrue();
        assertThat(result.skewedPartitions()).hasSize(1);
        assertThat(result.skewedPartitions().get(0).partitionId()).isEqualTo(2);
        assertThat(result.skewedPartitions().get(0).dataBytes()).isEqualTo(2000);
        assertThat(result.skewedPartitions().get(0).rowCount()).isEqualTo(200);
        assertThat(result.medianBytes()).isEqualTo(200);
        assertThat(result.maxBytes()).isEqualTo(2000);
    }

    @Test
    void skewBelowMinBytesIsIgnored() {
        // config.minSkewBytes = 100; partition 2 is 3x median but only 90 bytes
        Map<Integer, ShufflePartitionStats> stats = Map.of(
                0, new ShufflePartitionStats(0, 30, 3),
                1, new ShufflePartitionStats(1, 30, 3),
                2, new ShufflePartitionStats(2, 90, 9));

        SkewDetectionResult result = detector.detect(stats, config);
        assertThat(result.hasSkew()).isFalse();
    }

    @Test
    void recommendsSplitsBasedOnTargetSize() {
        // Partition 2 = 500 bytes, target = 50 → ceil(500/50) = 10 splits
        Map<Integer, ShufflePartitionStats> stats = Map.of(
                0, new ShufflePartitionStats(0, 100, 10),
                1, new ShufflePartitionStats(1, 100, 10),
                2, new ShufflePartitionStats(2, 500, 50));

        SkewDetectionResult result = detector.detect(stats, config);
        assertThat(result.hasSkew()).isTrue();
        assertThat(result.skewedPartitions().get(0).recommendedSplits()).isEqualTo(10);
    }

    @Test
    void multipleSkewedPartitions() {
        // 5 partitions: [100, 100, 100, 500, 800] -> median=100, threshold=2*100=200
        // Both 500 and 800 exceed threshold and minSkewBytes -> 2 skewed
        Map<Integer, ShufflePartitionStats> stats = Map.of(
                0, new ShufflePartitionStats(0, 100, 10),
                1, new ShufflePartitionStats(1, 100, 10),
                2, new ShufflePartitionStats(2, 100, 10),
                3, new ShufflePartitionStats(3, 500, 50),
                4, new ShufflePartitionStats(4, 800, 80));

        SkewDetectionResult result = detector.detect(stats, config);
        assertThat(result.hasSkew()).isTrue();
        assertThat(result.skewedPartitions()).hasSize(2);
    }

    @Test
    void medianCalculationEvenPartitions() {
        // 4 partitions: sizes [100, 100, 100, 1000] → median = (100+100)/2 = 100
        Map<Integer, ShufflePartitionStats> stats = Map.of(
                0, new ShufflePartitionStats(0, 100, 10),
                1, new ShufflePartitionStats(1, 100, 10),
                2, new ShufflePartitionStats(2, 100, 10),
                3, new ShufflePartitionStats(3, 1000, 100));

        SkewDetectionResult result = detector.detect(stats, config);
        assertThat(result.medianBytes()).isEqualTo(100);
    }

    @Test
    void splitCountCappedAt64() {
        // 3 partitions: [100, 100, 100000] -> median=100, threshold=2*100=200
        // Partition 2 = 100000 > 200 and > 100 -> skewed
        // splits = ceil(100000/50) = 2000, capped at 64
        Map<Integer, ShufflePartitionStats> stats = Map.of(
                0, new ShufflePartitionStats(0, 100, 10),
                1, new ShufflePartitionStats(1, 100, 10),
                2, new ShufflePartitionStats(2, 100000, 10000));

        SkewDetectionResult result = detector.detect(stats, config);
        assertThat(result.hasSkew()).isTrue();
        assertThat(result.skewedPartitions().get(0).recommendedSplits()).isEqualTo(64);
    }
}
