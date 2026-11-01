package org.pangrova.planner.property.adaptive;

import org.pangrova.planner.adaptive.AdaptiveQueryConfig;
import org.pangrova.planner.adaptive.ShuffleStageStats;
import org.pangrova.planner.adaptive.SkewJoinHandler;
import org.pangrova.planner.adaptive.SkewResult;
import java.util.HashMap;
import java.util.Map;

import org.pangrova.shuffle.api.commit.ShufflePartitionStats;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.LongRange;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based tests for {@link SkewJoinHandler#detectSkew}.
 *
 * <p><b>Validates: Requirements REQ-3, REQ-6</b></p>
 */
class SkewJoinHandlerPropertyTest {

    private final SkewJoinHandler handler = new SkewJoinHandler();

    /**
     * Property 1: Every skewed partition exceeds median × threshold AND minBytes.
     */
    @Property(tries = 10)
    void skewedPartitionsExceedThresholdAndMinBytes(
            @ForAll @IntRange(min = 2, max = 50) int numPartitions,
            @ForAll @LongRange(min = 1, max = 2_000_000_000) long maxPartitionBytes) {

        AdaptiveQueryConfig config = AdaptiveQueryConfig.ENABLED;
        Map<Integer, ShufflePartitionStats> partitions = generatePartitions(numPartitions, maxPartitionBytes);
        ShuffleStageStats stats = buildStats(partitions);

        SkewResult result = handler.detectSkew(stats, config);

        if (!result.hasSkew()) {
            return;
        }

        double threshold = config.skewPartitionThresholdRatio();
        long minBytes = config.skewPartitionMinBytes();
        long median = result.medianBytes();

        for (int id : result.skewedPartitions()) {
            long size = partitions.get(id).dataBytes();
            assertThat(size)
                    .as("Skewed partition %d must exceed median × threshold", id)
                    .isGreaterThan((long) (median * threshold));
            assertThat(size)
                    .as("Skewed partition %d must exceed minBytes", id)
                    .isGreaterThan(minBytes);
        }
    }

    /**
     * Property 2: Non-skewed partitions do NOT exceed both thresholds simultaneously.
     */
    @Property(tries = 10)
    void nonSkewedPartitionsBelowThreshold(
            @ForAll @IntRange(min = 2, max = 50) int numPartitions,
            @ForAll @LongRange(min = 1, max = 2_000_000_000) long maxPartitionBytes) {

        AdaptiveQueryConfig config = AdaptiveQueryConfig.ENABLED;
        Map<Integer, ShufflePartitionStats> partitions = generatePartitions(numPartitions, maxPartitionBytes);
        ShuffleStageStats stats = buildStats(partitions);

        SkewResult result = handler.detectSkew(stats, config);

        if (!result.hasSkew()) {
            return; // NO_SKEW has medianBytes=0, skip threshold check
        }

        double threshold = config.skewPartitionThresholdRatio();
        long minBytes = config.skewPartitionMinBytes();
        long median = result.medianBytes();

        for (Map.Entry<Integer, ShufflePartitionStats> entry : partitions.entrySet()) {
            if (!result.skewedPartitions().contains(entry.getKey())) {
                long size = entry.getValue().dataBytes();
                boolean exceedsRatio = size > median * threshold;
                boolean exceedsMin = size > minBytes;
                assertThat(exceedsRatio && exceedsMin)
                        .as("Non-skewed partition %d should not exceed both thresholds", entry.getKey())
                        .isFalse();
            }
        }
    }

    /**
     * Property 3: maxBytes in result is always >= medianBytes.
     */
    @Property(tries = 10)
    void maxBytesAlwaysGreaterOrEqualMedian(
            @ForAll @IntRange(min = 2, max = 50) int numPartitions,
            @ForAll @LongRange(min = 1, max = 2_000_000_000) long maxPartitionBytes) {

        Map<Integer, ShufflePartitionStats> partitions = generatePartitions(numPartitions, maxPartitionBytes);
        ShuffleStageStats stats = buildStats(partitions);

        SkewResult result = handler.detectSkew(stats, AdaptiveQueryConfig.ENABLED);

        if (result.hasSkew()) {
            assertThat(result.maxBytes()).isGreaterThanOrEqualTo(result.medianBytes());
        }
    }

    private Map<Integer, ShufflePartitionStats> generatePartitions(int count, long maxBytes) {
        Map<Integer, ShufflePartitionStats> map = new HashMap<>();
        for (int i = 0; i < count; i++) {
            long bytes = 1 + (maxBytes * (i + 1)) / count;
            map.put(i, new ShufflePartitionStats(i, bytes, bytes / 100));
        }
        return map;
    }

    private ShuffleStageStats buildStats(Map<Integer, ShufflePartitionStats> partitions) {
        long total = partitions.values().stream().mapToLong(ShufflePartitionStats::dataBytes).sum();
        long rows = partitions.values().stream().mapToLong(ShufflePartitionStats::rowCount).sum();
        return new ShuffleStageStats("s-prop", total, rows, 0, 0, partitions);
    }
}
