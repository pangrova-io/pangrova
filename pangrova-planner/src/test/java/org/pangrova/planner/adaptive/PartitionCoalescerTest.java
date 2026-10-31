package org.pangrova.planner.adaptive;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.pangrova.shuffle.api.ShufflePartitionStats;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link PartitionCoalescer#evaluate(ShuffleStageStats, AdaptiveQueryConfig)}.
 */
class PartitionCoalescerTest {

    private static final long MB = 1024L * 1024;
    private final PartitionCoalescer coalescer = new PartitionCoalescer();

    @Test
    void emptyPartitions_returnsNoOp() {
        ShuffleStageStats stats = statsWithPartitions(Map.of());
        assertThat(coalescer.evaluate(stats, AdaptiveQueryConfig.ENABLED)).isEqualTo(CoalesceResult.NO_OP);
    }

    @Test
    void singlePartition_belowMinPartitions_returnsNoOp() {
        var stats = statsWithPartitions(Map.of(0, new ShufflePartitionStats(0, 100 * MB, 1000)));
        assertThat(coalescer.evaluate(stats, AdaptiveQueryConfig.ENABLED)).isEqualTo(CoalesceResult.NO_OP);
    }

    @Test
    void allSmallPartitions_coalescedIntoFewerGroups() {
        Map<Integer, ShufflePartitionStats> partitions = IntStream.range(0, 10)
                .boxed()
                .collect(Collectors.toMap(i -> i, i -> new ShufflePartitionStats(i, 10 * MB, 100)));
        var stats = statsWithPartitions(partitions);
        var result = coalescer.evaluate(stats, AdaptiveQueryConfig.ENABLED);

        assertThat(result.isCoalesced()).isTrue();
        assertThat(result.coalescedCount()).isLessThan(result.originalCount());
    }

    @Test
    void totalDataPreserved() {
        var partitions = Map.of(
                0, new ShufflePartitionStats(0, 30 * MB, 100),
                1, new ShufflePartitionStats(1, 20 * MB, 100),
                2, new ShufflePartitionStats(2, 50 * MB, 100),
                3, new ShufflePartitionStats(3, 10 * MB, 100));
        var stats = statsWithPartitions(partitions);
        var result = coalescer.evaluate(stats, AdaptiveQueryConfig.ENABLED);

        long originalTotal = partitions.values().stream().mapToLong(ShufflePartitionStats::dataBytes).sum();
        long coalescedTotal = result.groups().stream().mapToLong(PartitionGroup::totalBytes).sum();
        assertThat(coalescedTotal).isEqualTo(originalTotal);
    }

    @Test
    void noGroupExceedsTargetSize() {
        var config = new AdaptiveQueryConfig(true, 128 * MB, 5.0, 256 * MB, 2);
        var partitions = Map.of(
                0, new ShufflePartitionStats(0, 60 * MB, 100),
                1, new ShufflePartitionStats(1, 60 * MB, 100),
                2, new ShufflePartitionStats(2, 60 * MB, 100),
                3, new ShufflePartitionStats(3, 10 * MB, 100));
        var stats = statsWithPartitions(partitions);
        var result = coalescer.evaluate(stats, config);

        assertThat(result.isCoalesced()).isTrue();
        assertThat(result.groups()).allSatisfy(g ->
                assertThat(g.totalBytes()).isLessThanOrEqualTo(config.targetPartitionBytes()));
    }

    @Test
    void alreadyOptimalPartitions_returnsNoOp() {
        var config = new AdaptiveQueryConfig(true, 64 * MB, 5.0, 256 * MB, 2);
        var partitions = Map.of(
                0, new ShufflePartitionStats(0, 64 * MB, 100),
                1, new ShufflePartitionStats(1, 64 * MB, 100));
        var stats = statsWithPartitions(partitions);
        var result = coalescer.evaluate(stats, config);

        assertThat(result).isEqualTo(CoalesceResult.NO_OP);
    }

    @Test
    void ascendingSortOrder_smallPartitionsPackedFirst() {
        var config = new AdaptiveQueryConfig(true, 100 * MB, 5.0, 256 * MB, 2);
        var partitions = Map.of(
                0, new ShufflePartitionStats(0, 10 * MB, 100),
                1, new ShufflePartitionStats(1, 20 * MB, 100),
                2, new ShufflePartitionStats(2, 30 * MB, 100),
                3, new ShufflePartitionStats(3, 80 * MB, 100));
        var stats = statsWithPartitions(partitions);
        var result = coalescer.evaluate(stats, config);

        assertThat(result.isCoalesced()).isTrue();
        assertThat(result.coalescedCount()).isEqualTo(2);
    }

    private static ShuffleStageStats statsWithPartitions(Map<Integer, ShufflePartitionStats> partitions) {
        long totalBytes = partitions.values().stream().mapToLong(ShufflePartitionStats::dataBytes).sum();
        long totalRows = partitions.values().stream().mapToLong(ShufflePartitionStats::rowCount).sum();
        return new ShuffleStageStats("test-stage", totalBytes, totalRows, totalBytes, totalRows, partitions);
    }
}
