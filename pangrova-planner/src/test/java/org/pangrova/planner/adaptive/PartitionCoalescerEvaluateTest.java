package org.pangrova.planner.adaptive;

import java.util.Map;

import org.pangrova.shuffle.ShufflePartitionStats;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies partition-aware coalescing via {@link PartitionCoalescer#evaluate}.
 *
 * <p><b>Validates: Requirements REQ-3, REQ-5</b></p>
 */
class PartitionCoalescerEvaluateTest {

    private final PartitionCoalescer coalescer = new PartitionCoalescer();

    @Test
    void emptyPartitionStats_returnsNoOp() {
        ShuffleStageStats stats = new ShuffleStageStats("s1", 0, 0, 0, 0, Map.of());
        CoalesceResult result = coalescer.evaluate(stats, AdaptiveQueryConfig.ENABLED);
        assertThat(result).isEqualTo(CoalesceResult.NO_OP);
        assertThat(result.isCoalesced()).isFalse();
    }

    @Test
    void singlePartition_belowMinPartitions_returnsNoOp() {
        ShuffleStageStats stats = new ShuffleStageStats("s1", 1024, 10, 0, 0,
                Map.of(0, new ShufflePartitionStats(0, 1024, 10)));
        CoalesceResult result = coalescer.evaluate(stats, AdaptiveQueryConfig.ENABLED);
        assertThat(result).isEqualTo(CoalesceResult.NO_OP);
    }

    @Test
    void smallPartitions_coalescedIntoFewerGroups() {
        long mb = 1024L * 1024;
        // 8 partitions of 10MB each = 80MB total; target = 256MB
        // All should fit in 1 group
        Map<Integer, ShufflePartitionStats> partitions = Map.of(
                0, new ShufflePartitionStats(0, 10 * mb, 100),
                1, new ShufflePartitionStats(1, 10 * mb, 100),
                2, new ShufflePartitionStats(2, 10 * mb, 100),
                3, new ShufflePartitionStats(3, 10 * mb, 100),
                4, new ShufflePartitionStats(4, 10 * mb, 100),
                5, new ShufflePartitionStats(5, 10 * mb, 100),
                6, new ShufflePartitionStats(6, 10 * mb, 100),
                7, new ShufflePartitionStats(7, 10 * mb, 100));
        ShuffleStageStats stats = new ShuffleStageStats("s1", 80 * mb, 800, 0, 0, partitions);

        CoalesceResult result = coalescer.evaluate(stats, AdaptiveQueryConfig.ENABLED);

        assertThat(result.isCoalesced()).isTrue();
        assertThat(result.originalCount()).isEqualTo(8);
        assertThat(result.coalescedCount()).isEqualTo(1);
        assertThat(result.groups()).hasSize(1);
        assertThat(result.groups().get(0).partitionIds()).hasSize(8);
    }

    @Test
    void largePartitions_noCoalescing() {
        long mb = 1024L * 1024;
        // 4 partitions of 300MB each; target = 256MB → each is its own group
        Map<Integer, ShufflePartitionStats> partitions = Map.of(
                0, new ShufflePartitionStats(0, 300 * mb, 1000),
                1, new ShufflePartitionStats(1, 300 * mb, 1000),
                2, new ShufflePartitionStats(2, 300 * mb, 1000),
                3, new ShufflePartitionStats(3, 300 * mb, 1000));
        ShuffleStageStats stats = new ShuffleStageStats("s1", 1200 * mb, 4000, 0, 0, partitions);

        CoalesceResult result = coalescer.evaluate(stats, AdaptiveQueryConfig.ENABLED);

        assertThat(result).isEqualTo(CoalesceResult.NO_OP);
    }

    @Test
    void mixedSizes_correctBinPacking() {
        long mb = 1024L * 1024;
        // target = 100MB (custom config)
        AdaptiveQueryConfig config = new AdaptiveQueryConfig(true, 100 * mb, 5.0, 256 * mb, 2);
        // Partitions: 10, 20, 30, 40, 50 MB (sorted ascending)
        // Group 1: 10+20+30 = 60MB (adding 40 would be 100, still fits)
        // Actually: 10+20+30+40 = 100 → fits. 10+20+30+40+50 = 150 > 100
        // So group 1: 10+20+30+40 = 100, group 2: 50
        Map<Integer, ShufflePartitionStats> partitions = Map.of(
                0, new ShufflePartitionStats(0, 50 * mb, 500),
                1, new ShufflePartitionStats(1, 10 * mb, 100),
                2, new ShufflePartitionStats(2, 30 * mb, 300),
                3, new ShufflePartitionStats(3, 20 * mb, 200),
                4, new ShufflePartitionStats(4, 40 * mb, 400));
        ShuffleStageStats stats = new ShuffleStageStats("s1", 150 * mb, 1500, 0, 0, partitions);

        CoalesceResult result = coalescer.evaluate(stats, config);

        assertThat(result.isCoalesced()).isTrue();
        assertThat(result.originalCount()).isEqualTo(5);
        assertThat(result.coalescedCount()).isLessThan(5);
        // All partition ids accounted for
        long totalPartitions = result.groups().stream()
                .mapToLong(g -> g.partitionIds().size())
                .sum();
        assertThat(totalPartitions).isEqualTo(5);
    }

    @Test
    void customMinPartitions_respected() {
        long mb = 1024L * 1024;
        AdaptiveQueryConfig config = new AdaptiveQueryConfig(true, 256 * mb, 5.0, 256 * mb, 5);
        // Only 3 partitions, but minPartitionsForCoalesce = 5
        Map<Integer, ShufflePartitionStats> partitions = Map.of(
                0, new ShufflePartitionStats(0, 1 * mb, 10),
                1, new ShufflePartitionStats(1, 1 * mb, 10),
                2, new ShufflePartitionStats(2, 1 * mb, 10));
        ShuffleStageStats stats = new ShuffleStageStats("s1", 3 * mb, 30, 0, 0, partitions);

        CoalesceResult result = coalescer.evaluate(stats, config);
        assertThat(result).isEqualTo(CoalesceResult.NO_OP);
    }
}
