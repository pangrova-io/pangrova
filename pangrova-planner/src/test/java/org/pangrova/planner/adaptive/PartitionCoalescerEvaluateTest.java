package org.pangrova.planner.adaptive;

import org.pangrova.planner.api.adaptive.CoalesceResult;
import org.pangrova.planner.api.adaptive.PartitionGroup;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.pangrova.shuffle.api.ShufflePartitionStats;
import org.pangrova.shuffle.api.ShuffleCommitReceipt;
import org.pangrova.shuffle.api.ShufflePublication;
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
        assertThat(result.groups().get(0).partitionCount()).isEqualTo(8);
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
    void mixedSizes_keepAdjacentOriginalPartitionRanges() {
        long mb = 1024L * 1024;
        // target = 100MB (custom config)
        AdaptiveQueryConfig config = new AdaptiveQueryConfig(true, 100 * mb, 5.0, 256 * mb, 2);
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
        assertThat(result.groups()).containsExactly(
                new org.pangrova.planner.api.adaptive.PartitionGroup(0, 3, 90 * mb),
                new org.pangrova.planner.api.adaptive.PartitionGroup(3, 5, 60 * mb));
    }

    @Test
    void missingPartitionIsDifferentFromAnExplicitEmptyPartition() {
        var first = new ShufflePartitionStats(0, 32, 1);
        var third = new ShufflePartitionStats(2, 64, 1);
        var incomplete = new ShuffleStageStats("s1", 96, 2, 0, 0, Map.of(0, first, 2, third));
        var complete = new ShuffleStageStats("s1", 96, 2, 0, 0,
                Map.of(0, first, 1, new ShufflePartitionStats(1, 0, 0), 2, third));

        assertThat(coalescer.evaluate(incomplete, AdaptiveQueryConfig.ENABLED)).isEqualTo(CoalesceResult.NO_OP);
        assertThat(coalescer.evaluate(complete, AdaptiveQueryConfig.ENABLED).groups()).containsExactly(
                new org.pangrova.planner.api.adaptive.PartitionGroup(0, 3, 96));
    }

    @Test
    void committedReceiptsRequireExactCoverageAndBoundTheReadsPerConsumer() {
        String checksum = "00".repeat(32);
        var empty = new ShuffleCommitReceipt.Partition(-1, 0, 0, checksum);
        var full = new ShuffleCommitReceipt.Partition(0, 16, 1, checksum);
        var receipt = new ShuffleCommitReceipt(new ShufflePublication("query", 1, 2, "shuffle"),
                1, checksum, Map.of(0, full, 1, empty, 2, full, 3, full));

        assertThat(coalescer.evaluate(receipt, 5, AdaptiveQueryConfig.ENABLED, 2)).isEqualTo(CoalesceResult.NO_OP);
        assertThat(coalescer.evaluate(receipt, 4, AdaptiveQueryConfig.ENABLED, 1)).isEqualTo(CoalesceResult.NO_OP);
        assertThat(coalescer.evaluate(receipt, 4, AdaptiveQueryConfig.ENABLED, 2).groups()).containsExactly(
                new org.pangrova.planner.api.adaptive.PartitionGroup(0, 2, 16),
                new org.pangrova.planner.api.adaptive.PartitionGroup(2, 4, 32));
    }

    @Test
    void alignedJoinUsesCombinedBytesAndChecksEachInputsCoverage() {
        var config = new AdaptiveQueryConfig(true, 100, 5.0, 256, 2);
        var left = receipt(1, 45, 20, 30, 0);
        var right = receipt(2, 15, 10, 20, 20);
        assertThat(coalescer.evaluateAligned(List.of(left, right), 4, config, 4).groups()).containsExactly(
                new PartitionGroup(0, 2, 90), new PartitionGroup(2, 4, 70));
        assertThat(coalescer.evaluateAligned(List.of(left, receipt(2, 15, 10)), 4, config, 4))
                .isEqualTo(CoalesceResult.NO_OP);
        assertThat(coalescer.evaluateAligned(List.of(left, right), 4, config, 1)).isEqualTo(CoalesceResult.NO_OP);
    }

    @Test
    void combinedPartitionBytesSaturateWithoutCombiningAnOversizePartition() {
        var config = new AdaptiveQueryConfig(true, 100, 5.0, 256, 2);
        var left = receipt(1, Long.MAX_VALUE, 0, 1);
        var right = receipt(2, 1, 0, 1);
        assertThat(coalescer.evaluateAligned(List.of(left, right), 3, config, 3).groups()).containsExactly(
                new PartitionGroup(0, 1, Long.MAX_VALUE), new PartitionGroup(1, 3, 2));
    }

    private static ShuffleCommitReceipt receipt(int stage, long... bytes) {
        String checksum = "00".repeat(32);
        var partitions = new HashMap<Integer, ShuffleCommitReceipt.Partition>();
        for (int i = 0; i < bytes.length; i++) {
            long rows = bytes[i] == 0 ? 0 : 1;
            partitions.put(i, new ShuffleCommitReceipt.Partition(rows - 1, bytes[i], rows, checksum));
        }
        return new ShuffleCommitReceipt(new ShufflePublication("query", stage, 2, "shuffle-" + stage),
                1, checksum, partitions);
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
