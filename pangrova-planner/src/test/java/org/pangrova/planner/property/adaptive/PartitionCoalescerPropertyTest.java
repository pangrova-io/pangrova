package org.pangrova.planner.property.adaptive;

import org.pangrova.planner.adaptive.AdaptiveQueryConfig;
import org.pangrova.planner.api.adaptive.CoalesceResult;
import org.pangrova.planner.adaptive.PartitionCoalescer;
import org.pangrova.planner.api.adaptive.PartitionGroup;
import org.pangrova.planner.adaptive.ShuffleStageStats;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.pangrova.shuffle.api.ShufflePartitionStats;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.LongRange;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based tests for {@link PartitionCoalescer#evaluate}.
 *
 * <p><b>Validates: Requirements REQ-3, REQ-5</b></p>
 */
class PartitionCoalescerPropertyTest {

    private final PartitionCoalescer coalescer = new PartitionCoalescer();

    /**
     * Property 1: All original partition ids appear exactly once across coalesced groups.
     */
    @Property(tries = 10)
    void allPartitionsAccountedFor(
            @ForAll @IntRange(min = 2, max = 50) int numPartitions,
            @ForAll @LongRange(min = 1, max = 500_000_000) long maxPartitionBytes) {

        Map<Integer, ShufflePartitionStats> partitions = generatePartitions(numPartitions, maxPartitionBytes);
        ShuffleStageStats stats = buildStats(partitions);

        CoalesceResult result = coalescer.evaluate(stats, AdaptiveQueryConfig.ENABLED);

        if (result == CoalesceResult.NO_OP) {
            return; // no coalescing, nothing to verify
        }

        Set<Integer> allIds = new HashSet<>();
        for (PartitionGroup group : result.groups()) {
            for (int id = group.startInclusive(); id < group.endExclusive(); id++) {
                assertThat(allIds.add(id))
                        .as("Partition %d appears in multiple groups", id)
                        .isTrue();
            }
        }
        assertThat(allIds).isEqualTo(partitions.keySet());
    }

    /**
     * Property 2: No coalesced group exceeds targetPartitionBytes
     * (except single-partition groups that are inherently larger).
     */
    @Property(tries = 10)
    void groupsRespectTargetSize(
            @ForAll @IntRange(min = 2, max = 50) int numPartitions,
            @ForAll @LongRange(min = 1, max = 500_000_000) long maxPartitionBytes) {

        long targetBytes = AdaptiveQueryConfig.ENABLED.targetPartitionBytes();
        Map<Integer, ShufflePartitionStats> partitions = generatePartitions(numPartitions, maxPartitionBytes);
        ShuffleStageStats stats = buildStats(partitions);

        CoalesceResult result = coalescer.evaluate(stats, AdaptiveQueryConfig.ENABLED);

        if (result == CoalesceResult.NO_OP) {
            return;
        }

        for (PartitionGroup group : result.groups()) {
            if (group.partitionCount() > 1) {
                assertThat(group.totalBytes())
                        .as("Multi-partition group exceeds target")
                        .isLessThanOrEqualTo(targetBytes);
            }
        }
    }

    /**
     * Property 3: Coalesced count is always less than or equal to original count.
     */
    @Property(tries = 10)
    void coalescedCountNeverExceedsOriginal(
            @ForAll @IntRange(min = 2, max = 50) int numPartitions,
            @ForAll @LongRange(min = 1, max = 500_000_000) long maxPartitionBytes) {

        Map<Integer, ShufflePartitionStats> partitions = generatePartitions(numPartitions, maxPartitionBytes);
        ShuffleStageStats stats = buildStats(partitions);

        CoalesceResult result = coalescer.evaluate(stats, AdaptiveQueryConfig.ENABLED);

        if (result != CoalesceResult.NO_OP) {
            assertThat(result.coalescedCount()).isLessThan(result.originalCount());
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
