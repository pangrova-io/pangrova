package org.pangrova.planner.core.adaptive;

import org.pangrova.planner.api.adaptive.CoalesceResult;
import org.pangrova.planner.api.adaptive.PartitionGroup;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.pangrova.shuffle.api.ShufflePartitionStats;

/**
 * Merges small shuffle partitions using greedy bin-packing.
 *
 * <p>Sorts partitions ascending by size and greedily packs them into groups
 * up to {@link AdaptiveQueryConfig#targetPartitionBytes()}. Returns
 * {@link CoalesceResult#NO_OP} when partition stats are unavailable or
 * coalescing would not reduce count.</p>
 */
public class PartitionCoalescer {

    /**
     * Evaluates partition coalescing using real per-partition stats.
     */
    public CoalesceResult evaluate(ShuffleStageStats stats, AdaptiveQueryConfig config) {
        Map<Integer, ShufflePartitionStats> partitionStats = stats.partitionStats();
        if (partitionStats.isEmpty() || partitionStats.size() < config.minPartitionsForCoalesce()) {
            return CoalesceResult.NO_OP;
        }
        List<PartitionGroup> groups = packPartitions(partitionStats, config.targetPartitionBytes());
        if (groups.size() >= partitionStats.size()) {
            return CoalesceResult.NO_OP;
        }
        return new CoalesceResult(groups, partitionStats.size(), groups.size());
    }

    private List<PartitionGroup> packPartitions(Map<Integer, ShufflePartitionStats> partitionStats,
                                                long targetBytes) {
        List<Map.Entry<Integer, ShufflePartitionStats>> sorted = sortBySize(partitionStats);
        return binPack(sorted, targetBytes);
    }

    private static List<Map.Entry<Integer, ShufflePartitionStats>> sortBySize(
            Map<Integer, ShufflePartitionStats> partitionStats) {
        return partitionStats.entrySet()
                .stream()
                .sorted(Comparator.comparingLong(e -> e.getValue().dataBytes()))
                .toList();
    }

    private static List<PartitionGroup> binPack(
            List<Map.Entry<Integer, ShufflePartitionStats>> sorted, long targetBytes) {
        List<PartitionGroup> groups = new ArrayList<>();
        List<Integer> currentIds = new ArrayList<>();
        long currentBytes = 0;

        for (Map.Entry<Integer, ShufflePartitionStats> entry : sorted) {
            long partBytes = entry.getValue().dataBytes();
            if (!currentIds.isEmpty() && currentBytes + partBytes > targetBytes) {
                groups.add(new PartitionGroup(currentIds, currentBytes));
                currentIds = new ArrayList<>();
                currentBytes = 0;
            }
            currentIds.add(entry.getKey());
            currentBytes += partBytes;
        }
        if (!currentIds.isEmpty()) {
            groups.add(new PartitionGroup(currentIds, currentBytes));
        }
        return groups;
    }

}
