package org.pangrova.planner.adaptive;

import org.pangrova.planner.api.adaptive.CoalesceResult;
import org.pangrova.planner.api.adaptive.PartitionGroup;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.pangrova.shuffle.api.ShufflePartitionStats;

/**
 * Forms adjacent partition ranges under a byte budget.
 *
 * <p>Original partition order makes it possible to align inputs of a
 * partitioned operator. A candidate requires statistics for every original
 * partition, including an explicit zero for an empty partition.</p>
 */
public class PartitionCoalescer {

    /**
     * Evaluates partition coalescing using real per-partition stats.
     */
    public CoalesceResult evaluate(ShuffleStageStats stats, AdaptiveQueryConfig config) {
        Map<Integer, ShufflePartitionStats> partitionStats = stats.partitionStats();
        int partitionCount = partitionStats.size();
        if (partitionCount < Math.max(2, config.minPartitionsForCoalesce())) {
            return CoalesceResult.NO_OP;
        }
        List<PartitionGroup> groups = new ArrayList<>();
        long targetBytes = config.targetPartitionBytes();
        int firstPartition = 0;
        long currentBytes = 0;
        for (int partition = 0; partition < partitionCount; partition++) {
            var observed = partitionStats.get(partition);
            if (observed == null || observed.partitionId() != partition) {
                return CoalesceResult.NO_OP;
            }
            long partBytes = observed.dataBytes();
            if (partition > firstPartition && currentBytes > targetBytes - partBytes) {
                groups.add(new PartitionGroup(firstPartition, partition, currentBytes));
                firstPartition = partition;
                currentBytes = 0;
            }
            currentBytes += partBytes;
        }
        groups.add(new PartitionGroup(firstPartition, partitionCount, currentBytes));
        return groups.size() < partitionCount
                ? new CoalesceResult(groups, partitionCount, groups.size()) : CoalesceResult.NO_OP;
    }

}
