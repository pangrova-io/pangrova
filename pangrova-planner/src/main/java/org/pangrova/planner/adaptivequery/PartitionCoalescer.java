package org.pangrova.planner.adaptivequery;

import org.pangrova.planner.api.adaptivequery.CoalesceResult;
import org.pangrova.planner.api.adaptivequery.PartitionGroup;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.IntToLongFunction;

import org.pangrova.shuffle.api.ShuffleCommitReceipt;
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
        return group(partitionStats.size(), config, Integer.MAX_VALUE, partition -> {
            var observed = partitionStats.get(partition);
            return observed != null && observed.partitionId() == partition ? observed.dataBytes() : -1;
        });
    }

    /** Bounds reads per consumer using the committed publication, without copying its partition statistics. */
    public CoalesceResult evaluate(ShuffleCommitReceipt receipt, int partitionCount,
            AdaptiveQueryConfig config, int maxPartitionsPerGroup) {
        if (receipt.partitions().size() != partitionCount || maxPartitionsPerGroup < 2) {
            return CoalesceResult.NO_OP;
        }
        return group(partitionCount, config, maxPartitionsPerGroup,
                partition -> receipt.partitions().get(partition).bytes());
    }

    /** Uses the combined size of each original partition to form one range mapping for every input. */
    public CoalesceResult evaluateAligned(List<ShuffleCommitReceipt> receipts, int partitionCount,
            AdaptiveQueryConfig config, int maxPartitionsPerGroup) {
        if (receipts.size() < 2 || maxPartitionsPerGroup < 2
                || receipts.stream().anyMatch(receipt -> receipt.partitions().size() != partitionCount)) {
            return CoalesceResult.NO_OP;
        }
        return group(partitionCount, config, maxPartitionsPerGroup, partition -> {
            long total = 0;
            for (var receipt : receipts) {
                total += Math.min(Long.MAX_VALUE - total, receipt.partitions().get(partition).bytes());
            }
            return total;
        });
    }

    private static CoalesceResult group(int partitionCount, AdaptiveQueryConfig config,
            int maxPartitionsPerGroup, IntToLongFunction partitionBytes) {
        if (partitionCount < Math.max(2, config.minPartitionsForCoalesce())) {
            return CoalesceResult.NO_OP;
        }
        List<PartitionGroup> groups = new ArrayList<>();
        long targetBytes = config.targetPartitionBytes();
        int firstPartition = 0;
        long currentBytes = 0;
        for (int partition = 0; partition < partitionCount; partition++) {
            long partBytes = partitionBytes.applyAsLong(partition);
            if (partBytes < 0) {
                return CoalesceResult.NO_OP;
            }
            if (partition > firstPartition && (partition - firstPartition == maxPartitionsPerGroup
                    || currentBytes > targetBytes - partBytes)) {
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
