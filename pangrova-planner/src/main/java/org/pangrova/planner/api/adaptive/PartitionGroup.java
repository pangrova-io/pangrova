package org.pangrova.planner.api.adaptive;

import org.pangrova.common.api.annotation.Internal;

import java.util.List;

/**
 * A group of partitions coalesced into a single logical partition.
 *
 * @param partitionIds partition identifiers in this group
 * @param totalBytes   sum of data bytes across all partitions in the group
 */
@Internal
public record PartitionGroup(List<Integer> partitionIds, long totalBytes) {

    public PartitionGroup {
        partitionIds = List.copyOf(partitionIds);
        if (totalBytes < 0) {
            throw new IllegalArgumentException("totalBytes must be non-negative, got: " + totalBytes);
        }
    }
}
