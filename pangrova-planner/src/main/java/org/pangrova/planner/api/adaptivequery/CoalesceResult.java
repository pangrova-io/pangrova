package org.pangrova.planner.api.adaptivequery;

import org.pangrova.common.api.annotation.Internal;

import java.util.List;

/**
 * Result of partition coalescing evaluation.
 *
 * @param groups           coalesced partition groups
 * @param originalCount    number of partitions before coalescing
 * @param coalescedCount   number of groups after coalescing
 */
@Internal
public record CoalesceResult(List<PartitionGroup> groups, int originalCount, int coalescedCount) {

    /** No coalescing performed. */
    public static final CoalesceResult NO_OP = new CoalesceResult(List.of(), 0, 0);

    public CoalesceResult {
        groups = List.copyOf(groups);
        int nextPartition = 0;
        for (var group : groups) {
            if (group.startInclusive() != nextPartition) {
                throw new IllegalArgumentException("Coalesced ranges must cover each original partition once");
            }
            nextPartition = group.endExclusive();
        }
        if (nextPartition != originalCount || groups.size() != coalescedCount) {
            throw new IllegalArgumentException("Coalesced range coverage does not match the partition counts");
        }
    }

    /** Returns true when coalescing reduced the partition count. */
    public boolean isCoalesced() {
        return coalescedCount > 0 && coalescedCount < originalCount;
    }
}
