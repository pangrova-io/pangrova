package org.pangrova.planner.adaptive;

import java.util.List;

/**
 * Result of partition coalescing evaluation.
 *
 * @param groups           coalesced partition groups
 * @param originalCount    number of partitions before coalescing
 * @param coalescedCount   number of groups after coalescing
 */
public record CoalesceResult(List<PartitionGroup> groups, int originalCount, int coalescedCount) {

    /** No coalescing performed. */
    public static final CoalesceResult NO_OP = new CoalesceResult(List.of(), 0, 0);

    public CoalesceResult {
        groups = List.copyOf(groups);
    }

    /** Returns true when coalescing reduced the partition count. */
    public boolean isCoalesced() {
        return coalescedCount > 0 && coalescedCount < originalCount;
    }
}
