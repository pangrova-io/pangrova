package org.pangrova.planner.api.adaptive;

import org.pangrova.common.api.annotation.Internal;

/**
 * A contiguous range of committed output partitions for a candidate consumer.
 *
 * @param startInclusive first original partition
 * @param endExclusive partition after the last one in the range
 * @param totalBytes   sum of data bytes across all partitions in the group
 */
@Internal
public record PartitionGroup(int startInclusive, int endExclusive, long totalBytes) {

    public PartitionGroup {
        if (startInclusive < 0 || endExclusive <= startInclusive || totalBytes < 0) {
            throw new IllegalArgumentException("Invalid shuffle partition range");
        }
    }

    /** Number of original partitions covered by this range. */
    public int partitionCount() {
        return endExclusive - startInclusive;
    }
}
