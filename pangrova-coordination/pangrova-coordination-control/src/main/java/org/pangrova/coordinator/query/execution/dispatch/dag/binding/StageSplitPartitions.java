package org.pangrova.coordinator.query.execution.dispatch.dag.binding;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.function.ToLongFunction;
import org.pangrova.common.api.annotation.Internal;

/** Generic immutable split allocation and affinity metadata; no connector enumeration or dispatch. */
@Internal
public final class StageSplitPartitions {
    // Small cohorts avoid heap maintenance. Large cohorts use logarithmic placement.
    private static final int HEAP_PARTITION_THRESHOLD = 64;

    private StageSplitPartitions() {}

    /**
     * Assigns the largest scans first to the least loaded partition. When a scan
     * estimate is unavailable, its weight is one unit. Ties follow input order
     * and then partition ID, keeping allocations deterministic.
     */
    public static <T> List<List<T>> partitionSplits(
            List<T> splits, int numPartitions, ToLongFunction<T> scanBytes) {
        if (numPartitions == 1) {
            return List.of(List.copyOf(splits));
        }
        List<List<T>> result = new ArrayList<>(numPartitions);
        for (int i = 0; i < numPartitions; i++) {
            result.add(new ArrayList<>());
        }
        if (splits.isEmpty()) {
            return result;
        }
        if (numPartitions == 0) {
            throw new IllegalArgumentException("A nonempty scan requires at least one partition");
        }
        var ordered = new ArrayList<>(splits);
        ordered.sort(Comparator.comparingLong(scanBytes).reversed());
        long[] partitionLoad = new long[numPartitions];
        var heap = numPartitions >= HEAP_PARTITION_THRESHOLD
                ? new PriorityQueue<Integer>(Comparator.comparingLong((Integer id) -> partitionLoad[id])
                        .thenComparingInt(Integer::intValue))
                : null;
        if (heap != null) {
            for (int i = 0; i < numPartitions; i++) {
                heap.add(i);
            }
        }
        for (T split : ordered) {
            int partition = heap == null ? leastLoadedPartition(partitionLoad) : heap.remove();
            result.get(partition).add(split);
            partitionLoad[partition] += Math.min(Long.MAX_VALUE - partitionLoad[partition],
                    Math.max(1L, scanBytes.applyAsLong(split)));
            if (heap != null) {
                heap.add(partition);
            }
        }
        return result;
    }

    private static int leastLoadedPartition(long[] load) {
        int best = 0;
        for (int i = 1; i < load.length; i++) {
            if (load[i] < load[best]) {
                best = i;
            }
        }
        return best;
    }

}
