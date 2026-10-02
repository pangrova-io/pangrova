package org.pangrova.coordinator.query.execution.dispatch.dag.binding;

import java.util.ArrayList;
import java.util.List;
import org.pangrova.coordinator.query.execution.split.SplitFetcher;
import org.pangrova.spi.annotation.Internal;

/** Generic immutable split allocation and affinity metadata; no connector enumeration or dispatch. */
@Internal
public final class StageSplitPartitions {
    private StageSplitPartitions() {}

    /**
     * Distributes splits across partitions using greedy load balancing.
     * Sorts splits by serialized size descending, then assigns each split
     * to the partition with the smallest accumulated load (LPT algorithm).
     * This reduces data skew compared to round-robin when split sizes vary.
     */
    public static List<List<String>> partitionSplits(List<String> splits, int numPartitions) {
        List<List<String>> result = new ArrayList<>(numPartitions);
        long[] partitionLoad = new long[numPartitions];
        for (int i = 0; i < numPartitions; i++) {
            result.add(new ArrayList<>());
        }
        if (splits.isEmpty()) {
            return result;
        }
        // Sort indices by split serialized length descending (LPT heuristic).
        Integer[] indices = new Integer[splits.size()];
        for (int i = 0; i < indices.length; i++) {
            indices[i] = i;
        }
        java.util.Arrays.sort(indices,
                (a, b) -> Integer.compare(splits.get(b).length(), splits.get(a).length()));
        for (int idx : indices) {
            int minPartition = 0;
            for (int p = 1; p < numPartitions; p++) {
                if (partitionLoad[p] < partitionLoad[minPartition]) {
                    minPartition = p;
                }
            }
            result.get(minPartition).add(splits.get(idx));
            partitionLoad[minPartition] += splits.get(idx).length();
        }
        return result;
    }

    /** Derives physical-object cache affinity without changing the exact split allocation order. */
    public static List<List<String>> buildPartitionFilePaths(
            List<SplitFetcher.SplitWithPaths> splitsWithPaths,
            List<List<String>> partitionedSplits) {
        List<List<String>> result = new ArrayList<>(partitionedSplits.size());
        for (int i = 0; i < partitionedSplits.size(); i++) {
            result.add(new ArrayList<>());
        }
        java.util.Map<String, List<String>> pathsBySplit = splitsWithPaths.stream()
                .collect(java.util.stream.Collectors.toUnmodifiableMap(
                        SplitFetcher.SplitWithPaths::serializedData,
                        SplitFetcher.SplitWithPaths::filePaths));
        for (int partition = 0; partition < partitionedSplits.size(); partition++) {
            for (String split : partitionedSplits.get(partition)) {
                List<String> paths = pathsBySplit.get(split);
                if (paths == null) {
                    throw new IllegalStateException(
                            "partition contains a split absent from affinity metadata");
                }
                result.get(partition).addAll(paths);
            }
        }
        return result;
    }

}
