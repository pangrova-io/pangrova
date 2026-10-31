package org.pangrova.coordinator.query.execution.result;

import java.util.List;

/**
 * Metadata describing partial query results when PARTIAL_RESULT_OK hint is active.
 */
public record PartialResultMetadata(
        int completedTasks,
        int totalTasks,
        double completenessRatio,
        List<Integer> missingPartitions,
        String warningMessage) {

    public PartialResultMetadata {
        missingPartitions = List.copyOf(missingPartitions);
    }

    /**
     * Factory method computing ratio and generating a warning message.
     */
    public static PartialResultMetadata of(int completed, int total, List<Integer> missing) {
        double ratio = total > 0 ? (double) completed / total : 0.0;
        String warning = String.format(
                "Partial result: %d/%d tasks completed (%.1f%%), %d partitions missing",
                completed, total, ratio * 100, missing.size());
        return new PartialResultMetadata(completed, total, ratio, List.copyOf(missing), warning);
    }
}
