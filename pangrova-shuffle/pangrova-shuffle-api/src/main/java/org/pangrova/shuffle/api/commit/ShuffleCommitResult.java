package org.pangrova.shuffle.api.commit;

import java.util.Map;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Result of committing a shuffle stage, containing partition-level statistics
 * for AQE (Adaptive Query Execution) evaluation.
 *
 * @param partitionStats per-partition statistics keyed by partition ID
 * @param totalBytes     total bytes across all partitions (non-negative)
 * @param totalRows      total rows across all partitions (non-negative)
 */
public record ShuffleCommitResult(
        Map<Integer, ShufflePartitionStats> partitionStats,
        long totalBytes,
        long totalRows
) {

    /** Empty result for no-op or local shuffle paths. */
    public static final ShuffleCommitResult EMPTY = new ShuffleCommitResult(Map.of(), 0, 0);

    public ShuffleCommitResult {
        checkNotNull(partitionStats, "partitionStats must not be null");
        partitionStats = Map.copyOf(partitionStats);
        if (totalBytes < 0) {
            throw new IllegalArgumentException("totalBytes must be non-negative, got: " + totalBytes);
        }
        if (totalRows < 0) {
            throw new IllegalArgumentException("totalRows must be non-negative, got: " + totalRows);
        }
    }
}
