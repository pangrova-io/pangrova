package org.pangrova.coordinator.ai.embedding;

/**
 * Immutable descriptor for a partition of backfill work.
 *
 * <p>Represents a contiguous range of rows to be embedded by a single Worker task.
 * The start offset and row count define the slice within the snapshot scan.</p>
 *
 * @param partitionId unique identifier for this partition
 * @param startOffset row offset within the full scan result
 * @param rowCount    number of rows in this partition
 */
public record BackfillPartition(
        String partitionId,
        long startOffset,
        long rowCount) {

    public BackfillPartition {
        if (partitionId == null || partitionId.isBlank()) {
            throw new IllegalArgumentException("partitionId required");
        }
        if (startOffset < 0) {
            throw new IllegalArgumentException("startOffset must be non-negative");
        }
        if (rowCount <= 0) {
            throw new IllegalArgumentException("rowCount must be positive");
        }
    }
}
