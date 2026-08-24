package org.pangrova.coordinator.ai.embedding;

import java.time.Instant;

/**
 * Immutable checkpoint representing the progress of a backfill operation.
 *
 * <p>Persisted after each batch commit to enable resume-from-last-committed-batch
 * on crash recovery. The partition index and batch offset together identify the
 * exact position within the work queue.</p>
 *
 * @param backfillId      unique identifier for the backfill operation
 * @param tableName       the table being backfilled
 * @param columnName      the column being backfilled
 * @param snapshotId      the snapshot used for isolation
 * @param partitionIndex  the last fully committed partition index
 * @param batchOffset     the offset within the current partition (rows committed so far)
 * @param processedRows   cumulative rows successfully processed
 * @param failedRows      cumulative rows skipped due to failure
 * @param createdAt       when this checkpoint was created
 */
public record BackfillCheckpoint(
        String backfillId,
        String tableName,
        String columnName,
        long snapshotId,
        int partitionIndex,
        long batchOffset,
        long processedRows,
        long failedRows,
        Instant createdAt) {

    public BackfillCheckpoint {
        backfillId = requireNotBlank(backfillId, "backfillId");
        tableName = requireNotBlank(tableName, "tableName");
        columnName = requireNotBlank(columnName, "columnName");
        snapshotId = requireNonNegative(snapshotId, "snapshotId");
        partitionIndex = requireNonNegative(partitionIndex, "partitionIndex");
        batchOffset = requireNonNegative(batchOffset, "batchOffset");
        processedRows = requireNonNegative(processedRows, "processedRows");
        failedRows = requireNonNegative(failedRows, "failedRows");
        if (createdAt == null) {
            throw new IllegalArgumentException("createdAt required");
        }
    }

    private static String requireNotBlank(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " required");
        }
        return value;
    }

    private static long requireNonNegative(long value, String fieldName) {
        if (value < 0) {
            throw new IllegalArgumentException(fieldName + " must be non-negative");
        }
        return value;
    }

    private static int requireNonNegative(int value, String fieldName) {
        if (value < 0) {
            throw new IllegalArgumentException(fieldName + " must be non-negative");
        }
        return value;
    }
}
