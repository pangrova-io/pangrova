package org.pangrova.coordinator.ai.embedding;

/**
 * Immutable result of a single backfill batch dispatched to a Worker.
 *
 * @param processedRows rows successfully embedded and committed in this batch
 * @param failedRows    rows skipped due to embedding failure (not retried in same run)
 */
public record BackfillBatchResult(long processedRows, long failedRows) {

    public BackfillBatchResult {
        if (processedRows < 0) {
            throw new IllegalArgumentException("processedRows must be non-negative");
        }
        if (failedRows < 0) {
            throw new IllegalArgumentException("failedRows must be non-negative");
        }
    }
}
