package org.pangrova.coordinator.ai.embedding;

import java.time.Instant;

/**
 * Snapshot of queue status for a single table, exposed via {@code system.embedding_queue}.
 *
 * @param tableName          fully qualified table name
 * @param pendingRows        number of rows awaiting embedding computation
 * @param oldestEnqueueTime  timestamp of the oldest pending row (null if queue is empty)
 * @param backpressureActive true if the queue depth has reached the backpressure threshold
 */
public record EmbeddingQueueStatus(
        String tableName,
        long pendingRows,
        Instant oldestEnqueueTime,
        boolean backpressureActive) {

    public EmbeddingQueueStatus {
        if (tableName == null || tableName.isBlank()) {
            throw new IllegalArgumentException("tableName required");
        }
        if (pendingRows < 0) {
            throw new IllegalArgumentException("pendingRows must be non-negative");
        }
    }
}
