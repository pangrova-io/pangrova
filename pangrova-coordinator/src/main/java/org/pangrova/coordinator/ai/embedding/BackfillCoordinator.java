package org.pangrova.coordinator.ai.embedding;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Distributed batch processing coordinator for embedding backfill operations.
 *
 * <p>Scans rows matching a filter (typically NULL embedding columns) at a fixed snapshot,
 * partitions work across available Workers, dispatches embedding tasks, and commits
 * results incrementally. Supports checkpoint-based recovery, rate limiting, cancellation,
 * and skip-failed-rows semantics.</p>
 *
 * <p>Snapshot isolation: only processes rows visible at backfill start snapshot.
 * Rows inserted after backfill start are handled by next invocation or ASYNC drain.</p>
 *
 * <p>Delivery semantics: at-least-once. On Worker crash after embed but before commit,
 * rows may be re-embedded on recovery (idempotent result due to deterministic embedding).</p>
 *
 * <p>Thread safety: implementations must be safe for concurrent use.</p>
 */
public interface BackfillCoordinator {

    /**
     * Starts a new backfill operation for the specified table and column.
     *
     * <p>Takes a snapshot at invocation time and only processes rows visible at that snapshot.
     * If a previous backfill was interrupted, resumes from the last committed checkpoint.</p>
     *
     * @param request the backfill configuration
     * @return a future that completes with the backfill result summary
     * @throws BackfillAlreadyRunningException if a backfill is already active for this table/column
     */
    CompletableFuture<BackfillResult> start(BackfillRequest request);

    /**
     * Cancels an in-progress backfill operation.
     *
     * <p>In-flight Worker tasks are allowed to complete; no new partitions are dispatched.
     * Already committed batches are retained.</p>
     *
     * @param tableName  the table name
     * @param columnName the column being backfilled
     * @return true if a running backfill was cancelled, false if none was active
     */
    boolean cancel(String tableName, String columnName);

    /**
     * Returns the current status of a backfill operation.
     *
     * @param tableName the table name
     * @return the status if a backfill exists (running or completed), empty otherwise
     */
    Optional<BackfillStatus> getStatus(String tableName);

    /**
     * Immutable request descriptor for a backfill operation.
     *
     * @param tableName   fully qualified table name
     * @param columnName  the embedding column to backfill
     * @param modelName   the embedding model to use
     * @param filter      optional WHERE clause filter (null means all NULL rows)
     * @param batchSize   rows per Worker batch (default 1000)
     * @param concurrency number of parallel Worker tasks (default from config)
     */
    record BackfillRequest(
            String tableName,
            String columnName,
            String modelName,
            String filter,
            int batchSize,
            int concurrency) {

        public BackfillRequest {
            tableName = requireNotBlank(tableName, "tableName");
            columnName = requireNotBlank(columnName, "columnName");
            modelName = requireNotBlank(modelName, "modelName");
            if (batchSize <= 0) {
                throw new IllegalArgumentException("batchSize must be positive");
            }
            if (concurrency <= 0) {
                throw new IllegalArgumentException("concurrency must be positive");
            }
        }

        private static String requireNotBlank(String value, String fieldName) {
            if (value == null || value.isBlank()) {
                throw new IllegalArgumentException(fieldName + " required");
            }
            return value;
        }
    }

    /**
     * Summary result of a completed or cancelled backfill operation.
     *
     * @param tableName      the table that was backfilled
     * @param columnName     the column that was backfilled
     * @param totalRows      total rows scanned at snapshot
     * @param processedRows  rows successfully embedded and committed
     * @param failedRows     rows skipped due to embedding failure
     * @param elapsedTime    wall-clock duration of the backfill
     * @param cancelled      true if the backfill was cancelled before completion
     * @param snapshotId     the snapshot used for isolation
     */
    record BackfillResult(
            String tableName,
            String columnName,
            long totalRows,
            long processedRows,
            long failedRows,
            Duration elapsedTime,
            boolean cancelled,
            long snapshotId) {}

    /**
     * Current status of a backfill operation.
     *
     * @param tableName      the table being backfilled
     * @param columnName     the column being backfilled
     * @param state          current lifecycle state
     * @param totalRows      total rows to process (from snapshot scan)
     * @param processedRows  rows committed so far
     * @param failedRows     rows skipped due to failure
     * @param startTime      when the backfill started
     * @param lastCheckpoint the last committed checkpoint identifier
     * @param snapshotId     the snapshot used for isolation
     */
    record BackfillStatus(
            String tableName,
            String columnName,
            BackfillState state,
            long totalRows,
            long processedRows,
            long failedRows,
            Instant startTime,
            String lastCheckpoint,
            long snapshotId) {}

    /**
     * Lifecycle states of a backfill operation.
     */
    enum BackfillState {
        /** Scanning rows and partitioning work. */
        SCANNING,
        /** Dispatching partitions to Workers. */
        RUNNING,
        /** Cancellation requested; draining in-flight tasks. */
        CANCELLING,
        /** All partitions processed or cancelled. */
        COMPLETED,
        /** Backfill failed with unrecoverable error. */
        FAILED
    }
}
