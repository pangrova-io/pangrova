package org.pangrova.coordinator.ai.embedding;

import java.util.List;

/**
 * Persistence store for embedding work items.
 *
 * <p>Enables crash-recovery: on enqueue, work items are persisted with status PENDING.
 * On drain (processing start), items transition to IN_PROGRESS. On successful embedding
 * generation, items are marked COMPLETED. On coordinator restart/failover, all PENDING
 * and IN_PROGRESS items are loaded back into the heap queue for reprocessing.</p>
 *
 * <p>Thread safety: implementations must be safe for concurrent use from multiple threads.</p>
 */
public interface EmbeddingWorkStore {

    /**
     * Status of an embedding work item in the persistence layer.
     */
    enum WorkItemStatus {
        /** Enqueued but not yet picked up for processing. */
        PENDING,
        /** Currently being processed by a worker. */
        IN_PROGRESS,
        /** Successfully completed — embedding generated and written. */
        COMPLETED
    }

    /**
     * Persists a new work item with PENDING status.
     *
     * @param row the pending embedding row to persist
     */
    void save(PendingEmbeddingRow row);

    /**
     * Transitions items from PENDING to IN_PROGRESS.
     *
     * <p>Called when items are drained from the queue for processing.</p>
     *
     * @param rows the rows being processed
     */
    void markInProgress(List<PendingEmbeddingRow> rows);

    /**
     * Marks a single item as COMPLETED and removes it from the active work set.
     *
     * <p>Called after the embedding has been successfully generated and written.</p>
     *
     * @param tableName the table name
     * @param rowId     the row identifier
     */
    void markCompleted(String tableName, String rowId);

    /**
     * Loads all non-completed (PENDING or IN_PROGRESS) work items.
     *
     * <p>Used on coordinator startup/failover to recover the work queue.
     * IN_PROGRESS items are treated as abandoned (coordinator crashed mid-processing)
     * and should be reprocessed.</p>
     *
     * @return list of all recoverable work items, never null
     */
    List<PendingEmbeddingRow> loadUnfinished();

    /**
     * Returns the count of unfinished items for a specific table.
     *
     * @param tableName the table name
     * @return count of PENDING + IN_PROGRESS items for this table
     */
    long countUnfinished(String tableName);
}
