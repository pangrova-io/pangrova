package org.pangrova.coordinator.ai.embedding;

/**
 * Dispatches a backfill partition to a Worker for embedding execution.
 *
 * <p>The dispatcher selects an available Worker, sends the partition descriptor,
 * and waits for the batch result. Failed rows within the batch are counted but
 * not retried in the same backfill run (skip-failed-rows semantics).</p>
 *
 * <p>Rate limiting is handled by the caller (BackfillCoordinator) via concurrency
 * semaphore; this interface focuses on single-partition dispatch.</p>
 */
public interface BackfillWorkerDispatcher {

    /**
     * Dispatches a partition to a Worker for embedding and returns the result.
     *
     * <p>The Worker embeds all rows in the partition using the specified model,
     * commits results incrementally, and reports success/failure counts.</p>
     *
     * @param tableName  the fully qualified table name
     * @param columnName the embedding column
     * @param modelName  the embedding model to use
     * @param partition  the partition descriptor (row range)
     * @param snapshotId the snapshot for isolation (Worker reads at this snapshot)
     * @return the batch result with processed/failed row counts
     */
    BackfillBatchResult dispatch(String tableName, String columnName,
                                String modelName, BackfillPartition partition,
                                long snapshotId);
}
