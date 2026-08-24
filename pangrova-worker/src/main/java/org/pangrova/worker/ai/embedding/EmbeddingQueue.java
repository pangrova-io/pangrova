package org.pangrova.worker.ai.embedding;

import java.util.List;

/**
 * Abstraction over the pending embedding queue for async drain operations.
 *
 * <p>The coordinator manages the queue state; the worker drainer pulls batches
 * from this interface, processes them, and commits results back. Implementations
 * typically delegate to the coordinator via RPC.</p>
 *
 * <p>Thread safety: implementations must be safe for concurrent use from
 * the drainer's scheduled thread.</p>
 */
public interface EmbeddingQueue {

    /**
     * Polls a batch of pending rows ready for embedding.
     *
     * <p>Returns up to {@code maxBatchSize} rows that have not exceeded the
     * maximum retry count. Rows returned are considered "in-flight" until
     * committed or re-enqueued via {@link #reEnqueue(List)}.</p>
     *
     * @param maxBatchSize maximum number of rows to return
     * @return list of pending rows (may be empty if queue is drained)
     */
    List<PendingEmbeddingRow> poll(int maxBatchSize);

    /**
     * Commits successfully computed embeddings for a batch of rows.
     *
     * <p>After this call, the rows are removed from the pending queue and
     * their embeddings are written to the table.</p>
     *
     * @param results the computed embedding results to commit
     */
    void commit(List<EmbeddingResult> results);

    /**
     * Re-enqueues rows that failed embedding with an incremented attempt count.
     *
     * <p>Rows that have reached the maximum retry limit should NOT be passed
     * to this method; they should be reported as permanently failed via
     * {@link #markFailed(List)}.</p>
     *
     * @param rows the rows to re-enqueue for retry
     */
    void reEnqueue(List<PendingEmbeddingRow> rows);

    /**
     * Marks rows as permanently failed (max retries exhausted).
     *
     * <p>These rows are removed from the active queue and recorded as failed
     * for observability (visible in system.embedding_queue with FAILED status).</p>
     *
     * @param rows the rows that exhausted all retry attempts
     */
    void markFailed(List<PendingEmbeddingRow> rows);
}
