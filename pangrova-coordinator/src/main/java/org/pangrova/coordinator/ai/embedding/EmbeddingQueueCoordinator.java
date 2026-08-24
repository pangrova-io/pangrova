package org.pangrova.coordinator.ai.embedding;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manages the ASYNC embedding queue for background vector computation.
 *
 * <p>When a table uses EMBEDDING MODE ASYNC, rows are committed with NULL embedding
 * vectors and enqueued here for background processing by the
 * {@code AsyncEmbeddingDrainer} in the worker layer.</p>
 *
 * <p>Responsibilities:</p>
 * <ul>
 *   <li>Enqueue rows for background embedding computation</li>
 *   <li>Expose queue status via {@code system.embedding_queue} system table</li>
 *   <li>Enforce backpressure: block new ASYNC INSERT when queue exceeds 100,000 rows per table</li>
 * </ul>
 *
 * <p>Thread safety: all operations are safe for concurrent use from multiple coordinator threads.</p>
 */
@ThreadSafe
public final class EmbeddingQueueCoordinator {

    private static final Logger LOG = LoggerFactory.getLogger(EmbeddingQueueCoordinator.class);

    /** Maximum pending rows per table before backpressure blocks new inserts. */
    static final long DEFAULT_BACKPRESSURE_THRESHOLD = 100_000L;

    private final long backpressureThreshold;
    private final ConcurrentHashMap<String, TableQueue> tableQueues = new ConcurrentHashMap<>();

    /**
     * Creates a coordinator with the default backpressure threshold (100,000 rows per table).
     */
    public EmbeddingQueueCoordinator() {
        this(DEFAULT_BACKPRESSURE_THRESHOLD);
    }

    /**
     * Creates a coordinator with a custom backpressure threshold.
     *
     * @param backpressureThreshold maximum pending rows per table before blocking (must be positive)
     */
    public EmbeddingQueueCoordinator(long backpressureThreshold) {
        if (backpressureThreshold <= 0) {
            throw new IllegalArgumentException(
                    "backpressureThreshold must be positive, got: " + backpressureThreshold);
        }
        this.backpressureThreshold = backpressureThreshold;
    }

    /**
     * Enqueues a row for background embedding computation.
     *
     * <p>If the queue for the given table exceeds the backpressure threshold,
     * this method throws {@link EmbeddingQueueFullException} to signal that
     * the caller should block or reject the INSERT.</p>
     *
     * @param tableName    fully qualified table name (catalog.schema.table)
     * @param rowId        unique identifier for the row (primary key or row offset)
     * @param modelName    the embedding model to use
     * @param vectorColumn the target vector column name
     * @param sourceText   concatenated source column text for embedding
     * @throws EmbeddingQueueFullException if the table's queue exceeds the backpressure threshold
     */
    public void enqueue(String tableName, String rowId, String modelName,
                        String vectorColumn, String sourceText) {
        Objects.requireNonNull(tableName, "tableName must not be null");
        Objects.requireNonNull(rowId, "rowId must not be null");
        Objects.requireNonNull(modelName, "modelName must not be null");
        Objects.requireNonNull(vectorColumn, "vectorColumn must not be null");
        Objects.requireNonNull(sourceText, "sourceText must not be null");

        TableQueue queue = tableQueues.computeIfAbsent(tableName, TableQueue::new);

        if (queue.size() >= backpressureThreshold) {
            LOG.warn("event=embedding_queue_backpressure table={} depth={} threshold={}",
                    tableName, queue.size(), backpressureThreshold);
            throw new EmbeddingQueueFullException(tableName, queue.size(), backpressureThreshold);
        }

        PendingEmbeddingRow entry = new PendingEmbeddingRow(
                tableName, rowId, modelName, vectorColumn, sourceText, Instant.now());
        queue.add(entry);

        LOG.debug("event=embedding_enqueued table={} rowId={} model={} column={} depth={}",
                tableName, rowId, modelName, vectorColumn, queue.size());
    }

    /**
     * Drains up to {@code maxRows} entries from the specified table's queue.
     *
     * <p>Used by the {@code AsyncEmbeddingDrainer} to fetch work items for
     * background processing.</p>
     *
     * @param tableName fully qualified table name
     * @param maxRows   maximum number of rows to drain (must be positive)
     * @return list of pending rows (may be empty if queue is empty), never null
     */
    public List<PendingEmbeddingRow> drain(String tableName, int maxRows) {
        Objects.requireNonNull(tableName, "tableName must not be null");
        if (maxRows <= 0) {
            throw new IllegalArgumentException("maxRows must be positive, got: " + maxRows);
        }

        TableQueue queue = tableQueues.get(tableName);
        if (queue == null) {
            return List.of();
        }

        List<PendingEmbeddingRow> batch = new ArrayList<>(Math.min(maxRows, 64));
        for (int i = 0; i < maxRows; i++) {
            PendingEmbeddingRow entry = queue.poll();
            if (entry == null) {
                break;
            }
            batch.add(entry);
        }

        if (!batch.isEmpty()) {
            LOG.debug("event=embedding_drained table={} count={} remaining={}",
                    tableName, batch.size(), queue.size());
        }
        return Collections.unmodifiableList(batch);
    }

    /**
     * Returns the current queue depth for a specific table.
     *
     * @param tableName fully qualified table name
     * @return number of pending rows (0 if no queue exists for this table)
     */
    public long queueDepth(String tableName) {
        Objects.requireNonNull(tableName, "tableName must not be null");
        TableQueue queue = tableQueues.get(tableName);
        return queue != null ? queue.size() : 0;
    }

    /**
     * Checks whether the queue for a table has reached the backpressure threshold.
     *
     * @param tableName fully qualified table name
     * @return true if the queue depth equals or exceeds the threshold
     */
    public boolean isBackpressureActive(String tableName) {
        return queueDepth(tableName) >= backpressureThreshold;
    }

    /**
     * Returns queue status for all tables, suitable for the {@code system.embedding_queue}
     * system table.
     *
     * @return unmodifiable list of queue status entries, one per table with pending rows
     */
    public List<EmbeddingQueueStatus> queryQueueStatus() {
        List<EmbeddingQueueStatus> statuses = new ArrayList<>();
        for (Map.Entry<String, TableQueue> entry : tableQueues.entrySet()) {
            TableQueue queue = entry.getValue();
            long depth = queue.size();
            if (depth > 0) {
                Instant oldestEnqueueTime = queue.oldestEnqueueTime();
                statuses.add(new EmbeddingQueueStatus(
                        entry.getKey(),
                        depth,
                        oldestEnqueueTime,
                        depth >= backpressureThreshold));
            }
        }
        return Collections.unmodifiableList(statuses);
    }

    /**
     * Returns the configured backpressure threshold.
     *
     * @return maximum pending rows per table before blocking
     */
    public long backpressureThreshold() {
        return backpressureThreshold;
    }

    /**
     * Per-table queue backed by a ConcurrentLinkedQueue with an atomic size counter
     * for O(1) size queries (ConcurrentLinkedQueue.size() is O(n)).
     */
    private static final class TableQueue {
        private final ConcurrentLinkedQueue<PendingEmbeddingRow> entries = new ConcurrentLinkedQueue<>();
        private final AtomicLong count = new AtomicLong(0);

        TableQueue(String tableName) {
            // tableName used as ConcurrentHashMap key; no need to store here
        }

        void add(PendingEmbeddingRow entry) {
            entries.add(entry);
            count.incrementAndGet();
        }

        PendingEmbeddingRow poll() {
            PendingEmbeddingRow entry = entries.poll();
            if (entry != null) {
                count.decrementAndGet();
            }
            return entry;
        }

        long size() {
            return count.get();
        }

        Instant oldestEnqueueTime() {
            PendingEmbeddingRow head = entries.peek();
            return head != null ? head.enqueuedAt() : null;
        }
    }
}
