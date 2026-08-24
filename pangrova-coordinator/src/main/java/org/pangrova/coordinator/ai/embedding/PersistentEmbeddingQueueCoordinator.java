package org.pangrova.coordinator.ai.embedding;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Persistent embedding queue coordinator that delegates work item storage to
 * {@link EmbeddingWorkStore} for crash-recovery.
 *
 * <p>On enqueue, items are saved to the work store with PENDING status before
 * being added to the in-memory queue. On coordinator restart, unfinished items
 * are recovered from the work store via {@link #recover()}.</p>
 *
 * <p>Exposes backpressure metrics: queue depth, enqueue rate, drain rate,
 * and backpressure activation count.</p>
 *
 * <p>Thread safety: all operations are safe for concurrent use.</p>
 */
@ThreadSafe
public final class PersistentEmbeddingQueueCoordinator {

    private static final Logger LOG = LoggerFactory.getLogger(PersistentEmbeddingQueueCoordinator.class);

    static final long DEFAULT_BACKPRESSURE_THRESHOLD = 100_000L;

    private final long backpressureThreshold;
    private final EmbeddingWorkStore workStore;
    private final EmbeddingQueueCoordinator delegate;

    // Backpressure metrics
    private final AtomicLong totalEnqueued = new AtomicLong(0);
    private final AtomicLong totalDrained = new AtomicLong(0);
    private final AtomicLong backpressureActivations = new AtomicLong(0);
    private final AtomicLong heapBytesEstimate = new AtomicLong(0);

    /** Estimated average row overhead in bytes (text + metadata). */
    private static final long ROW_OVERHEAD_BYTES = 512;

    /** Maximum aggregate heap for the queue before throttling drain. */
    private final long maxHeapBytes;

    public PersistentEmbeddingQueueCoordinator(
            EmbeddingWorkStore workStore) {
        this(workStore, DEFAULT_BACKPRESSURE_THRESHOLD, 256L * 1024 * 1024);
    }

    /** Full constructor with configurable backpressure threshold and heap budget. */
    public PersistentEmbeddingQueueCoordinator(
            EmbeddingWorkStore workStore,
            long backpressureThreshold,
            long maxHeapBytes) {
        this.workStore = Objects.requireNonNull(workStore, "workStore required");
        if (backpressureThreshold <= 0) {
            throw new IllegalArgumentException("backpressureThreshold must be positive");
        }
        if (maxHeapBytes <= 0) {
            throw new IllegalArgumentException("maxHeapBytes must be positive");
        }
        this.backpressureThreshold = backpressureThreshold;
        this.maxHeapBytes = maxHeapBytes;
        this.delegate = new EmbeddingQueueCoordinator(backpressureThreshold);
    }

    /**
     * Enqueues a row with persistent write-ahead to the work store.
     */
    public void enqueue(String tableName, String rowId, String modelName,
                        String vectorColumn, String sourceText) {
        if (heapBytesEstimate.get() >= maxHeapBytes) {
            backpressureActivations.incrementAndGet();
            throw new EmbeddingQueueFullException(
                    tableName, delegate.queueDepth(tableName), backpressureThreshold);
        }

        // Persist first (write-ahead), then enqueue in memory
        PendingEmbeddingRow row = new PendingEmbeddingRow(
                tableName, rowId, modelName, vectorColumn, sourceText, Instant.now());
        workStore.save(row);
        delegate.enqueue(tableName, rowId, modelName, vectorColumn, sourceText);
        totalEnqueued.incrementAndGet();
        heapBytesEstimate.addAndGet(ROW_OVERHEAD_BYTES);
    }

    /**
     * Drains up to maxRows entries from the given table's queue.
     */
    public List<PendingEmbeddingRow> drain(String tableName, int maxRows) {
        List<PendingEmbeddingRow> drained = delegate.drain(tableName, maxRows);
        if (!drained.isEmpty()) {
            workStore.markInProgress(drained);
            totalDrained.addAndGet(drained.size());
            heapBytesEstimate.addAndGet(-drained.size() * ROW_OVERHEAD_BYTES);
        }
        return drained;
    }

    /**
     * Marks a row as completed in the persistent store.
     */
    public void markCompleted(String tableName, String rowId) {
        workStore.markCompleted(tableName, rowId);
    }

    /**
     * Recovers unfinished work items from the persistent store on startup.
     *
     * @return number of items recovered
     */
    public int recover() {
        List<PendingEmbeddingRow> unfinished = workStore.loadUnfinished();
        int recovered = 0;
        for (PendingEmbeddingRow row : unfinished) {
            try {
                delegate.enqueue(row.tableName(), row.rowId(), row.modelName(),
                        row.vectorColumn(), row.sourceText());
                heapBytesEstimate.addAndGet(ROW_OVERHEAD_BYTES);
                recovered++;
            } catch (EmbeddingQueueFullException e) {
                LOG.warn("event=recovery_backpressure table={} recovered={} remaining={}",
                        row.tableName(), recovered, unfinished.size() - recovered);
                break;
            }
        }
        if (recovered > 0) {
            LOG.info("event=embedding_queue_recovered count={}", recovered);
        }
        return recovered;
    }

    /**
     * Returns current queue depth for a table.
     */
    public long queueDepth(String tableName) {
        return delegate.queueDepth(tableName);
    }

    /**
     * Returns queue status for all tables.
     */
    public List<EmbeddingQueueStatus> queryQueueStatus() {
        return delegate.queryQueueStatus();
    }

    // -- Backpressure metrics --

    public long totalEnqueued() {
        return totalEnqueued.get();
    }

    public long totalDrained() {
        return totalDrained.get();
    }

    public long backpressureActivations() {
        return backpressureActivations.get();
    }

    public long estimatedHeapBytes() {
        return heapBytesEstimate.get();
    }

    public long maxHeapBytes() {
        return maxHeapBytes;
    }
}
