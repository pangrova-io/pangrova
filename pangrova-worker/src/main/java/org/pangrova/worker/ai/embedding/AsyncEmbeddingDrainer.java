package org.pangrova.worker.ai.embedding;

import org.pangrova.common.concurrent.BackoffStrategy;
import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.spi.ai.embedding.EmbeddingProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Background task that drains the async embedding queue in batches.
 *
 * <p>Polls pending rows from the {@link EmbeddingQueue}, groups them by model,
 * calls {@link EmbeddingProvider#embedBatch(List)} for each group, and commits
 * results back. Failed rows are retried with exponential backoff up to
 * {@link #MAX_RETRIES} attempts per row.</p>
 *
 * <p>Concurrency strategy: single scheduled thread executes drain cycles.
 * {@code AtomicBoolean running} guards lifecycle transitions. The drainer
 * is the exclusive owner of its {@link ScheduledExecutorService}.</p>
 *
 * @see EmbeddingQueue
 * @see PendingEmbeddingRow
 */
public final class AsyncEmbeddingDrainer implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncEmbeddingDrainer.class);

    /** Maximum retry attempts per row before marking as permanently failed. */
    static final int MAX_RETRIES = 3;

    /** Default batch size for polling the queue. */
    static final int DEFAULT_BATCH_SIZE = 64;

    /** Initial backoff delay in milliseconds between drain cycles when queue is empty. */
    static final long INITIAL_BACKOFF_MS = 100;

    /** Maximum backoff delay in milliseconds between drain cycles when queue is empty. */
    static final long MAX_BACKOFF_MS = 5000;

    /** Fixed delay between drain cycles when queue has items (milliseconds). */
    static final long DRAIN_INTERVAL_MS = 50;

    private final EmbeddingQueue queue;
    private final EmbeddingProviderLookup providerLookup;
    private final int batchSize;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final BackoffStrategy emptyQueueBackoff;

    private volatile ScheduledFuture<?> scheduledTask;

    /**
     * Creates a drainer with default batch size (64).
     *
     * @param queue          the embedding queue to drain
     * @param providerLookup resolves model names to embedding providers
     */
    public AsyncEmbeddingDrainer(EmbeddingQueue queue, EmbeddingProviderLookup providerLookup) {
        this(queue, providerLookup, DEFAULT_BATCH_SIZE);
    }

    /**
     * Creates a drainer with a custom batch size.
     *
     * @param queue          the embedding queue to drain
     * @param providerLookup resolves model names to embedding providers
     * @param batchSize      maximum rows per drain cycle (must be positive)
     */
    public AsyncEmbeddingDrainer(EmbeddingQueue queue,
                                 EmbeddingProviderLookup providerLookup,
                                 int batchSize) {
        if (queue == null) {
            throw new IllegalArgumentException("queue must not be null");
        }
        if (providerLookup == null) {
            throw new IllegalArgumentException("providerLookup must not be null");
        }
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be positive, got: " + batchSize);
        }
        this.queue = queue;
        this.providerLookup = providerLookup;
        this.batchSize = batchSize;
        this.scheduler = ManagedExecutorFactory.newSingleThreadScheduledExecutor(
                "async-embedding-drainer");
        this.emptyQueueBackoff = BackoffStrategy.poll(MAX_BACKOFF_MS);
    }

    /**
     * Starts the background drain loop.
     *
     * <p>Idempotent: calling start on an already-running drainer has no effect.</p>
     */
    public void start() {
        if (running.compareAndSet(false, true)) {
            LOG.info("event=async_embedding_drainer_started batchSize={}", batchSize);
            scheduleNextDrain(DRAIN_INTERVAL_MS);
        }
    }

    /**
     * Stops the drain loop and shuts down the scheduler.
     *
     * <p>Idempotent: calling close on an already-stopped drainer has no effect.
     * In-flight drain cycles complete before shutdown.</p>
     */
    @Override
    public void close() {
        if (running.compareAndSet(true, false)) {
            LOG.info("event=async_embedding_drainer_stopping");
            ScheduledFuture<?> task = scheduledTask;
            if (task != null) {
                task.cancel(false);
            }
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
            LOG.info("event=async_embedding_drainer_stopped");
        }
    }

    /**
     * Returns whether the drainer is currently running.
     *
     * @return true if the drain loop is active
     */
    public boolean isRunning() {
        return running.get();
    }

    /**
     * Executes a single drain cycle. Visible for testing.
     */
    void drainOnce() {
        List<PendingEmbeddingRow> batch = queue.poll(batchSize);
        if (batch.isEmpty()) {
            long backoffMs = emptyQueueBackoff.next();
            LOG.debug("event=async_drain_empty backoffMs={}", backoffMs);
            scheduleNextDrain(backoffMs);
            return;
        }

        emptyQueueBackoff.reset();
        LOG.debug("event=async_drain_batch size={}", batch.size());

        List<EmbeddingResult> successes = new ArrayList<>();
        List<PendingEmbeddingRow> retryable = new ArrayList<>();
        List<PendingEmbeddingRow> exhausted = new ArrayList<>();

        processBatch(batch, successes, retryable, exhausted);

        if (!successes.isEmpty()) {
            queue.commit(successes);
            LOG.info("event=async_drain_committed count={}", successes.size());
        }
        if (!retryable.isEmpty()) {
            queue.reEnqueue(retryable);
            LOG.warn("event=async_drain_retry count={}", retryable.size());
        }
        if (!exhausted.isEmpty()) {
            queue.markFailed(exhausted);
            LOG.error("event=async_drain_failed count={}", exhausted.size());
        }

        scheduleNextDrain(DRAIN_INTERVAL_MS);
    }

    private void processBatch(List<PendingEmbeddingRow> batch,
                              List<EmbeddingResult> successes,
                              List<PendingEmbeddingRow> retryable,
                              List<PendingEmbeddingRow> exhausted) {
        Map<String, List<PendingEmbeddingRow>> byModel = groupByModel(batch);

        for (Map.Entry<String, List<PendingEmbeddingRow>> entry : byModel.entrySet()) {
            String modelName = entry.getKey();
            List<PendingEmbeddingRow> modelRows = entry.getValue();

            EmbeddingProvider provider = providerLookup.resolve(modelName);
            if (provider == null) {
                LOG.error("event=async_drain_provider_missing model={} rowCount={}",
                        modelName, modelRows.size());
                classifyFailedRows(modelRows, retryable, exhausted);
                continue;
            }

            embedModelBatch(provider, modelRows, successes, retryable, exhausted);
        }
    }

    private void embedModelBatch(EmbeddingProvider provider,
                                 List<PendingEmbeddingRow> rows,
                                 List<EmbeddingResult> successes,
                                 List<PendingEmbeddingRow> retryable,
                                 List<PendingEmbeddingRow> exhausted) {
        int effectiveBatchSize = Math.min(batchSize, provider.maxBatchSize());

        for (int offset = 0; offset < rows.size(); offset += effectiveBatchSize) {
            int end = Math.min(offset + effectiveBatchSize, rows.size());
            List<PendingEmbeddingRow> chunk = rows.subList(offset, end);

            List<String> texts = buildTexts(chunk);
            try {
                List<float[]> embeddings = provider.embedBatch(texts);
                for (int i = 0; i < chunk.size(); i++) {
                    PendingEmbeddingRow row = chunk.get(i);
                    successes.add(new EmbeddingResult(
                            row.rowId(), row.tableName(), row.vectorColumn(), embeddings.get(i)));
                }
            } catch (Exception e) {
                LOG.warn("event=async_drain_batch_failed model={} chunkSize={} error={}",
                        provider.modelName(), chunk.size(), e.getMessage());
                classifyFailedRows(chunk, retryable, exhausted);
            }
        }
    }

    private List<String> buildTexts(List<PendingEmbeddingRow> rows) {
        List<String> texts = new ArrayList<>(rows.size());
        for (PendingEmbeddingRow row : rows) {
            texts.add(concatenateSourceColumns(row.sourceColumns(), row.sourceValues()));
        }
        return texts;
    }

    private String concatenateSourceColumns(List<String> sourceColumns, Map<String, String> values) {
        return EmbeddingTextConcatenator.concatenate(sourceColumns, values);
    }

    private void classifyFailedRows(List<PendingEmbeddingRow> rows,
                                    List<PendingEmbeddingRow> retryable,
                                    List<PendingEmbeddingRow> exhausted) {
        for (PendingEmbeddingRow row : rows) {
            if (row.attemptCount() + 1 >= MAX_RETRIES) {
                exhausted.add(row);
            } else {
                retryable.add(incrementAttempt(row));
            }
        }
    }

    private PendingEmbeddingRow incrementAttempt(PendingEmbeddingRow row) {
        return new PendingEmbeddingRow(
                row.rowId(), row.tableName(), row.vectorColumn(),
                row.modelName(), row.sourceColumns(), row.sourceValues(),
                row.attemptCount() + 1);
    }

    private Map<String, List<PendingEmbeddingRow>> groupByModel(List<PendingEmbeddingRow> rows) {
        Map<String, List<PendingEmbeddingRow>> grouped = new LinkedHashMap<>();
        for (PendingEmbeddingRow row : rows) {
            grouped.computeIfAbsent(row.modelName(), k -> new ArrayList<>()).add(row);
        }
        return grouped;
    }

    private void scheduleNextDrain(long delayMs) {
        if (running.get()) {
            scheduledTask = scheduler.schedule(this::drainOnce, delayMs, TimeUnit.MILLISECONDS);
        }
    }
}
