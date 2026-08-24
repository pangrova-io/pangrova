package org.pangrova.worker.ai.embedding;

import org.pangrova.spi.ai.embedding.BlobReference;
import org.pangrova.spi.ai.embedding.EmbeddingProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies AsyncEmbeddingDrainer: batch processing, retry classification,
 * exponential backoff on empty queue, and lifecycle management.
 */
class AsyncEmbeddingDrainerTest {

    private AsyncEmbeddingDrainer drainer;

    @AfterEach
    void tearDown() {
        if (drainer != null) {
            drainer.close();
        }
    }

    @Test
    void drainOnceCommitsSuccessfulEmbeddings() {
        StubEmbeddingProvider provider = new StubEmbeddingProvider("model-a", 3, 64);
        EmbeddingProviderLookup lookup = name -> "model-a".equals(name) ? provider : null;

        List<PendingEmbeddingRow> pending = List.of(
                row("r1", "table1", "vec", "model-a", List.of("content"), Map.of("content", "hello"), 0),
                row("r2", "table1", "vec", "model-a", List.of("content"), Map.of("content", "world"), 0));

        RecordingQueue queue = new RecordingQueue(pending);
        drainer = new AsyncEmbeddingDrainer(queue, lookup);

        drainer.drainOnce();

        assertThat(queue.committed).hasSize(2);
        assertThat(queue.committed.get(0).rowId()).isEqualTo("r1");
        assertThat(queue.committed.get(1).rowId()).isEqualTo("r2");
        assertThat(queue.committed.get(0).embedding()).hasSize(3);
    }

    @Test
    void drainOnceRetriesFailedRowsBelowMaxRetries() {
        FailingEmbeddingProvider provider = new FailingEmbeddingProvider("model-a", 3);
        EmbeddingProviderLookup lookup = name -> "model-a".equals(name) ? provider : null;

        List<PendingEmbeddingRow> pending = List.of(
                row("r1", "table1", "vec", "model-a", List.of("content"), Map.of("content", "hello"), 0),
                row("r2", "table1", "vec", "model-a", List.of("content"), Map.of("content", "world"), 1));

        RecordingQueue queue = new RecordingQueue(pending);
        drainer = new AsyncEmbeddingDrainer(queue, lookup);

        drainer.drainOnce();

        assertThat(queue.committed).isEmpty();
        assertThat(queue.reEnqueued).hasSize(2);
        assertThat(queue.reEnqueued.get(0).rowId()).isEqualTo("r1");
        assertThat(queue.reEnqueued.get(0).attemptCount()).isEqualTo(1);
        assertThat(queue.reEnqueued.get(1).rowId()).isEqualTo("r2");
        assertThat(queue.reEnqueued.get(1).attemptCount()).isEqualTo(2);
        assertThat(queue.failed).isEmpty();
    }

    @Test
    void drainOnceMarksExhaustedRowsAsFailed() {
        FailingEmbeddingProvider provider = new FailingEmbeddingProvider("model-a", 3);
        EmbeddingProviderLookup lookup = name -> "model-a".equals(name) ? provider : null;

        List<PendingEmbeddingRow> pending = List.of(
                row("r1", "table1", "vec", "model-a", List.of("content"), Map.of("content", "hello"), 2));

        RecordingQueue queue = new RecordingQueue(pending);
        drainer = new AsyncEmbeddingDrainer(queue, lookup);

        drainer.drainOnce();

        // attemptCount=2, +1=3, 3 >= MAX_RETRIES(3) → exhausted
        assertThat(queue.committed).isEmpty();
        assertThat(queue.reEnqueued).isEmpty();
        assertThat(queue.failed).hasSize(1);
        assertThat(queue.failed.get(0).rowId()).isEqualTo("r1");
    }

    @Test
    void drainOnceHandlesMissingProvider() {
        EmbeddingProviderLookup lookup = name -> null;

        List<PendingEmbeddingRow> pending = List.of(
                row("r1", "table1", "vec", "unknown-model", List.of("content"), Map.of("content", "hello"), 0));

        RecordingQueue queue = new RecordingQueue(pending);
        drainer = new AsyncEmbeddingDrainer(queue, lookup);

        drainer.drainOnce();

        // Missing provider → classify as failed (attempt 0+1=1 < 3 → retryable)
        assertThat(queue.reEnqueued).hasSize(1);
        assertThat(queue.committed).isEmpty();
    }

    @Test
    void drainOnceGroupsByModelAndBatchesCorrectly() {
        AtomicInteger batchCallCount = new AtomicInteger();
        StubEmbeddingProvider providerA = new StubEmbeddingProvider("model-a", 3, 2) {
            @Override
            public List<float[]> embedBatch(List<String> texts) {
                batchCallCount.incrementAndGet();
                return super.embedBatch(texts);
            }
        };
        EmbeddingProviderLookup lookup = name -> "model-a".equals(name) ? providerA : null;

        // 3 rows with batch size 2 → should produce 2 batch calls
        List<PendingEmbeddingRow> pending = List.of(
                row("r1", "table1", "vec", "model-a", List.of("content"), Map.of("content", "a"), 0),
                row("r2", "table1", "vec", "model-a", List.of("content"), Map.of("content", "b"), 0),
                row("r3", "table1", "vec", "model-a", List.of("content"), Map.of("content", "c"), 0));

        RecordingQueue queue = new RecordingQueue(pending);
        drainer = new AsyncEmbeddingDrainer(queue, lookup, 2);

        drainer.drainOnce();

        assertThat(queue.committed).hasSize(3);
        assertThat(batchCallCount.get()).isEqualTo(2); // batch of 2 + batch of 1
    }

    @Test
    void drainOnceHandlesEmptyQueue() {
        EmbeddingProviderLookup lookup = name -> null;
        RecordingQueue queue = new RecordingQueue(List.of());
        drainer = new AsyncEmbeddingDrainer(queue, lookup);

        drainer.drainOnce();

        assertThat(queue.committed).isEmpty();
        assertThat(queue.reEnqueued).isEmpty();
        assertThat(queue.failed).isEmpty();
    }

    @Test
    void drainOnceConcatenatesMultipleSourceColumns() {
        List<String> capturedTexts = new CopyOnWriteArrayList<>();
        StubEmbeddingProvider provider = new StubEmbeddingProvider("model-a", 3, 64) {
            @Override
            public List<float[]> embedBatch(List<String> texts) {
                capturedTexts.addAll(texts);
                return super.embedBatch(texts);
            }
        };
        EmbeddingProviderLookup lookup = name -> "model-a".equals(name) ? provider : null;

        List<PendingEmbeddingRow> pending = List.of(
                row("r1", "table1", "vec", "model-a", List.of("title", "content"),
                        Map.of("title", "Hello", "content", "World"), 0));

        RecordingQueue queue = new RecordingQueue(pending);
        drainer = new AsyncEmbeddingDrainer(queue, lookup);

        drainer.drainOnce();

        assertThat(capturedTexts).containsExactly("Hello\nWorld");
    }

    @Test
    void startAndCloseLifecycle() throws InterruptedException {
        RecordingQueue queue = new RecordingQueue(List.of());
        EmbeddingProviderLookup lookup = name -> null;
        drainer = new AsyncEmbeddingDrainer(queue, lookup);

        assertThat(drainer.isRunning()).isFalse();

        drainer.start();
        assertThat(drainer.isRunning()).isTrue();

        // Start is idempotent
        drainer.start();
        assertThat(drainer.isRunning()).isTrue();

        drainer.close();
        assertThat(drainer.isRunning()).isFalse();

        // Close is idempotent
        drainer.close();
        assertThat(drainer.isRunning()).isFalse();
    }

    @Test
    void backgroundDrainProcessesRows() throws InterruptedException {
        CountDownLatch commitLatch = new CountDownLatch(1);
        StubEmbeddingProvider provider = new StubEmbeddingProvider("model-a", 3, 64);
        EmbeddingProviderLookup lookup = name -> "model-a".equals(name) ? provider : null;

        List<PendingEmbeddingRow> pending = List.of(
                row("r1", "table1", "vec", "model-a", List.of("content"), Map.of("content", "hello"), 0));

        RecordingQueue queue = new RecordingQueue(pending) {
            @Override
            public void commit(List<EmbeddingResult> results) {
                super.commit(results);
                commitLatch.countDown();
            }
        };

        drainer = new AsyncEmbeddingDrainer(queue, lookup);
        drainer.start();

        boolean committed = commitLatch.await(5, TimeUnit.SECONDS);
        assertThat(committed).isTrue();
        assertThat(queue.committed).hasSize(1);
    }

    @Test
    void constructorRejectsNullQueue() {
        assertThatThrownBy(() -> new AsyncEmbeddingDrainer(null, name -> null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void constructorRejectsNullLookup() {
        assertThatThrownBy(() -> new AsyncEmbeddingDrainer(new RecordingQueue(List.of()), null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void constructorRejectsInvalidBatchSize() {
        assertThatThrownBy(() -> new AsyncEmbeddingDrainer(new RecordingQueue(List.of()), name -> null, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void maxRetriesIsThree() {
        assertThat(AsyncEmbeddingDrainer.MAX_RETRIES).isEqualTo(3);
    }

    // --- Test helpers ---

    private static PendingEmbeddingRow row(String rowId, String tableName, String vectorColumn,
                                           String modelName, List<String> sourceColumns,
                                           Map<String, String> sourceValues, int attemptCount) {
        return new PendingEmbeddingRow(rowId, tableName, vectorColumn, modelName,
                sourceColumns, sourceValues, attemptCount);
    }

    /**
     * Queue implementation that records all operations for assertion.
     */
    private static class RecordingQueue implements EmbeddingQueue {
        private final List<PendingEmbeddingRow> pendingRows;
        final List<EmbeddingResult> committed = new CopyOnWriteArrayList<>();
        final List<PendingEmbeddingRow> reEnqueued = new CopyOnWriteArrayList<>();
        final List<PendingEmbeddingRow> failed = new CopyOnWriteArrayList<>();
        private final AtomicInteger pollCount = new AtomicInteger();

        RecordingQueue(List<PendingEmbeddingRow> pendingRows) {
            this.pendingRows = new ArrayList<>(pendingRows);
        }

        @Override
        public List<PendingEmbeddingRow> poll(int maxBatchSize) {
            if (pollCount.getAndIncrement() == 0) {
                return Collections.unmodifiableList(pendingRows);
            }
            return List.of();
        }

        @Override
        public void commit(List<EmbeddingResult> results) {
            committed.addAll(results);
        }

        @Override
        public void reEnqueue(List<PendingEmbeddingRow> rows) {
            reEnqueued.addAll(rows);
        }

        @Override
        public void markFailed(List<PendingEmbeddingRow> rows) {
            failed.addAll(rows);
        }
    }

    /**
     * Provider that returns deterministic vectors.
     */
    private static class StubEmbeddingProvider implements EmbeddingProvider {
        private final String modelName;
        private final int dimensions;
        private final int maxBatchSize;

        StubEmbeddingProvider(String modelName, int dimensions, int maxBatchSize) {
            this.modelName = modelName;
            this.dimensions = dimensions;
            this.maxBatchSize = maxBatchSize;
        }

        @Override
        public float[] embed(String text) {
            return generateVector(text);
        }

        @Override
        public List<float[]> embedBatch(List<String> texts) {
            return texts.stream().map(this::generateVector).toList();
        }

        @Override
        public List<float[]> embedBlob(List<BlobReference> refs) {
            return refs.stream().map(r -> generateVector(r.uri())).toList();
        }

        @Override
        public int dimensions() { return dimensions; }

        @Override
        public String modelName() { return modelName; }

        @Override
        public int maxBatchSize() { return maxBatchSize; }

        private float[] generateVector(String text) {
            float[] vec = new float[dimensions];
            int hash = text.hashCode();
            for (int i = 0; i < dimensions; i++) {
                vec[i] = ((hash >> i) & 0xFF) / 255.0f;
            }
            return vec;
        }
    }

    /**
     * Provider that always throws to test error handling.
     */
    private static final class FailingEmbeddingProvider implements EmbeddingProvider {
        private final String modelName;
        private final int dimensions;

        FailingEmbeddingProvider(String modelName, int dimensions) {
            this.modelName = modelName;
            this.dimensions = dimensions;
        }

        @Override
        public float[] embed(String text) {
            throw new RuntimeException("Connection refused");
        }

        @Override
        public List<float[]> embedBatch(List<String> texts) {
            throw new RuntimeException("Connection refused");
        }

        @Override
        public List<float[]> embedBlob(List<BlobReference> refs) {
            throw new RuntimeException("Connection refused");
        }

        @Override
        public int dimensions() { return dimensions; }

        @Override
        public String modelName() { return modelName; }

        @Override
        public int maxBatchSize() { return 64; }
    }
}
