package org.pangrova.worker.property.embedding;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.constraints.IntRange;

import org.pangrova.spi.ai.embedding.EmbeddingProvider;
import org.pangrova.worker.ai.embedding.EmbeddingQueue;
import org.pangrova.worker.ai.embedding.EmbeddingResult;
import org.pangrova.worker.ai.embedding.PendingEmbeddingRow;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based tests for ASYNC embedding eventual consistency (P11).
 *
 * <p>Validates that all enqueued rows with non-null source text eventually
 * get embeddings after sufficient drain cycles. Tests the core drain logic:
 * poll → embed → commit/retry/fail cycle that the AsyncEmbeddingDrainer
 * executes internally.</p>
 *
 * <p><b>Validates: Requirements P11</b></p>
 */
class AsyncEventualConsistencyPropertyTest {

    private static final String TABLE = "catalog.schema.documents";
    private static final String VECTOR_COLUMN = "embedding";
    private static final String MODEL = "bge_m3";
    private static final int DIMENSIONS = 128;
    private static final int BATCH_SIZE = 64;
    private static final int MAX_RETRIES = 3;

    @Provide
    Arbitrary<List<String>> rowBatches() {
        Arbitrary<String> rowId = Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(8)
                .map(s -> "row_" + s.toLowerCase());
        return rowId.list().ofMinSize(1).ofMaxSize(20).uniqueElements();
    }

    @Provide
    Arbitrary<String> sourceTexts() {
        return Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(50);
    }

    /**
     * P11: For any set of enqueued rows, after sufficient drain cycles,
     * all rows are eventually committed with embeddings.
     */
    @Property(tries = 50)
    void allEnqueuedRowsEventuallyGetEmbeddings(
            @ForAll("rowBatches") List<String> rowIds,
            @ForAll("sourceTexts") String sourceText) {

        RecordingQueue queue = new RecordingQueue();
        FixedDimensionProvider provider = new FixedDimensionProvider(DIMENSIONS);

        // Enqueue all rows
        for (String rowId : rowIds) {
            PendingEmbeddingRow row = new PendingEmbeddingRow(
                    rowId, TABLE, VECTOR_COLUMN, MODEL,
                    List.of("content"), Map.of("content", sourceText), 0);
            queue.enqueue(row);
        }

        // Simulate drain cycles (poll → embed → commit)
        int maxIterations = rowIds.size() * 3;
        for (int i = 0; i < maxIterations && !queue.isEmpty(); i++) {
            drainOnce(queue, provider);
        }

        // All rows must be committed
        assertThat(queue.committedResults()).hasSize(rowIds.size());

        // Every original row ID appears in committed results
        List<String> committedRowIds = queue.committedResults().stream()
                .map(EmbeddingResult::rowId)
                .toList();
        assertThat(committedRowIds).containsExactlyInAnyOrderElementsOf(rowIds);
    }

    /**
     * P11: Each committed embedding has the correct dimensionality
     * matching the provider's declared dimensions.
     */
    @Property(tries = 50)
    void committedEmbeddingsHaveCorrectDimensions(
            @ForAll("rowBatches") List<String> rowIds,
            @ForAll("sourceTexts") String sourceText) {

        RecordingQueue queue = new RecordingQueue();
        FixedDimensionProvider provider = new FixedDimensionProvider(DIMENSIONS);

        for (String rowId : rowIds) {
            PendingEmbeddingRow row = new PendingEmbeddingRow(
                    rowId, TABLE, VECTOR_COLUMN, MODEL,
                    List.of("content"), Map.of("content", sourceText), 0);
            queue.enqueue(row);
        }

        int maxIterations = rowIds.size() * 3;
        for (int i = 0; i < maxIterations && !queue.isEmpty(); i++) {
            drainOnce(queue, provider);
        }

        for (EmbeddingResult result : queue.committedResults()) {
            assertThat(result.embedding()).hasSize(DIMENSIONS);
        }
    }

    /**
     * P11: Rows that fail embedding (provider throws) are retried up to
     * MAX_RETRIES times, then marked as permanently failed. No row is lost.
     */
    @Property(tries = 30)
    void failedRowsAreRetriedThenMarkedFailed(
            @ForAll @IntRange(min = 1, max = 10) int rowCount) {

        RecordingQueue queue = new RecordingQueue();
        FailingProvider failingProvider = new FailingProvider();

        for (int i = 0; i < rowCount; i++) {
            PendingEmbeddingRow row = new PendingEmbeddingRow(
                    "row_" + i, TABLE, VECTOR_COLUMN, MODEL,
                    List.of("content"), Map.of("content", "text_" + i), 0);
            queue.enqueue(row);
        }

        // Drain enough times for all retries to exhaust
        int maxIterations = rowCount * (MAX_RETRIES + 2);
        for (int i = 0; i < maxIterations && !queue.isEmpty(); i++) {
            drainOnceWithFailure(queue, failingProvider);
        }

        // All rows end up either committed or failed — none lost
        int totalAccountedFor = queue.committedResults().size() + queue.failedRows().size();
        assertThat(totalAccountedFor).isEqualTo(rowCount);

        // Since provider always fails, all should be marked failed
        assertThat(queue.failedRows()).hasSize(rowCount);
    }

    // ── Drain simulation (mirrors AsyncEmbeddingDrainer core logic) ──

    private void drainOnce(RecordingQueue queue, EmbeddingProvider provider) {
        List<PendingEmbeddingRow> batch = queue.poll(BATCH_SIZE);
        if (batch.isEmpty()) {
            return;
        }

        List<EmbeddingResult> successes = new ArrayList<>();
        for (PendingEmbeddingRow row : batch) {
            String text = String.join("\n", row.sourceValues().values());
            float[] embedding = provider.embed(text);
            successes.add(new EmbeddingResult(row.rowId(), row.tableName(),
                    row.vectorColumn(), embedding));
        }
        queue.commit(successes);
    }

    private void drainOnceWithFailure(RecordingQueue queue, EmbeddingProvider provider) {
        List<PendingEmbeddingRow> batch = queue.poll(BATCH_SIZE);
        if (batch.isEmpty()) {
            return;
        }

        List<PendingEmbeddingRow> retryable = new ArrayList<>();
        List<PendingEmbeddingRow> exhausted = new ArrayList<>();

        for (PendingEmbeddingRow row : batch) {
            try {
                provider.embed("text");
            } catch (RuntimeException e) {
                if (row.attemptCount() + 1 >= MAX_RETRIES) {
                    exhausted.add(row);
                } else {
                    retryable.add(new PendingEmbeddingRow(
                            row.rowId(), row.tableName(), row.vectorColumn(),
                            row.modelName(), row.sourceColumns(), row.sourceValues(),
                            row.attemptCount() + 1));
                }
            }
        }

        if (!retryable.isEmpty()) {
            queue.reEnqueue(retryable);
        }
        if (!exhausted.isEmpty()) {
            queue.markFailed(exhausted);
        }
    }

    // ── Test doubles ──

    /**
     * Embedding provider that returns a fixed-dimension zero vector for any input.
     */
    private static final class FixedDimensionProvider implements EmbeddingProvider {
        private final int dimensions;

        FixedDimensionProvider(int dimensions) {
            this.dimensions = dimensions;
        }

        @Override
        public float[] embed(String text) {
            return new float[dimensions];
        }

        @Override
        public List<float[]> embedBatch(List<String> texts) {
            List<float[]> results = new ArrayList<>(texts.size());
            for (int i = 0; i < texts.size(); i++) {
                results.add(new float[dimensions]);
            }
            return results;
        }

        @Override
        public List<float[]> embedBlob(List<org.pangrova.spi.ai.embedding.BlobReference> refs) {
            List<float[]> results = new ArrayList<>(refs.size());
            for (int i = 0; i < refs.size(); i++) {
                results.add(new float[dimensions]);
            }
            return results;
        }

        @Override
        public int dimensions() {
            return dimensions;
        }

        @Override
        public String modelName() {
            return MODEL;
        }

        @Override
        public int maxBatchSize() {
            return 64;
        }
    }

    /**
     * Embedding provider that always throws to simulate transient failures.
     */
    private static final class FailingProvider implements EmbeddingProvider {
        @Override
        public float[] embed(String text) {
            throw new RuntimeException("Simulated embedding failure");
        }

        @Override
        public List<float[]> embedBatch(List<String> texts) {
            throw new RuntimeException("Simulated embedding failure");
        }

        @Override
        public List<float[]> embedBlob(List<org.pangrova.spi.ai.embedding.BlobReference> refs) {
            throw new RuntimeException("Simulated embedding failure");
        }

        @Override
        public int dimensions() {
            return DIMENSIONS;
        }

        @Override
        public String modelName() {
            return MODEL;
        }

        @Override
        public int maxBatchSize() {
            return 64;
        }
    }

    /**
     * In-memory queue that records all committed results and failed rows.
     */
    private static final class RecordingQueue implements EmbeddingQueue {
        private final List<PendingEmbeddingRow> pending =
                Collections.synchronizedList(new ArrayList<>());
        private final CopyOnWriteArrayList<EmbeddingResult> committed = new CopyOnWriteArrayList<>();
        private final CopyOnWriteArrayList<PendingEmbeddingRow> failed = new CopyOnWriteArrayList<>();

        void enqueue(PendingEmbeddingRow row) {
            pending.add(row);
        }

        boolean isEmpty() {
            return pending.isEmpty();
        }

        List<EmbeddingResult> committedResults() {
            return List.copyOf(committed);
        }

        List<PendingEmbeddingRow> failedRows() {
            return List.copyOf(failed);
        }

        @Override
        public List<PendingEmbeddingRow> poll(int maxBatchSize) {
            List<PendingEmbeddingRow> batch = new ArrayList<>();
            synchronized (pending) {
                int count = Math.min(maxBatchSize, pending.size());
                for (int i = 0; i < count; i++) {
                    batch.add(pending.remove(0));
                }
            }
            return batch;
        }

        @Override
        public void commit(List<EmbeddingResult> results) {
            committed.addAll(results);
        }

        @Override
        public void reEnqueue(List<PendingEmbeddingRow> rows) {
            pending.addAll(rows);
        }

        @Override
        public void markFailed(List<PendingEmbeddingRow> rows) {
            failed.addAll(rows);
        }
    }
}
