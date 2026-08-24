package org.pangrova.worker.ai.embedding;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.spi.ai.embedding.EmbeddingProvider;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies SyncEmbeddingExecutor: single-row embed, batch embed, skip-unchanged,
 * error handling, and batching logic.
 */
class SyncEmbeddingExecutorTest {

    private final StubEmbeddingProvider stubProvider = new StubEmbeddingProvider("test-model", 3, 64);
    private final EmbeddingProviderLookup lookup = modelName -> {
        if ("test-model".equals(modelName)) {
            return stubProvider;
        }
        return null;
    };

    @Test
    void embedRowSingleColumn() {
        SyncEmbeddingExecutor executor = new SyncEmbeddingExecutor(lookup);
        List<EmbedRequest> requests = List.of(
                new EmbedRequest("embedding", "test-model", List.of("content"), 3));

        Map<String, float[]> result = executor.embedRow(requests, Map.of("content", "hello world"));

        assertThat(result).containsKey("embedding");
        assertThat(result.get("embedding")).hasSize(3);
    }

    @Test
    void embedRowMultipleSourceColumns() {
        SyncEmbeddingExecutor executor = new SyncEmbeddingExecutor(lookup);
        List<EmbedRequest> requests = List.of(
                new EmbedRequest("embedding", "test-model", List.of("title", "content"), 3));

        Map<String, float[]> result = executor.embedRow(requests,
                Map.of("title", "Hello", "content", "World"));

        assertThat(result).containsKey("embedding");
        // Verify the provider received concatenated text
        assertThat(stubProvider.lastEmbeddedText()).isEqualTo("Hello\nWorld");
    }

    @Test
    void embedRowSkipsNullSourceColumns() {
        SyncEmbeddingExecutor executor = new SyncEmbeddingExecutor(lookup);
        List<EmbedRequest> requests = List.of(
                new EmbedRequest("embedding", "test-model", List.of("title", "content"), 3));

        Map<String, float[]> result = executor.embedRow(requests,
                Map.of("title", "Hello"));

        assertThat(result).containsKey("embedding");
        assertThat(stubProvider.lastEmbeddedText()).isEqualTo("Hello");
    }

    @Test
    void embedRowSkipsWhenAllSourceColumnsNull() {
        SyncEmbeddingExecutor executor = new SyncEmbeddingExecutor(lookup);
        List<EmbedRequest> requests = List.of(
                new EmbedRequest("embedding", "test-model", List.of("title", "content"), 3));

        Map<String, float[]> result = executor.embedRow(requests, Map.of());

        assertThat(result).isEmpty();
    }

    @Test
    void embedRowEmptyRequestsReturnsEmpty() {
        SyncEmbeddingExecutor executor = new SyncEmbeddingExecutor(lookup);

        Map<String, float[]> result = executor.embedRow(List.of(), Map.of("content", "hello"));

        assertThat(result).isEmpty();
    }

    @Test
    void embedRowThrowsOnProviderUnavailable() {
        SyncEmbeddingExecutor executor = new SyncEmbeddingExecutor(lookup);
        List<EmbedRequest> requests = List.of(
                new EmbedRequest("embedding", "unknown-model", List.of("content"), 3));

        assertThatThrownBy(() -> executor.embedRow(requests, Map.of("content", "hello")))
                .isInstanceOf(PangrovaException.class)
                .satisfies(ex -> assertThat(((PangrovaException) ex).getErrorCode())
                        .isEqualTo(PangrovaErrorCode.AI_EMBEDDING_PROVIDER_UNAVAILABLE));
    }

    @Test
    void embedRowThrowsOnProviderFailure() {
        EmbeddingProviderLookup failingLookup = modelName -> new FailingEmbeddingProvider();
        SyncEmbeddingExecutor executor = new SyncEmbeddingExecutor(failingLookup);
        List<EmbedRequest> requests = List.of(
                new EmbedRequest("embedding", "fail-model", List.of("content"), 3));

        assertThatThrownBy(() -> executor.embedRow(requests, Map.of("content", "hello")))
                .isInstanceOf(PangrovaException.class)
                .satisfies(ex -> assertThat(((PangrovaException) ex).getErrorCode())
                        .isEqualTo(PangrovaErrorCode.AI_EMBEDDING_PROVIDER_UNAVAILABLE));
    }

    @Test
    void embedBatchProcessesMultipleRows() {
        SyncEmbeddingExecutor executor = new SyncEmbeddingExecutor(lookup);
        List<EmbedRequest> requests = List.of(
                new EmbedRequest("embedding", "test-model", List.of("content"), 3));
        List<Map<String, String>> rows = List.of(
                Map.of("content", "row1"),
                Map.of("content", "row2"),
                Map.of("content", "row3"));

        List<Map<String, float[]>> results = executor.embedBatch(requests, rows);

        assertThat(results).hasSize(3);
        for (Map<String, float[]> row : results) {
            assertThat(row).containsKey("embedding");
            assertThat(row.get("embedding")).hasSize(3);
        }
    }

    @Test
    void embedBatchRespectsBatchSize() {
        // Use batch size of 2 to force multiple batches for 3 rows
        SyncEmbeddingExecutor executor = new SyncEmbeddingExecutor(lookup, 2);
        List<EmbedRequest> requests = List.of(
                new EmbedRequest("embedding", "test-model", List.of("content"), 3));
        List<Map<String, String>> rows = List.of(
                Map.of("content", "row1"),
                Map.of("content", "row2"),
                Map.of("content", "row3"));

        List<Map<String, float[]>> results = executor.embedBatch(requests, rows);

        assertThat(results).hasSize(3);
        // Provider should have been called with batches of 2 then 1
        assertThat(stubProvider.batchCallCount()).isEqualTo(2);
    }

    @Test
    void embedBatchSkipsEmptyTextRows() {
        SyncEmbeddingExecutor executor = new SyncEmbeddingExecutor(lookup);
        List<EmbedRequest> requests = List.of(
                new EmbedRequest("embedding", "test-model", List.of("content"), 3));
        List<Map<String, String>> rows = List.of(
                Map.of("content", "row1"),
                Map.of(),  // empty source → no embedding
                Map.of("content", "row3"));

        List<Map<String, float[]>> results = executor.embedBatch(requests, rows);

        assertThat(results).hasSize(3);
        assertThat(results.get(0)).containsKey("embedding");
        assertThat(results.get(1).get("embedding")).isNull();
        assertThat(results.get(2)).containsKey("embedding");
    }

    @Test
    void embedBatchEmptyRowsReturnsEmptyMaps() {
        SyncEmbeddingExecutor executor = new SyncEmbeddingExecutor(lookup);

        List<Map<String, float[]>> results = executor.embedBatch(List.of(), List.of(Map.of()));

        assertThat(results).hasSize(1);
        assertThat(results.get(0)).isEmpty();
    }

    @Test
    void needsRecomputeReturnsTrueWhenSourceChanged() {
        SyncEmbeddingExecutor executor = new SyncEmbeddingExecutor(lookup);
        EmbedRequest request = new EmbedRequest("embedding", "test-model", List.of("title", "content"), 3);

        boolean result = executor.needsRecompute(request,
                Map.of("title", "old", "content", "same"),
                Map.of("title", "new", "content", "same"));

        assertThat(result).isTrue();
    }

    @Test
    void needsRecomputeReturnsFalseWhenSourceUnchanged() {
        SyncEmbeddingExecutor executor = new SyncEmbeddingExecutor(lookup);
        EmbedRequest request = new EmbedRequest("embedding", "test-model", List.of("title", "content"), 3);

        boolean result = executor.needsRecompute(request,
                Map.of("title", "same", "content", "same"),
                Map.of("title", "same", "content", "same"));

        assertThat(result).isFalse();
    }

    @Test
    void needsRecomputeHandlesNullValues() {
        SyncEmbeddingExecutor executor = new SyncEmbeddingExecutor(lookup);
        EmbedRequest request = new EmbedRequest("embedding", "test-model", List.of("title"), 3);

        // Both null → unchanged
        assertThat(executor.needsRecompute(request, Map.of(), Map.of())).isFalse();

        // null → value → changed
        assertThat(executor.needsRecompute(request, Map.of(), Map.of("title", "new"))).isTrue();

        // value → null → changed
        assertThat(executor.needsRecompute(request, Map.of("title", "old"), Map.of())).isTrue();
    }

    @Test
    void constructorRejectsNullLookup() {
        assertThatThrownBy(() -> new SyncEmbeddingExecutor(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void constructorRejectsInvalidBatchSize() {
        assertThatThrownBy(() -> new SyncEmbeddingExecutor(lookup, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SyncEmbeddingExecutor(lookup, -1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void defaultBatchSizeIs64() {
        assertThat(SyncEmbeddingExecutor.DEFAULT_BATCH_SIZE).isEqualTo(64);
    }

    // --- Test helpers (real implementations, no mocks) ---

    /**
     * A simple embedding provider that returns deterministic vectors for testing.
     */
    private static final class StubEmbeddingProvider implements EmbeddingProvider {

        private final String modelName;
        private final int dimensions;
        private final int maxBatchSize;
        private volatile String lastEmbeddedText;
        private volatile int batchCallCount;

        StubEmbeddingProvider(String modelName, int dimensions, int maxBatchSize) {
            this.modelName = modelName;
            this.dimensions = dimensions;
            this.maxBatchSize = maxBatchSize;
        }

        @Override
        public float[] embed(String text) {
            this.lastEmbeddedText = text;
            return generateVector(text);
        }

        @Override
        public List<float[]> embedBatch(List<String> texts) {
            batchCallCount++;
            return texts.stream().map(this::generateVector).toList();
        }

        @Override
        public List<float[]> embedBlob(List<org.pangrova.spi.ai.embedding.BlobReference> refs) {
            return refs.stream().map(r -> generateVector(r.uri())).toList();
        }

        @Override
        public int dimensions() {
            return dimensions;
        }

        @Override
        public String modelName() {
            return modelName;
        }

        @Override
        public int maxBatchSize() {
            return maxBatchSize;
        }

        String lastEmbeddedText() {
            return lastEmbeddedText;
        }

        int batchCallCount() {
            return batchCallCount;
        }

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
     * A provider that always throws to test error handling.
     */
    private static final class FailingEmbeddingProvider implements EmbeddingProvider {

        @Override
        public float[] embed(String text) {
            throw new RuntimeException("Connection refused");
        }

        @Override
        public List<float[]> embedBatch(List<String> texts) {
            throw new RuntimeException("Connection refused");
        }

        @Override
        public List<float[]> embedBlob(List<org.pangrova.spi.ai.embedding.BlobReference> refs) {
            throw new RuntimeException("Connection refused");
        }

        @Override
        public int dimensions() {
            return 3;
        }

        @Override
        public String modelName() {
            return "fail-model";
        }

        @Override
        public int maxBatchSize() {
            return 64;
        }
    }
}
