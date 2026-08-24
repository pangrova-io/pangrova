package org.pangrova.worker.property.embedding;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.constraints.IntRange;

import org.pangrova.spi.ai.embedding.BlobReference;
import org.pangrova.spi.ai.embedding.EmbeddingProvider;
import org.pangrova.worker.ai.embedding.EmbedRequest;
import org.pangrova.worker.ai.embedding.EmbeddingProviderLookup;
import org.pangrova.worker.ai.embedding.SyncEmbeddingExecutor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based tests for embedding idempotence (P1).
 *
 * <p>Validates that the same input text always produces the same embedding vector.
 * This is critical for backfill at-least-once semantics (Req 7.10): re-embedding
 * a row after crash recovery must produce an identical result.</p>
 *
 * <p><b>Validates: Requirements P1</b></p>
 */
class EmbeddingIdempotencePropertyTest {

    private static final String VECTOR_COLUMN = "embedding";
    private static final String MODEL_NAME = "deterministic_model";
    private static final int DIMENSIONS = 256;

    @Provide
    Arbitrary<String> inputTexts() {
        return Arbitraries.strings().ofMinLength(1).ofMaxLength(300);
    }

    @Provide
    Arbitrary<List<String>> sourceColumnNames() {
        return Arbitraries.of("title", "content", "description", "summary")
                .list().ofMinSize(1).ofMaxSize(3).uniqueElements();
    }

    /**
     * P1: Embedding the same text twice produces identical vectors.
     */
    @Property(tries = 100)
    void sameInputProducesSameEmbedding(@ForAll("inputTexts") String text) {

        IdempotentProvider provider = new IdempotentProvider(DIMENSIONS);
        EmbeddingProviderLookup lookup = name -> provider;
        SyncEmbeddingExecutor executor = new SyncEmbeddingExecutor(lookup);

        EmbedRequest request = new EmbedRequest(
                VECTOR_COLUMN, MODEL_NAME, List.of("content"), DIMENSIONS);
        Map<String, String> sourceValues = Map.of("content", text);

        Map<String, float[]> first = executor.embedRow(List.of(request), sourceValues);
        Map<String, float[]> second = executor.embedRow(List.of(request), sourceValues);

        assertThat(first.get(VECTOR_COLUMN)).isEqualTo(second.get(VECTOR_COLUMN));
    }

    /**
     * P1: Embedding the same text N times always produces the same vector.
     * Tests repeated invocations to catch any hidden state accumulation.
     */
    @Property(tries = 50)
    void repeatedEmbeddingIsStable(
            @ForAll("inputTexts") String text,
            @ForAll @IntRange(min = 3, max = 10) int repetitions) {

        IdempotentProvider provider = new IdempotentProvider(DIMENSIONS);
        EmbeddingProviderLookup lookup = name -> provider;
        SyncEmbeddingExecutor executor = new SyncEmbeddingExecutor(lookup);

        EmbedRequest request = new EmbedRequest(
                VECTOR_COLUMN, MODEL_NAME, List.of("content"), DIMENSIONS);
        Map<String, String> sourceValues = Map.of("content", text);

        Map<String, float[]> baseline = executor.embedRow(List.of(request), sourceValues);
        float[] expected = baseline.get(VECTOR_COLUMN);

        for (int i = 1; i < repetitions; i++) {
            Map<String, float[]> result = executor.embedRow(List.of(request), sourceValues);
            assertThat(result.get(VECTOR_COLUMN))
                    .as("Repetition %d should produce identical vector", i)
                    .isEqualTo(expected);
        }
    }

    /**
     * P1: Multi-column concatenation is idempotent. The same set of source column
     * values always produces the same embedding regardless of invocation count.
     */
    @Property(tries = 50)
    void multiColumnIdempotence(
            @ForAll("sourceColumnNames") List<String> sourceColumns,
            @ForAll("inputTexts") String text) {

        IdempotentProvider provider = new IdempotentProvider(DIMENSIONS);
        EmbeddingProviderLookup lookup = name -> provider;
        SyncEmbeddingExecutor executor = new SyncEmbeddingExecutor(lookup);

        EmbedRequest request = new EmbedRequest(
                VECTOR_COLUMN, MODEL_NAME, sourceColumns, DIMENSIONS);

        Map<String, String> sourceValues = new HashMap<>();
        for (String col : sourceColumns) {
            sourceValues.put(col, text);
        }

        Map<String, float[]> first = executor.embedRow(List.of(request), sourceValues);
        Map<String, float[]> second = executor.embedRow(List.of(request), sourceValues);

        assertThat(first.get(VECTOR_COLUMN)).isEqualTo(second.get(VECTOR_COLUMN));
    }

    /**
     * P1: Batch embedding is idempotent. The same batch of texts produces
     * identical vectors on repeated invocations.
     */
    @Property(tries = 50)
    void batchEmbeddingIsIdempotent(
            @ForAll("inputTexts") String text,
            @ForAll @IntRange(min = 1, max = 10) int batchSize) {

        IdempotentProvider provider = new IdempotentProvider(DIMENSIONS);
        EmbeddingProviderLookup lookup = name -> provider;
        SyncEmbeddingExecutor executor = new SyncEmbeddingExecutor(lookup);

        EmbedRequest request = new EmbedRequest(
                VECTOR_COLUMN, MODEL_NAME, List.of("content"), DIMENSIONS);

        List<Map<String, String>> rowSources = new ArrayList<>();
        for (int i = 0; i < batchSize; i++) {
            rowSources.add(Map.of("content", text + "_" + i));
        }

        List<Map<String, float[]>> first = executor.embedBatch(List.of(request), rowSources);
        List<Map<String, float[]>> second = executor.embedBatch(List.of(request), rowSources);

        assertThat(first).hasSize(second.size());
        for (int i = 0; i < first.size(); i++) {
            assertThat(first.get(i).get(VECTOR_COLUMN))
                    .as("Row %d should produce identical vector on re-embed", i)
                    .isEqualTo(second.get(i).get(VECTOR_COLUMN));
        }
    }

    // ── Test double ──

    /**
     * Deterministic embedding provider that produces the same output for the same input.
     * Uses a hash-based algorithm that is purely a function of the input text,
     * with no internal mutable state.
     */
    private static final class IdempotentProvider implements EmbeddingProvider {
        private final int dim;

        IdempotentProvider(int dim) {
            this.dim = dim;
        }

        @Override
        public float[] embed(String text) {
            float[] vec = new float[dim];
            int hash = text.hashCode();
            for (int i = 0; i < dim; i++) {
                vec[i] = (float) Math.sin((double) hash * 31 + i * 7);
            }
            return vec;
        }

        @Override
        public List<float[]> embedBatch(List<String> texts) {
            List<float[]> results = new ArrayList<>(texts.size());
            for (String text : texts) {
                results.add(embed(text));
            }
            return results;
        }

        @Override
        public List<float[]> embedBlob(List<BlobReference> refs) {
            List<float[]> results = new ArrayList<>(refs.size());
            for (BlobReference ref : refs) {
                results.add(embed(ref.uri()));
            }
            return results;
        }

        @Override
        public int dimensions() {
            return dim;
        }

        @Override
        public String modelName() {
            return MODEL_NAME;
        }

        @Override
        public int maxBatchSize() {
            return 64;
        }
    }
}
