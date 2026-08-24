package org.pangrova.worker.property.embedding;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

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
 * Property-based tests for embedding dimension consistency (P2).
 *
 * <p>Validates that the embedding output dimension always matches the model's
 * declared dimension, regardless of input text content or length. The
 * {@link SyncEmbeddingExecutor} must produce vectors whose length equals
 * the {@link EmbeddingProvider#dimensions()} contract.</p>
 *
 * <p><b>Validates: Requirements P2</b></p>
 */
class DimensionConsistencyPropertyTest {

    private static final String VECTOR_COLUMN = "embedding";
    private static final String MODEL_NAME = "test_model";

    @Provide
    Arbitrary<Integer> dimensions() {
        return Arbitraries.of(64, 128, 256, 384, 512, 768, 1024, 1536);
    }

    @Provide
    Arbitrary<String> inputTexts() {
        return Arbitraries.strings().ofMinLength(1).ofMaxLength(200);
    }

    @Provide
    Arbitrary<List<String>> textBatches() {
        return Arbitraries.strings().ofMinLength(1).ofMaxLength(100)
                .list().ofMinSize(1).ofMaxSize(20);
    }

    @Provide
    Arbitrary<List<String>> sourceColumnNames() {
        return Arbitraries.of("title", "content", "description", "summary")
                .list().ofMinSize(1).ofMaxSize(3).uniqueElements();
    }

    /**
     * P2: For any declared dimension and any input text, embed() always returns
     * a vector whose length equals the provider's declared dimension.
     */
    @Property(tries = 100)
    void singleEmbedOutputMatchesDeclaredDimension(
            @ForAll("dimensions") int dimension,
            @ForAll("inputTexts") String text) {

        DeterministicProvider provider = new DeterministicProvider(dimension);
        EmbeddingProviderLookup lookup = name -> provider;
        SyncEmbeddingExecutor executor = new SyncEmbeddingExecutor(lookup);

        EmbedRequest request = new EmbedRequest(
                VECTOR_COLUMN, MODEL_NAME, List.of("content"), dimension);

        Map<String, String> sourceValues = Map.of("content", text);
        Map<String, float[]> result = executor.embedRow(List.of(request), sourceValues);

        assertThat(result).containsKey(VECTOR_COLUMN);
        assertThat(result.get(VECTOR_COLUMN)).hasSize(dimension);
    }

    /**
     * P2: For any declared dimension and any batch of texts, embedBatch() always
     * returns vectors whose length equals the provider's declared dimension.
     */
    @Property(tries = 50)
    void batchEmbedOutputMatchesDeclaredDimension(
            @ForAll("dimensions") int dimension,
            @ForAll("textBatches") List<String> texts) {

        DeterministicProvider provider = new DeterministicProvider(dimension);
        EmbeddingProviderLookup lookup = name -> provider;
        SyncEmbeddingExecutor executor = new SyncEmbeddingExecutor(lookup);

        EmbedRequest request = new EmbedRequest(
                VECTOR_COLUMN, MODEL_NAME, List.of("content"), dimension);

        List<Map<String, String>> rowSources = new ArrayList<>();
        for (String text : texts) {
            rowSources.add(Map.of("content", text));
        }

        List<Map<String, float[]>> results = executor.embedBatch(List.of(request), rowSources);

        assertThat(results).hasSize(texts.size());
        for (Map<String, float[]> row : results) {
            assertThat(row).containsKey(VECTOR_COLUMN);
            assertThat(row.get(VECTOR_COLUMN)).hasSize(dimension);
        }
    }

    /**
     * P2: Multi-column concatenation does not affect output dimension.
     * Regardless of how many source columns are concatenated, the output
     * vector dimension matches the model's declared dimension.
     */
    @Property(tries = 50)
    void multiColumnConcatenationPreservesDimension(
            @ForAll("dimensions") int dimension,
            @ForAll("sourceColumnNames") List<String> sourceColumns,
            @ForAll("inputTexts") String text) {

        DeterministicProvider provider = new DeterministicProvider(dimension);
        EmbeddingProviderLookup lookup = name -> provider;
        SyncEmbeddingExecutor executor = new SyncEmbeddingExecutor(lookup);

        EmbedRequest request = new EmbedRequest(
                VECTOR_COLUMN, MODEL_NAME, sourceColumns, dimension);

        Map<String, String> sourceValues = new HashMap<>();
        for (String col : sourceColumns) {
            sourceValues.put(col, text);
        }

        Map<String, float[]> result = executor.embedRow(List.of(request), sourceValues);

        assertThat(result).containsKey(VECTOR_COLUMN);
        assertThat(result.get(VECTOR_COLUMN)).hasSize(dimension);
    }

    // ── Test double ──

    /**
     * Deterministic embedding provider that always returns a vector of the declared
     * dimension. Uses a simple hash-based fill to produce consistent but non-trivial values.
     */
    private static final class DeterministicProvider implements EmbeddingProvider {
        private final int dim;

        DeterministicProvider(int dim) {
            this.dim = dim;
        }

        @Override
        public float[] embed(String text) {
            float[] vec = new float[dim];
            int hash = text.hashCode();
            for (int i = 0; i < dim; i++) {
                vec[i] = (float) Math.sin(hash + i);
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
            for (int i = 0; i < refs.size(); i++) {
                results.add(new float[dim]);
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
