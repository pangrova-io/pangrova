package org.pangrova.indexing.engine.vector;

import org.pangrova.spi.index.data.vector.VectorIndexData;
import org.pangrova.spi.index.vector.DistanceMetric;
import org.pangrova.spi.index.vector.FilteredSearchStrategy;
import org.pangrova.spi.index.vector.VectorSearchRequest;
import org.pangrova.spi.index.vector.VectorSearchResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Task 166: validates pre-filter, iterative-scan, and predicate-aware
 * filtered vector search strategies on HNSW.
 */
class HnswFilteredSearchTest {

    private HnswEngine engine;
    private VectorIndexData indexData;
    private static final int DIM = 8;
    private static final int VECTOR_COUNT = 200;

    @BeforeEach
    void setUp() {
        engine = new HnswEngine();
        List<float[]> vectors = new ArrayList<>();
        for (int i = 0; i < VECTOR_COUNT; i++) {
            float[] v = new float[DIM];
            for (int d = 0; d < DIM; d++) {
                v[d] = ThreadLocalRandom.current().nextFloat();
            }
            vectors.add(v);
        }
        Map<String, String> props = Collections.emptyMap();
        long[] rowOffsets = new long[vectors.size()];
        for (int i = 0; i < rowOffsets.length; i++) {
            rowOffsets[i] = i;
        }
        indexData = engine.build(vectors, rowOffsets, DIM, DistanceMetric.L2, props);
    }

    @Test
    @DisplayName("pre-filter returns only rows in the allowed set")
    void preFilterReturnsOnlyAllowedRows() {
        Set<Long> allowed = Set.of(10L, 20L, 30L, 40L, 50L, 60L, 70L, 80L, 90L, 100L);
        float[] query = new float[DIM];
        VectorSearchRequest request = new VectorSearchRequest(
                "vec", query, 5, DistanceMetric.L2, FilteredSearchStrategy.PRE_FILTER);

        Map<String, String> params = Collections.emptyMap();
        List<VectorSearchResult.ScoredRow> results =
                engine.search(indexData, request, params, allowed);

        assertThat(results).hasSizeLessThanOrEqualTo(5);
        for (VectorSearchResult.ScoredRow row : results) {
            assertThat(allowed).contains(row.rowOffset());
        }
    }

    @Test
    @DisplayName("iterative scan widens search to find filtered results")
    void iterativeScanFindsFilteredResults() {
        Set<Long> allowed = new HashSet<>();
        for (long i = 0; i < VECTOR_COUNT; i += 10) {
            allowed.add(i);
        }
        float[] query = new float[DIM];
        VectorSearchRequest request = new VectorSearchRequest(
                "vec", query, 5, DistanceMetric.L2, FilteredSearchStrategy.ITERATIVE_SCAN);

        Map<String, String> params = Collections.emptyMap();
        List<VectorSearchResult.ScoredRow> results =
                engine.search(indexData, request, params, allowed);

        assertThat(results).hasSizeLessThanOrEqualTo(5);
        for (VectorSearchResult.ScoredRow row : results) {
            assertThat(allowed).contains(row.rowOffset());
        }
        // Results should be sorted by distance
        for (int i = 1; i < results.size(); i++) {
            assertThat(results.get(i).distance())
                    .isGreaterThanOrEqualTo(results.get(i - 1).distance());
        }
    }

    @Test
    @DisplayName("predicate-aware search only returns allowed rows")
    void predicateAwareSearch() {
        Set<Long> allowed = new HashSet<>();
        for (long i = 0; i < VECTOR_COUNT; i += 5) {
            allowed.add(i);
        }
        float[] query = new float[DIM];
        VectorSearchRequest request = new VectorSearchRequest(
                "vec", query, 5, DistanceMetric.L2, FilteredSearchStrategy.PREDICATE_AWARE);

        Map<String, String> params = Collections.emptyMap();
        List<VectorSearchResult.ScoredRow> results =
                engine.search(indexData, request, params, allowed);

        assertThat(results).hasSizeLessThanOrEqualTo(5);
        for (VectorSearchResult.ScoredRow row : results) {
            assertThat(allowed).contains(row.rowOffset());
        }
    }

    @Test
    @DisplayName("null allowedRows falls back to unfiltered search")
    void nullAllowedRowsFallsBack() {
        float[] query = new float[DIM];
        VectorSearchRequest request = new VectorSearchRequest(
                "vec", query, 5, DistanceMetric.L2, FilteredSearchStrategy.PRE_FILTER);

        Map<String, String> params = Collections.emptyMap();
        List<VectorSearchResult.ScoredRow> results =
                engine.search(indexData, request, params, null);

        assertThat(results).hasSize(5);
    }

    @Test
    @DisplayName("post-filter strategy with allowedRows ignores the bitmap")
    void postFilterIgnoresBitmap() {
        Set<Long> allowed = Set.of(1L);
        float[] query = new float[DIM];
        VectorSearchRequest request = new VectorSearchRequest(
                "vec", query, 5, DistanceMetric.L2, FilteredSearchStrategy.POST_FILTER);

        Map<String, String> params = Collections.emptyMap();
        List<VectorSearchResult.ScoredRow> results =
                engine.search(indexData, request, params, allowed);

        // POST_FILTER ignores allowedRows, returns full results
        assertThat(results).hasSize(5);
    }
}
