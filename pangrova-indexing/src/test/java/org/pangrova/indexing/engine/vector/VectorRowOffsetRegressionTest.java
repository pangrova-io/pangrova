package org.pangrova.indexing.engine.vector;

import org.pangrova.spi.index.data.IndexData;
import org.pangrova.spi.index.vector.DistanceMetric;
import org.pangrova.spi.index.vector.FilteredSearchStrategy;
import org.pangrova.spi.index.vector.VectorSearchRequest;
import org.pangrova.spi.index.vector.VectorSearchResult.ScoredRow;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for the ANN row-offset mapping (finding F-CHAIN-INDEX-05).
 *
 * <p>When a vector column contains NULLs, the build path skips them but must
 * still report each retained vector at its <em>physical</em> 0-based file row
 * position — identical to {@link BruteForceVectorScanner}. Previously HNSW and
 * IVFPQ reported the dense (null-skipping) index, so ANN results pointed at the
 * wrong rows and disagreed with the brute-force scanner for the same file.</p>
 */
class VectorRowOffsetRegressionTest {

    private static final int DIM = 4;

    // Rows: [null, A, null, B, C, null, D]
    // Physical offsets of the four non-null vectors: A=1, B=3, C=4, D=6.
    // Dense (null-skipping) indices would be:          A=0, B=1, C=2, D=3.
    private static final float[] A = {1f, 0f, 0f, 0f};
    private static final float[] B = {0f, 1f, 0f, 0f};
    private static final float[] C = {0f, 0f, 1f, 0f};
    private static final float[] D = {0f, 0f, 0f, 1f};
    private static final Set<Long> PHYSICAL_OFFSETS = Set.of(1L, 3L, 4L, 6L);

    private static List<List<Comparable<?>>> rowsWithNulls() {
        List<List<Comparable<?>>> rows = new ArrayList<>();
        rows.add(nullRow());   // physical row 0
        rows.add(row(A));      // physical row 1
        rows.add(nullRow());   // physical row 2
        rows.add(row(B));      // physical row 3
        rows.add(row(C));      // physical row 4
        rows.add(nullRow());   // physical row 5
        rows.add(row(D));      // physical row 6
        return rows;
    }

    @Test
    @DisplayName("HNSW search reports physical file row offset, not the dense index")
    void hnswSearchReturnsPhysicalRowOffset() {
        VectorStrategyProvider provider = new VectorStrategyProvider();
        IndexData index = provider.build(rowsWithNulls().iterator(), Map.of(
                "algorithm", "HNSW", "dimension", String.valueOf(DIM), "metric", "L2"));

        // Query is exactly B -> nearest neighbour is B at physical row 3 (not 1).
        VectorSearchRequest request = new VectorSearchRequest("vec", B, 1, DistanceMetric.L2);
        List<ScoredRow> results = provider.vectorSearch(index, request, Map.of());

        assertThat(results).hasSize(1);
        assertThat(results.get(0).rowOffset()).isEqualTo(3L);
    }

    @Test
    @DisplayName("HNSW pre-filter matches the allow-set against physical row offsets")
    void hnswFilteredSearchUsesPhysicalRowOffset() {
        VectorStrategyProvider provider = new VectorStrategyProvider();
        IndexData index = provider.build(rowsWithNulls().iterator(), Map.of(
                "algorithm", "HNSW", "dimension", String.valueOf(DIM), "metric", "L2"));

        // Allow only physical row 3 (vector B). With the dense-index bug this set
        // would instead have matched node 3 (vector D), returning the wrong row.
        VectorSearchRequest request = new VectorSearchRequest(
                "vec", B, 4, DistanceMetric.L2, FilteredSearchStrategy.PRE_FILTER);
        List<ScoredRow> results = provider.filteredVectorSearch(
                index, request, Map.of(), Set.of(3L));

        assertThat(results).hasSize(1);
        assertThat(results.get(0).rowOffset()).isEqualTo(3L);
    }

    @Test
    @DisplayName("IVFPQ search reports only physical file row offsets")
    void ivfpqSearchReturnsPhysicalRowOffsets() {
        VectorStrategyProvider provider = new VectorStrategyProvider();
        IndexData index = provider.build(rowsWithNulls().iterator(), Map.of(
                "algorithm", "IVFPQ", "dimension", String.valueOf(DIM), "metric", "L2",
                "ivfpq.nlist", "2", "ivfpq.m", "2", "ivfpq.nbits", "8"));

        VectorSearchRequest request = new VectorSearchRequest("vec", B, 4, DistanceMetric.L2);
        List<ScoredRow> results = provider.vectorSearch(index, request, Map.of("ivfpq.nprobe", "2"));

        assertThat(results).isNotEmpty();
        // Every reported offset must be a real physical row (never 0/2/5 = NULLs,
        // never a dense index that collides with a NULL position).
        for (ScoredRow row : results) {
            assertThat(PHYSICAL_OFFSETS).contains(row.rowOffset());
        }
        assertThat(results).extracting(ScoredRow::rowOffset).contains(3L);
    }

    // --- Helpers (mirror the realistic List<Float> column-reader format) ---

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static List<Comparable<?>> row(float[] vector) {
        List<Float> boxed = new ArrayList<>(vector.length);
        for (float v : vector) {
            boxed.add(v);
        }
        List raw = new ArrayList();
        raw.add(boxed);
        return raw;
    }

    private static List<Comparable<?>> nullRow() {
        List<Comparable<?>> r = new ArrayList<>();
        r.add(null);
        return r;
    }
}
