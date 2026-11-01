package org.pangrova.indexing.planner.vector;

import org.pangrova.spi.indexing.descriptor.IndexDescriptor;
import org.pangrova.spi.indexing.state.IndexState;
import org.pangrova.spi.indexing.descriptor.IndexType;
import org.pangrova.spi.indexing.vector.DistanceMetric;
import org.pangrova.spi.indexing.vector.VectorHybridSearchStrategy;
import org.pangrova.spi.indexing.vector.VectorHybridSearchStrategy.Mode;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link VectorIndexRoutingRule}.
 */
class VectorIndexRoutingRuleTest {

    private static final Instant NOW = Instant.now();

    @Test
    void routesToAnnWhenMatchingIndexExists() {
        VectorTopKCandidate candidate = buildCandidate("test_table", "embedding",
                new float[]{1.0f, 2.0f, 3.0f}, 10, DistanceMetric.COSINE);

        IndexDescriptor cosineIndex = buildVectorIndex("idx_1", "embedding",
                DistanceMetric.COSINE, IndexState.READY);

        Optional<VectorSearchNode> result = VectorIndexRoutingRule.route(
                candidate,
                (table, col) -> List.of(cosineIndex),
                false);

        assertThat(result).isPresent();
        VectorSearchNode node = result.get();
        assertThat(node.tableName()).isEqualTo("test_table");
        assertThat(node.vectorColumn()).isEqualTo("embedding");
        assertThat(node.topK()).isEqualTo(10);
        assertThat(node.metric()).isEqualTo(DistanceMetric.COSINE);
        assertThat(node.indexId()).isEqualTo("idx_1");
        assertThat(node.hybridStrategy().mode()).isEqualTo(Mode.ANN_THEN_FILTER);
    }

    @Test
    void fallsBackToFullScanWhenNoIndex() {
        VectorTopKCandidate candidate = buildCandidate("test_table", "embedding",
                new float[]{1.0f, 2.0f}, 5, DistanceMetric.L2);

        Optional<VectorSearchNode> result = VectorIndexRoutingRule.route(
                candidate,
                (table, col) -> List.of(),
                false);

        assertThat(result).isEmpty();
    }

    @Test
    void fallsBackToFullScanWhenNullIndexList() {
        VectorTopKCandidate candidate = buildCandidate("test_table", "embedding",
                new float[]{1.0f}, 5, DistanceMetric.L2);

        Optional<VectorSearchNode> result = VectorIndexRoutingRule.route(
                candidate,
                (table, col) -> null,
                false);

        assertThat(result).isEmpty();
    }

    @Test
    void forcesExactSearchWhenHintPresent() {
        VectorTopKCandidate candidate = buildCandidate("test_table", "embedding",
                new float[]{1.0f, 2.0f}, 10, DistanceMetric.COSINE);

        IndexDescriptor cosineIndex = buildVectorIndex("idx_1", "embedding",
                DistanceMetric.COSINE, IndexState.READY);

        Optional<VectorSearchNode> result = VectorIndexRoutingRule.route(
                candidate,
                (table, col) -> List.of(cosineIndex),
                true);

        assertThat(result).isEmpty();
    }

    @Test
    void skipsIndexWhenMetricDoesNotMatch() {
        VectorTopKCandidate candidate = buildCandidate("test_table", "embedding",
                new float[]{1.0f, 2.0f}, 10, DistanceMetric.L2);

        IndexDescriptor cosineIndex = buildVectorIndex("idx_1", "embedding",
                DistanceMetric.COSINE, IndexState.READY);

        Optional<VectorSearchNode> result = VectorIndexRoutingRule.route(
                candidate,
                (table, col) -> List.of(cosineIndex),
                false);

        assertThat(result).isEmpty();
    }

    @Test
    void skipsIndexWhenNotReady() {
        VectorTopKCandidate candidate = buildCandidate("test_table", "embedding",
                new float[]{1.0f, 2.0f}, 10, DistanceMetric.COSINE);

        IndexDescriptor buildingIndex = buildVectorIndex("idx_1", "embedding",
                DistanceMetric.COSINE, IndexState.BUILDING);

        Optional<VectorSearchNode> result = VectorIndexRoutingRule.route(
                candidate,
                (table, col) -> List.of(buildingIndex),
                false);

        assertThat(result).isEmpty();
    }

    @Test
    void selectsPreFilterWhenSelectivityBelowThreshold() {
        VectorTopKCandidate candidate = buildCandidate("test_table", "embedding",
                new float[]{1.0f, 2.0f}, 10, DistanceMetric.COSINE);

        IndexDescriptor cosineIndex = buildVectorIndex("idx_1", "embedding",
                DistanceMetric.COSINE, IndexState.READY);

        Optional<VectorSearchNode> result = VectorIndexRoutingRule.route(
                candidate,
                (table, col) -> List.of(cosineIndex),
                false,
                0.05, // 5% selectivity — below 10% threshold
                100_000L,
                List.of("category = 'tech'"));

        assertThat(result).isPresent();
        assertThat(result.get().hybridStrategy().mode()).isEqualTo(Mode.FILTER_THEN_BRUTEFORCE);
    }

    @Test
    void selectsAnnThenFilterWhenSelectivityAboveThreshold() {
        VectorTopKCandidate candidate = buildCandidate("test_table", "embedding",
                new float[]{1.0f, 2.0f}, 10, DistanceMetric.COSINE);

        IndexDescriptor cosineIndex = buildVectorIndex("idx_1", "embedding",
                DistanceMetric.COSINE, IndexState.READY);

        Optional<VectorSearchNode> result = VectorIndexRoutingRule.route(
                candidate,
                (table, col) -> List.of(cosineIndex),
                false,
                0.80, // 80% selectivity — above threshold
                100_000L,
                List.of("created_at > '2024-01-01'"));

        assertThat(result).isPresent();
        VectorHybridSearchStrategy strategy = result.get().hybridStrategy();
        assertThat(strategy.mode()).isEqualTo(Mode.ANN_THEN_FILTER);
        // Over-fetched K should be > topK
        assertThat(strategy.effectiveK()).isGreaterThan(10);
    }

    @Test
    void annThenFilterWithNoPreFiltersUsesExactTopK() {
        VectorTopKCandidate candidate = buildCandidate("test_table", "embedding",
                new float[]{1.0f, 2.0f}, 10, DistanceMetric.COSINE);

        IndexDescriptor cosineIndex = buildVectorIndex("idx_1", "embedding",
                DistanceMetric.COSINE, IndexState.READY);

        Optional<VectorSearchNode> result = VectorIndexRoutingRule.route(
                candidate,
                (table, col) -> List.of(cosineIndex),
                false,
                1.0,
                100_000L,
                List.of());

        assertThat(result).isPresent();
        VectorHybridSearchStrategy strategy = result.get().hybridStrategy();
        assertThat(strategy.mode()).isEqualTo(Mode.ANN_THEN_FILTER);
        assertThat(strategy.effectiveK()).isEqualTo(10);
    }

    @Test
    void containsNoVectorIndexHintDetectsHint() {
        assertThat(VectorIndexRoutingRule.containsNoVectorIndexHint(
                "SELECT /*+ NO_VECTOR_INDEX */ id FROM t ORDER BY cosine_distance(v, 'q') LIMIT 10"))
                .isTrue();
    }

    @Test
    void containsNoVectorIndexHintReturnsFalseForNoHint() {
        assertThat(VectorIndexRoutingRule.containsNoVectorIndexHint(
                "SELECT id FROM t ORDER BY cosine_distance(v, 'q') LIMIT 10"))
                .isFalse();
    }

    @Test
    void containsNoVectorIndexHintReturnsFalseForNull() {
        assertThat(VectorIndexRoutingRule.containsNoVectorIndexHint(null)).isFalse();
    }

    @Test
    void containsNoVectorIndexHintReturnsFalseForEmpty() {
        assertThat(VectorIndexRoutingRule.containsNoVectorIndexHint("")).isFalse();
    }

    @Test
    void containsNoVectorIndexHintIsCaseInsensitive() {
        assertThat(VectorIndexRoutingRule.containsNoVectorIndexHint(
                "SELECT /*+ no_vector_index */ id FROM t")).isTrue();
    }

    @Test
    void containsNoVectorIndexHintIgnoresRegularComments() {
        assertThat(VectorIndexRoutingRule.containsNoVectorIndexHint(
                "SELECT /* NO_VECTOR_INDEX */ id FROM t")).isFalse();
    }

    @Test
    void selectsFirstMatchingReadyIndex() {
        VectorTopKCandidate candidate = buildCandidate("test_table", "embedding",
                new float[]{1.0f, 2.0f}, 10, DistanceMetric.COSINE);

        IndexDescriptor staleIndex = buildVectorIndex("idx_stale", "embedding",
                DistanceMetric.COSINE, IndexState.STALE);
        IndexDescriptor readyIndex = buildVectorIndex("idx_ready", "embedding",
                DistanceMetric.COSINE, IndexState.READY);

        Optional<VectorSearchNode> result = VectorIndexRoutingRule.route(
                candidate,
                (table, col) -> List.of(staleIndex, readyIndex),
                false);

        assertThat(result).isPresent();
        assertThat(result.get().indexId()).isEqualTo("idx_ready");
    }

    private static VectorTopKCandidate buildCandidate(
            String table, String column, float[] vector, int topK, DistanceMetric metric) {
        return new VectorTopKCandidate(table, column, vector, topK, metric);
    }

    private static IndexDescriptor buildVectorIndex(
            String indexId, String column, DistanceMetric metric, IndexState state) {
        return new IndexDescriptor(
                indexId,
                indexId + "_name",
                "catalog",
                "schema",
                "test_table",
                List.of(column),
                IndexType.VECTOR,
                Map.of("metric", metric.name().toLowerCase()),
                state,
                null,
                0,
                null,
                null,
                NOW,
                NOW,
                "default");
    }
}
