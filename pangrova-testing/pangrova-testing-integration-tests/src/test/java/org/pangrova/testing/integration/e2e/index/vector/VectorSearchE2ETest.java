package org.pangrova.testing.integration.e2e.index.vector;

import org.pangrova.connector.spi.data.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E tests for vector search: build HNSW index, execute ANN query,
 * verify top-K results are approximate nearest neighbors with recall > 0.8.
 *
 * <p>Uses a dataset of 20 vectors in 4D space with known ground-truth nearest
 * neighbors computed via brute-force, then verifies the HNSW index ANN query
 * achieves acceptable recall.
 *
 * <p>Validates: Requirements R12.9
 */
@DisplayName("Vector Search E2E — HNSW ANN Recall Verification")
class VectorSearchE2ETest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String FQN_PREFIX = CATALOG + "." + SCHEMA + ".";
    private static final int TOP_K = 5;
    private static final double MIN_RECALL = 0.8;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    private String tableName;

    /**
     * Pre-defined 20 vectors in 4D space, spread across multiple inserts
     * to create multiple data files for realistic index evaluation.
     */
    private static final double[][] VECTORS = {
        {1.0, 0.0, 0.0, 0.0},  // id=1
        {0.9, 0.1, 0.0, 0.0},  // id=2
        {0.8, 0.2, 0.0, 0.0},  // id=3
        {0.0, 1.0, 0.0, 0.0},  // id=4
        {0.0, 0.9, 0.1, 0.0},  // id=5
        {0.0, 0.0, 1.0, 0.0},  // id=6
        {0.0, 0.0, 0.9, 0.1},  // id=7
        {0.0, 0.0, 0.0, 1.0},  // id=8
        {0.5, 0.5, 0.0, 0.0},  // id=9
        {0.5, 0.0, 0.5, 0.0},  // id=10
        {0.0, 0.5, 0.5, 0.0},  // id=11
        {0.0, 0.5, 0.0, 0.5},  // id=12
        {0.3, 0.3, 0.3, 0.1},  // id=13
        {0.1, 0.1, 0.1, 0.7},  // id=14
        {0.7, 0.1, 0.1, 0.1},  // id=15
        {0.1, 0.7, 0.1, 0.1},  // id=16
        {0.1, 0.1, 0.7, 0.1},  // id=17
        {0.25, 0.25, 0.25, 0.25}, // id=18
        {0.6, 0.4, 0.0, 0.0},  // id=19
        {0.0, 0.0, 0.4, 0.6},  // id=20
    };

    /** Query vector for ANN search. */
    private static final double[] QUERY_VECTOR = {0.9, 0.1, 0.0, 0.0};

    @BeforeAll
    static void setupCatalog() {
        registerTestConnectors(List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString())));
    }

    @BeforeEach
    void createVectorTable(TestInfo info) {
        String method = info.getTestMethod().map(m -> m.getName()).orElse("unknown");
        tableName = "t_vsearch_" + method + "_" + System.nanoTime();
        sqlRunner.executeDdl("CREATE TABLE " + fqn()
                + " (id BIGINT, embedding VECTOR(4))");

        // Insert in batches of 5 to create multiple data files
        for (int batch = 0; batch < 4; batch++) {
            StringBuilder sb = new StringBuilder();
            sb.append("INSERT INTO ").append(fqn()).append(" (id, embedding) VALUES ");
            for (int i = 0; i < 5; i++) {
                int idx = batch * 5 + i;
                if (i > 0) sb.append(", ");
                sb.append("(").append(idx + 1).append(", ARRAY[");
                sb.append(formatVector(VECTORS[idx]));
                sb.append("])");
            }
            sqlRunner.execute(sb.toString());
        }
    }

    private String fqn() {
        return FQN_PREFIX + tableName;
    }

    private static String formatVector(double[] v) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < v.length; i++) {
            if (i > 0) sb.append(", ");
            sb.append(String.format(Locale.US, "%.1f", v[i]));
        }
        return sb.toString();
    }

    private static String formatQueryVector() {
        return formatVector(QUERY_VECTOR);
    }

    /**
     * Compute ground-truth top-K nearest neighbors by brute-force L2 distance.
     */
    private Set<Integer> groundTruthTopK() {
        List<int[]> ranked = new ArrayList<>();
        for (int i = 0; i < VECTORS.length; i++) {
            ranked.add(new int[]{i + 1, 0});
        }
        // Sort by L2 distance to QUERY_VECTOR
        ranked.sort(Comparator.comparingDouble(a -> l2Distance(VECTORS[a[0] - 1], QUERY_VECTOR)));
        return ranked.stream()
                .limit(TOP_K)
                .map(a -> a[0])
                .collect(Collectors.toSet());
    }

    private static double l2Distance(double[] a, double[] b) {
        double sum = 0;
        for (int i = 0; i < a.length; i++) {
            double diff = a[i] - b[i];
            sum += diff * diff;
        }
        return Math.sqrt(sum);
    }

    @Nested
    @DisplayName("HNSW ANN Recall")
    class HnswAnnRecall {

        @Test
        @DisplayName("HNSW index ANN query achieves recall > 0.8 against brute-force ground truth")
        void hnswRecallAboveThreshold() {
            // Build HNSW index
            sqlRunner.executeDdl("CREATE INDEX idx_hnsw ON " + fqn()
                    + "(embedding) USING VECTOR WITH ('algorithm'='HNSW', 'metric'='L2')");

            // Execute ANN query via index
            List<TypedRow> annResults = sqlRunner.execute(
                    "SELECT id, l2_distance(embedding, ARRAY[" + formatQueryVector() + "]) AS dist"
                            + " FROM " + fqn()
                            + " ORDER BY l2_distance(embedding, ARRAY[" + formatQueryVector() + "]) ASC"
                            + " LIMIT " + TOP_K);

            assertThat(annResults).hasSizeLessThanOrEqualTo(TOP_K);
            assertThat(annResults).isNotEmpty();

            // Compute recall: intersection of ANN results with ground truth
            Set<Integer> groundTruth = groundTruthTopK();
            Set<Integer> annIds = annResults.stream()
                    .map(r -> Integer.parseInt(r.getRaw("id").toString()))
                    .collect(Collectors.toSet());

            long hits = annIds.stream().filter(groundTruth::contains).count();
            double recall = (double) hits / TOP_K;

            assertThat(recall)
                    .as("HNSW recall (ANN top-%d vs brute-force top-%d): %.2f, ground truth=%s, ann=%s",
                            TOP_K, TOP_K, recall, groundTruth, annIds)
                    .isGreaterThanOrEqualTo(MIN_RECALL);
        }

        @Test
        @DisplayName("HNSW ANN top-1 result is the exact nearest neighbor")
        void hnswTop1IsExactNearest() {
            sqlRunner.executeDdl("CREATE INDEX idx_hnsw ON " + fqn()
                    + "(embedding) USING VECTOR WITH ('algorithm'='HNSW', 'metric'='L2')");

            List<TypedRow> results = sqlRunner.execute(
                    "SELECT id FROM " + fqn()
                            + " ORDER BY l2_distance(embedding, ARRAY[" + formatQueryVector() + "]) ASC"
                            + " LIMIT 1");

            assertThat(results).hasSize(1);
            // Query vector [0.9, 0.1, 0.0, 0.0] — exact match is id=2 (same vector)
            assertThat(results.get(0).getRaw("id").toString()).isEqualTo("2");
        }
    }

    @Nested
    @DisplayName("Brute-Force Baseline Consistency")
    class BruteForceBaseline {

        @Test
        @DisplayName("Brute-force query without index returns correct top-K ordering")
        void bruteForceReturnsCorrectOrder() {
            // No index — brute-force scan
            List<TypedRow> results = sqlRunner.execute(
                    "SELECT id, l2_distance(embedding, ARRAY[" + formatQueryVector() + "]) AS dist"
                            + " FROM " + fqn()
                            + " ORDER BY l2_distance(embedding, ARRAY[" + formatQueryVector() + "]) ASC"
                            + " LIMIT " + TOP_K);

            assertThat(results).hasSize(TOP_K);

            Set<Integer> bruteForceIds = results.stream()
                    .map(r -> Integer.parseInt(r.getRaw("id").toString()))
                    .collect(Collectors.toSet());
            Set<Integer> groundTruth = groundTruthTopK();

            // Brute-force should match ground truth exactly
            assertThat(bruteForceIds).isEqualTo(groundTruth);
        }

        @Test
        @DisplayName("HNSW recall matches or exceeds brute-force baseline")
        void hnswRecallMatchesBruteForce() {
            // Get brute-force results first
            List<TypedRow> bruteForce = sqlRunner.execute(
                    "SELECT id FROM " + fqn()
                            + " ORDER BY l2_distance(embedding, ARRAY[" + formatQueryVector() + "]) ASC"
                            + " LIMIT " + TOP_K);

            // Build index
            sqlRunner.executeDdl("CREATE INDEX idx_hnsw ON " + fqn()
                    + "(embedding) USING VECTOR WITH ('algorithm'='HNSW', 'metric'='L2')");

            // Get indexed results
            List<TypedRow> indexed = sqlRunner.execute(
                    "SELECT id FROM " + fqn()
                            + " ORDER BY l2_distance(embedding, ARRAY[" + formatQueryVector() + "]) ASC"
                            + " LIMIT " + TOP_K);

            Set<String> bruteForceIds = bruteForce.stream()
                    .map(r -> r.getRaw("id").toString())
                    .collect(Collectors.toSet());
            Set<String> indexedIds = indexed.stream()
                    .map(r -> r.getRaw("id").toString())
                    .collect(Collectors.toSet());

            long overlap = indexedIds.stream().filter(bruteForceIds::contains).count();
            double recall = (double) overlap / TOP_K;

            assertThat(recall)
                    .as("HNSW vs brute-force recall: %.2f", recall)
                    .isGreaterThanOrEqualTo(MIN_RECALL);
        }
    }

    @Nested
    @DisplayName("Cosine Metric ANN")
    class CosineMetricAnn {

        @Test
        @DisplayName("HNSW with cosine metric returns approximate nearest neighbors")
        void cosineAnnReturnsResults() {
            sqlRunner.executeDdl("CREATE INDEX idx_cos ON " + fqn()
                    + "(embedding) USING VECTOR WITH ('algorithm'='HNSW', 'metric'='COSINE')");

            List<TypedRow> results = sqlRunner.execute(
                    "SELECT id, cosine_distance(embedding, ARRAY[" + formatQueryVector() + "]) AS dist"
                            + " FROM " + fqn()
                            + " ORDER BY cosine_distance(embedding, ARRAY[" + formatQueryVector() + "]) ASC"
                            + " LIMIT " + TOP_K);

            assertThat(results).isNotEmpty();
            assertThat(results).hasSizeLessThanOrEqualTo(TOP_K);

            // Top-1 should be the vector most similar in direction to query
            // [0.9, 0.1, 0.0, 0.0] — closest by cosine is id=2 [0.9, 0.1, 0.0, 0.0]
            assertThat(results.get(0).getRaw("id").toString()).isEqualTo("2");
        }
    }
}
