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

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E tests for hybrid vector search: pre-filter + ANN combined queries.
 * Tests both ANN_THEN_FILTER and FILTER_THEN_BRUTEFORCE strategies.
 *
 * <p>Validates: Requirements 20.5
 */
@DisplayName("Vector Hybrid Search E2E")
class VectorHybridSearchE2ETest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String FQN_PREFIX = CATALOG + "." + SCHEMA + ".";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    private String tableName;

    @BeforeAll
    static void setupCatalog() {
        registerTestConnectors(List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString())));
    }

    @BeforeEach
    void createVectorTable(TestInfo info) {
        String method = info.getTestMethod().map(m -> m.getName()).orElse("unknown");
        tableName = "t_hybrid_" + method + "_" + System.nanoTime();
        sqlRunner.executeDdl("CREATE TABLE " + fqn()
                + " (id BIGINT, embedding VECTOR(4), category VARCHAR, score DOUBLE)");

        // Insert data with distinct categories for filter testing
        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, embedding, category, score) VALUES"
                + " (1, ARRAY[1.0, 0.0, 0.0, 0.0], 'A', 90.0),"
                + " (2, ARRAY[0.9, 0.1, 0.0, 0.0], 'A', 85.0),"
                + " (3, ARRAY[0.0, 1.0, 0.0, 0.0], 'B', 70.0)");
        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, embedding, category, score) VALUES"
                + " (4, ARRAY[0.0, 0.0, 1.0, 0.0], 'B', 60.0),"
                + " (5, ARRAY[0.0, 0.0, 0.0, 1.0], 'C', 50.0),"
                + " (6, ARRAY[0.5, 0.5, 0.0, 0.0], 'A', 80.0)");

        sqlRunner.executeDdl("CREATE INDEX idx_vec ON " + fqn()
                + "(embedding) USING VECTOR WITH ('algorithm'='HNSW', 'metric'='L2')");
    }

    private String fqn() {
        return FQN_PREFIX + tableName;
    }

    @Nested
    @DisplayName("Pre-filter + ANN")
    class PreFilterAnn {

        @Test
        @DisplayName("WHERE category = 'A' + ANN returns only category A neighbors")
        void filterThenAnn() {
            List<TypedRow> results = sqlRunner.execute(
                    "SELECT id, category, l2_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) AS dist"
                            + " FROM " + fqn()
                            + " WHERE category = 'A'"
                            + " ORDER BY l2_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) ASC"
                            + " LIMIT 3");

            assertThat(results).isNotEmpty();
            // All results must be category A
            for (TypedRow row : results) {
                assertThat(row.getRaw("category").toString()).isEqualTo("A");
            }
            // Nearest category-A vector to [1,0,0,0] is id=1
            assertThat(results.get(0).getRaw("id").toString()).isEqualTo("1");
        }

        @Test
        @DisplayName("WHERE score > 75 + ANN filters low-score vectors")
        void filterByScoreThenAnn() {
            List<TypedRow> results = sqlRunner.execute(
                    "SELECT id, score, l2_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) AS dist"
                            + " FROM " + fqn()
                            + " WHERE score > 75.0"
                            + " ORDER BY l2_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) ASC"
                            + " LIMIT 5");

            assertThat(results).isNotEmpty();
            for (TypedRow row : results) {
                double score = ((Number) row.getRaw("score")).doubleValue();
                assertThat(score).isGreaterThan(75.0);
            }
        }
    }

    @Nested
    @DisplayName("Result Correctness")
    class ResultCorrectness {

        @Test
        @DisplayName("Hybrid search results are subset of unfiltered ANN results")
        void hybridSubsetOfUnfiltered() {
            List<TypedRow> unfiltered = sqlRunner.execute(
                    "SELECT id FROM " + fqn()
                            + " ORDER BY l2_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) ASC"
                            + " LIMIT 6");

            List<TypedRow> filtered = sqlRunner.execute(
                    "SELECT id FROM " + fqn()
                            + " WHERE category = 'A'"
                            + " ORDER BY l2_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) ASC"
                            + " LIMIT 6");

            // Filtered results must be a subset of unfiltered
            List<String> unfilteredIds = unfiltered.stream()
                    .map(r -> r.getRaw("id").toString()).toList();
            for (TypedRow row : filtered) {
                assertThat(unfilteredIds).contains(row.getRaw("id").toString());
            }
        }
    }
}
