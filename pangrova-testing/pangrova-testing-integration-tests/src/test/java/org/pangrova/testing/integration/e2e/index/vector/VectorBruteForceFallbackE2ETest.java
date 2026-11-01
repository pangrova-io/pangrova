package org.pangrova.testing.integration.e2e.index.vector;

import org.pangrova.spi.connector.data.value.TypedRow;
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
 * E2E tests for vector brute-force fallback: queries on VECTOR columns
 * before any index is built must still return correct results via
 * brute-force scan.
 *
 * <p>Validates: Requirements 20.5
 */
@DisplayName("Vector Brute-Force Fallback E2E")
class VectorBruteForceFallbackE2ETest extends SqlE2ETestBase {

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
        tableName = "t_bf_" + method + "_" + System.nanoTime();
        sqlRunner.executeDdl("CREATE TABLE " + fqn()
                + " (id BIGINT, embedding VECTOR(4), label VARCHAR)");

        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, embedding, label) VALUES"
                + " (1, ARRAY[1.0, 0.0, 0.0, 0.0], 'alpha'),"
                + " (2, ARRAY[0.0, 1.0, 0.0, 0.0], 'beta')");
        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, embedding, label) VALUES"
                + " (3, ARRAY[0.0, 0.0, 1.0, 0.0], 'gamma'),"
                + " (4, ARRAY[0.0, 0.0, 0.0, 1.0], 'delta')");
    }

    private String fqn() {
        return FQN_PREFIX + tableName;
    }

    @Nested
    @DisplayName("No Index — Brute-Force Path")
    class NoIndex {

        @Test
        @DisplayName("Distance query without any vector index returns correct results via brute-force")
        void bruteForceWithoutIndex() {
            // No CREATE INDEX — query must still work via brute-force scan
            List<TypedRow> results = sqlRunner.execute(
                    "SELECT id, l2_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) AS dist"
                            + " FROM " + fqn()
                            + " ORDER BY l2_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) ASC"
                            + " LIMIT 4");

            assertThat(results).hasSize(4);
            // Nearest to [1,0,0,0] is id=1 (exact match, distance 0)
            assertThat(results.get(0).getRaw("id").toString()).isEqualTo("1");
        }

        @Test
        @DisplayName("Cosine distance without index returns correct ordering")
        void bruteForceCosineFallback() {
            List<TypedRow> results = sqlRunner.execute(
                    "SELECT id, cosine_distance(embedding, ARRAY[0.0, 1.0, 0.0, 0.0]) AS dist"
                            + " FROM " + fqn()
                            + " ORDER BY cosine_distance(embedding, ARRAY[0.0, 1.0, 0.0, 0.0]) ASC"
                            + " LIMIT 2");

            assertThat(results).hasSizeLessThanOrEqualTo(2);
            // Nearest to [0,1,0,0] is id=2
            assertThat(results.get(0).getRaw("id").toString()).isEqualTo("2");
        }
    }

    @Nested
    @DisplayName("Brute-Force vs Index Consistency")
    class Consistency {

        @Test
        @DisplayName("Brute-force and indexed results agree on nearest neighbor")
        void bruteForceMatchesIndexed() {
            // Query before index
            List<TypedRow> bruteForce = sqlRunner.execute(
                    "SELECT id FROM " + fqn()
                            + " ORDER BY l2_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) ASC"
                            + " LIMIT 1");

            // Build index
            sqlRunner.executeDdl("CREATE INDEX idx_hnsw ON " + fqn()
                    + "(embedding) USING VECTOR WITH ('algorithm'='HNSW', 'metric'='L2')");

            // Query after index
            List<TypedRow> indexed = sqlRunner.execute(
                    "SELECT id FROM " + fqn()
                            + " ORDER BY l2_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) ASC"
                            + " LIMIT 1");

            // Top-1 must agree
            assertThat(indexed.get(0).getRaw("id").toString())
                    .isEqualTo(bruteForce.get(0).getRaw("id").toString());
        }
    }
}
