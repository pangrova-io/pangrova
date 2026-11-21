package org.pangrova.testing.integration.e2e.index.vector;

import org.pangrova.types.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.pangrova.testing.integration.e2e.runner.QueryError;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E tests for vector index lifecycle: CREATE TABLE with VECTOR column,
 * INSERT vector data, CREATE INDEX USING VECTOR (HNSW/IVFPQ), ANN query,
 * and result verification.
 *
 * <p>Full SQL pipeline: Parser → Planner → Substrait → Native → Transport → Client.
 *
 * <p>Validates: Requirements 20.5
 */
@DisplayName("Vector Index E2E")
class VectorIndexE2ETest extends SqlE2ETestBase {

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
        tableName = "t_vec_" + method + "_" + System.nanoTime();
        sqlRunner.executeDdl("CREATE TABLE " + fqn()
                + " (id BIGINT, embedding VECTOR(4), label VARCHAR)");

        // Insert vectors in separate batches to create multiple data files
        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, embedding, label) VALUES"
                + " (1, ARRAY[1.0, 0.0, 0.0, 0.0], 'north'),"
                + " (2, ARRAY[0.0, 1.0, 0.0, 0.0], 'east')");
        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, embedding, label) VALUES"
                + " (3, ARRAY[0.0, 0.0, 1.0, 0.0], 'south'),"
                + " (4, ARRAY[0.0, 0.0, 0.0, 1.0], 'west')");
        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, embedding, label) VALUES"
                + " (5, ARRAY[0.5, 0.5, 0.0, 0.0], 'northeast'),"
                + " (6, ARRAY[0.0, 0.5, 0.5, 0.0], 'southeast')");
    }

    private String fqn() {
        return FQN_PREFIX + tableName;
    }

    @Nested
    @DisplayName("HNSW Index")
    class HnswIndex {

        @Test
        @DisplayName("CREATE INDEX USING VECTOR with HNSW and SHOW INDEXES lists it")
        void createHnswIndex() {
            sqlRunner.executeDdl("CREATE INDEX idx_hnsw ON " + fqn()
                    + "(embedding) USING VECTOR WITH ('algorithm'='HNSW', 'metric'='L2')");

            List<TypedRow> indexes = sqlRunner.execute("SHOW INDEXES FROM " + fqn());
            assertThat(indexes).isNotEmpty();
            TypedRow idx = indexes.stream()
                    .filter(r -> "idx_hnsw".equals(r.getRaw("index_name").toString()))
                    .findFirst().orElseThrow();
            assertThat(idx.getRaw("index_type").toString()).isEqualTo("VECTOR");
        }

        @Test
        @DisplayName("ANN query with L2 distance returns top-K nearest neighbors")
        void annQueryL2() {
            sqlRunner.executeDdl("CREATE INDEX idx_hnsw ON " + fqn()
                    + "(embedding) USING VECTOR WITH ('algorithm'='HNSW', 'metric'='L2')");

            List<TypedRow> results = sqlRunner.execute(
                    "SELECT id, label, l2_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) AS dist"
                            + " FROM " + fqn()
                            + " ORDER BY l2_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) ASC"
                            + " LIMIT 3");

            assertThat(results).hasSizeLessThanOrEqualTo(3);
            // Nearest to [1,0,0,0] should be id=1 (exact match, distance ~0)
            assertThat(results.get(0).getRaw("id").toString()).isEqualTo("1");
        }

        @Test
        @DisplayName("ANN query with cosine distance")
        void annQueryCosine() {
            sqlRunner.executeDdl("CREATE INDEX idx_cos ON " + fqn()
                    + "(embedding) USING VECTOR WITH ('algorithm'='HNSW', 'metric'='COSINE')");

            List<TypedRow> results = sqlRunner.execute(
                    "SELECT id, cosine_distance(embedding, ARRAY[0.0, 1.0, 0.0, 0.0]) AS dist"
                            + " FROM " + fqn()
                            + " ORDER BY cosine_distance(embedding, ARRAY[0.0, 1.0, 0.0, 0.0]) ASC"
                            + " LIMIT 2");

            assertThat(results).hasSizeLessThanOrEqualTo(2);
            // Nearest to [0,1,0,0] should be id=2 (exact match)
            assertThat(results.get(0).getRaw("id").toString()).isEqualTo("2");
        }
    }

    @Nested
    @DisplayName("IVFPQ Index")
    class IvfpqIndex {

        @Test
        @DisplayName("CREATE INDEX USING VECTOR with IVFPQ")
        void createIvfpqIndex() {
            sqlRunner.executeDdl("CREATE INDEX idx_ivfpq ON " + fqn()
                    + "(embedding) USING VECTOR WITH ('algorithm'='IVFPQ', 'metric'='L2',"
                    + " 'ivfpq.nlist'='2', 'ivfpq.m'='2', 'ivfpq.nbits'='8')");

            List<TypedRow> indexes = sqlRunner.execute("SHOW INDEXES FROM " + fqn());
            TypedRow idx = indexes.stream()
                    .filter(r -> "idx_ivfpq".equals(r.getRaw("index_name").toString()))
                    .findFirst().orElseThrow();
            assertThat(idx.getRaw("index_type").toString()).isEqualTo("VECTOR");
        }

        @Test
        @DisplayName("ANN query with IVFPQ returns results")
        void annQueryIvfpq() {
            sqlRunner.executeDdl("CREATE INDEX idx_ivfpq ON " + fqn()
                    + "(embedding) USING VECTOR WITH ('algorithm'='IVFPQ', 'metric'='L2',"
                    + " 'ivfpq.nlist'='2', 'ivfpq.m'='2', 'ivfpq.nbits'='8')");

            List<TypedRow> results = sqlRunner.execute(
                    "SELECT id, l2_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) AS dist"
                            + " FROM " + fqn()
                            + " ORDER BY l2_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) ASC"
                            + " LIMIT 3");

            assertThat(results).isNotEmpty();
            assertThat(results).hasSizeLessThanOrEqualTo(3);
        }
    }

    @Nested
    @DisplayName("DDL Validation")
    class DdlValidation {

        @Test
        @DisplayName("CREATE VECTOR INDEX on non-VECTOR column fails")
        void vectorIndexOnNonVectorColumn() {
            QueryError error = sqlRunner.executeExpectingError(
                    "CREATE INDEX idx_bad ON " + fqn()
                            + "(label) USING VECTOR WITH ('algorithm'='HNSW', 'metric'='L2')");
            assertThat(error.message().toLowerCase())
                    .containsAnyOf("vector", "type", "incompatible");
        }

        @Test
        @DisplayName("DROP VECTOR INDEX removes it from SHOW INDEXES")
        void dropVectorIndex() {
            sqlRunner.executeDdl("CREATE INDEX idx_drop ON " + fqn()
                    + "(embedding) USING VECTOR WITH ('algorithm'='HNSW', 'metric'='L2')");
            sqlRunner.executeDdl("DROP INDEX idx_drop ON " + fqn());

            List<TypedRow> indexes = sqlRunner.execute("SHOW INDEXES FROM " + fqn());
            boolean found = indexes.stream()
                    .anyMatch(r -> "idx_drop".equals(r.getRaw("index_name").toString())
                            && !"DROPPED".equals(r.getRaw("state").toString()));
            assertThat(found).isFalse();
        }
    }
}
