/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.testing.integration.e2e.ai;

import org.pangrova.types.value.TypedRow;
import org.pangrova.testing.testkit.E2ETest;
import org.pangrova.testing.integration.e2e.PangrovaE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Full-stack E2E tests for AI SQL capabilities through the real
 * coordinator + worker pipeline. Exercises SQL parsing, planning,
 * Substrait lowering, native vectorized execution (SIMD distance
 * functions), and result transport back to the client.
 */
@E2ETest
@DisplayName("AI SQL Read/Write Full-Stack E2E")
class AiSqlReadWriteE2ETest extends PangrovaE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String FQN_PREFIX = CATALOG + "." + SCHEMA + ".";
    private String tableName;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(
                CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeEach
    void assignTableName(TestInfo info) {
        String method = info.getTestMethod().map(m -> m.getName()).orElse("unknown");
        tableName = "t_ai_" + method + "_" + System.nanoTime();
    }

    private String fqn() {
        return FQN_PREFIX + tableName;
    }

    /** Generates one-hot vector INSERT values for ids 1..count with given dimension. */
    private String oneHotInsert(int count, int dim) {
        StringBuilder sb = new StringBuilder("INSERT INTO " + fqn() + " VALUES ");
        for (int i = 1; i <= count; i++) {
            if (i > 1) sb.append(", ");
            sb.append("(").append(i).append(", ARRAY[");
            for (int d = 0; d < dim; d++) {
                if (d > 0) sb.append(",");
                sb.append(d == (i - 1) % dim ? "1.0" : "0.0");
            }
            sb.append("])");
        }
        return sb.toString();
    }

    @Nested
    @DisplayName("Vector Column Operations")
    class VectorColumnOperations {

        @Test
        @DisplayName("Create table with vector column, insert, and select back")
        void createTableWithVectorColumn() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, embedding VECTOR(4))");
            executeDml("INSERT INTO " + fqn() + " VALUES"
                    + " (1, ARRAY[1.0, 0.0, 0.0, 0.0]), (2, ARRAY[0.0, 1.0, 0.0, 0.0]),"
                    + " (3, ARRAY[0.0, 0.0, 1.0, 0.0]), (4, ARRAY[0.0, 0.0, 0.0, 1.0]),"
                    + " (5, ARRAY[0.5, 0.5, 0.5, 0.5])");
            assertQueryReturnsRows("SELECT * FROM " + fqn(), 5);
            executeDdl("DROP TABLE " + fqn());
        }

        @Test
        @DisplayName("Insert vectors and query with L2 distance ORDER BY LIMIT")
        void insertAndQueryVectors() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, embedding VECTOR(4))");
            executeDml(oneHotInsert(10, 4));
            List<TypedRow> rows = executeQuery(
                    "SELECT id, l2_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) AS dist"
                            + " FROM " + fqn()
                            + " ORDER BY l2_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) ASC"
                            + " LIMIT 3");
            assertThat(rows).hasSize(3);
            assertDistancesAscending(rows);
            executeDdl("DROP TABLE " + fqn());
        }

        @Test
        @DisplayName("Cosine distance query returns correctly ordered results")
        void cosineDistanceQuery() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, embedding VECTOR(4))");
            executeDml("INSERT INTO " + fqn() + " VALUES"
                    + " (1, ARRAY[1.0, 0.0, 0.0, 0.0]), (2, ARRAY[0.707, 0.707, 0.0, 0.0]),"
                    + " (3, ARRAY[0.0, 1.0, 0.0, 0.0]), (4, ARRAY[0.0, 0.0, 1.0, 0.0]),"
                    + " (5, ARRAY[0.9, 0.1, 0.0, 0.0])");
            List<TypedRow> rows = executeQuery(
                    "SELECT id, cosine_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) AS dist"
                            + " FROM " + fqn()
                            + " ORDER BY cosine_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) ASC"
                            + " LIMIT 3");
            assertThat(rows).hasSize(3);
            assertDistancesAscending(rows);
            assertThat(((Number) rows.get(0).getRaw("id")).longValue()).isEqualTo(1L);
            executeDdl("DROP TABLE " + fqn());
        }

        @Test
        @DisplayName("Inner product query returns correctly ordered results")
        void innerProductQuery() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, embedding VECTOR(4))");
            executeDml("INSERT INTO " + fqn() + " VALUES"
                    + " (1, ARRAY[3.0, 0.0, 0.0, 0.0]), (2, ARRAY[2.0, 1.0, 0.0, 0.0]),"
                    + " (3, ARRAY[0.0, 3.0, 0.0, 0.0]), (4, ARRAY[1.0, 2.0, 0.0, 0.0])");
            List<TypedRow> rows = executeQuery(
                    "SELECT id, inner_product(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) AS score"
                            + " FROM " + fqn()
                            + " ORDER BY inner_product(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) ASC"
                            + " LIMIT 3");
            assertThat(rows).hasSize(3);
            for (int i = 0; i < rows.size() - 1; i++) {
                double s1 = ((Number) rows.get(i).getRaw("score")).doubleValue();
                double s2 = ((Number) rows.get(i + 1).getRaw("score")).doubleValue();
                assertThat(s1).isLessThanOrEqualTo(s2 + 1e-6);
            }
            executeDdl("DROP TABLE " + fqn());
        }
    }

    @Nested
    @DisplayName("Vector Index Lifecycle")
    class VectorIndexLifecycle {

        @Test
        @DisplayName("Create index, verify via SHOW INDEXES, query with index")
        void createAndQueryVectorIndex() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, embedding VECTOR(4))");
            executeDml(oneHotInsert(20, 4));
            executeDdl("CREATE INDEX idx_ai_hnsw ON " + fqn()
                    + "(embedding) USING VECTOR WITH ('algorithm'='HNSW', 'metric'='L2')");
            // CREATE returns after durable submission. Verify that a real Worker
            // built and published the generation before exercising retrieval.
            org.awaitility.Awaitility.await("HNSW index published")
                    .atMost(Duration.ofSeconds(60))
                    .pollInterval(Duration.ofMillis(500))
                    .untilAsserted(() -> assertThat(executeQuery(
                            "SHOW INDEXES FROM " + fqn()).stream()
                            .filter(row -> "idx_ai_hnsw".equals(
                                    row.getRaw("index_name")))
                            .map(row -> row.getRaw("state")))
                            .containsExactly("READY"));
            List<TypedRow> rows = executeQuery(
                    "SELECT id, l2_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) AS dist"
                            + " FROM " + fqn()
                            + " ORDER BY l2_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) ASC"
                            + " LIMIT 5");
            assertThat(rows).hasSize(5);
            assertDistancesAscending(rows);
            executeDdl("DROP INDEX idx_ai_hnsw ON " + fqn());
            executeDdl("DROP TABLE " + fqn());
        }

        @Test
        @DisplayName("Index rebuild incorporates newly inserted data")
        void indexRebuildAndQuery() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, embedding VECTOR(4))");
            executeDml("INSERT INTO " + fqn() + " VALUES"
                    + " (1, ARRAY[1.0, 0.0, 0.0, 0.0]), (2, ARRAY[0.0, 1.0, 0.0, 0.0]),"
                    + " (3, ARRAY[0.0, 0.0, 1.0, 0.0])");
            executeDdl("CREATE INDEX idx_ai_rebuild ON " + fqn()
                    + "(embedding) USING VECTOR WITH ('algorithm'='HNSW', 'metric'='L2')");
            org.awaitility.Awaitility.await("initial HNSW generation published")
                    .atMost(Duration.ofSeconds(60))
                    .pollInterval(Duration.ofMillis(500))
                    .untilAsserted(() -> assertThat(executeQuery("SHOW INDEXES FROM " + fqn()).stream()
                            .filter(row -> "idx_ai_rebuild".equals(row.getRaw("index_name")))
                            .map(row -> row.getRaw("state")))
                            .containsExactly("READY"));
            executeDml("INSERT INTO " + fqn() + " VALUES"
                    + " (4, ARRAY[0.99, 0.01, 0.0, 0.0]), (5, ARRAY[0.98, 0.02, 0.0, 0.0])");
            executeDdl("ALTER INDEX idx_ai_rebuild ON " + fqn() + " REBUILD");
            List<TypedRow> rows = executeQuery(
                    "SELECT id, l2_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) AS dist"
                            + " FROM " + fqn()
                            + " ORDER BY l2_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) ASC"
                            + " LIMIT 3");
            assertThat(rows).hasSize(3);
            long closestId = ((Number) rows.get(0).getRaw("id")).longValue();
            assertThat(closestId).isIn(1L, 4L, 5L);
            executeDdl("DROP INDEX idx_ai_rebuild ON " + fqn());
            executeDdl("DROP TABLE " + fqn());
        }

        @Test
        @DisplayName("Drop and recreate index with same name")
        void dropAndRecreateIndex() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, embedding VECTOR(4))");
            executeDml("INSERT INTO " + fqn() + " VALUES"
                    + " (1, ARRAY[1.0, 0.0, 0.0, 0.0]), (2, ARRAY[0.0, 1.0, 0.0, 0.0]),"
                    + " (3, ARRAY[0.0, 0.0, 1.0, 0.0])");
            executeDdl("CREATE INDEX idx_ai_recreate ON " + fqn()
                    + "(embedding) USING VECTOR WITH ('algorithm'='HNSW', 'metric'='L2')");
            executeDdl("DROP INDEX idx_ai_recreate ON " + fqn());
            executeDdl("CREATE INDEX idx_ai_recreate ON " + fqn()
                    + "(embedding) USING VECTOR WITH ('algorithm'='HNSW', 'metric'='L2')");
            List<TypedRow> indexes = executeQuery("SHOW INDEXES FROM " + fqn());
            assertThat(indexes).isNotEmpty();
            List<TypedRow> rows = executeQuery(
                    "SELECT id, l2_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) AS dist"
                            + " FROM " + fqn()
                            + " ORDER BY l2_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) ASC"
                            + " LIMIT 2");
            assertThat(rows).hasSize(2);
            assertThat(((Number) rows.get(0).getRaw("id")).longValue()).isEqualTo(1L);
            executeDdl("DROP INDEX idx_ai_recreate ON " + fqn());
            executeDdl("DROP TABLE " + fqn());
        }
    }

    @Nested
    @DisplayName("Filtered Vector Search")
    class FilteredVectorSearch {

        @Test
        @DisplayName("WHERE clause filters combined with vector distance ordering")
        void whereClauseWithVectorOrder() {
            executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, category VARCHAR, embedding VECTOR(4))");
            executeDml("INSERT INTO " + fqn() + " VALUES"
                    + " (1, 'A', ARRAY[1.0, 0.0, 0.0, 0.0]),"
                    + " (2, 'B', ARRAY[0.9, 0.1, 0.0, 0.0]),"
                    + " (3, 'A', ARRAY[0.8, 0.2, 0.0, 0.0]),"
                    + " (4, 'B', ARRAY[0.0, 1.0, 0.0, 0.0]),"
                    + " (5, 'A', ARRAY[0.0, 0.0, 1.0, 0.0]),"
                    + " (6, 'A', ARRAY[0.95, 0.05, 0.0, 0.0])");
            List<TypedRow> rows = executeQuery(
                    "SELECT id, l2_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) AS dist"
                            + " FROM " + fqn() + " WHERE category = 'A'"
                            + " ORDER BY l2_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) ASC"
                            + " LIMIT 3");
            assertThat(rows).hasSize(3);
            assertDistancesAscending(rows);
            assertThat(((Number) rows.get(0).getRaw("id")).longValue()).isEqualTo(1L);
            executeDdl("DROP TABLE " + fqn());
        }

        @Test
        @DisplayName("Multiple WHERE filters with vector distance ordering")
        void multipleFilters() {
            executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, category VARCHAR, score DOUBLE, embedding VECTOR(4))");
            executeDml("INSERT INTO " + fqn() + " VALUES"
                    + " (1, 'A', 10.0, ARRAY[1.0, 0.0, 0.0, 0.0]),"
                    + " (2, 'A',  3.0, ARRAY[0.9, 0.1, 0.0, 0.0]),"
                    + " (3, 'B',  8.0, ARRAY[0.8, 0.2, 0.0, 0.0]),"
                    + " (4, 'A',  7.0, ARRAY[0.7, 0.3, 0.0, 0.0]),"
                    + " (5, 'A',  8.0, ARRAY[0.85, 0.15, 0.0, 0.0]),"
                    + " (6, 'A',  9.0, ARRAY[0.0, 1.0, 0.0, 0.0])");
            List<TypedRow> rows = executeQuery(
                    "SELECT id, l2_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) AS dist"
                            + " FROM " + fqn()
                            + " WHERE category = 'A' AND score > 5.0"
                            + " ORDER BY l2_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) ASC"
                            + " LIMIT 3");
            assertThat(rows).hasSize(3);
            assertDistancesAscending(rows);
            executeDdl("DROP TABLE " + fqn());
        }
    }

    @Nested
    @DisplayName("Distance Function Precision")
    class DistanceFunctionPrecision {

        @Test
        @DisplayName("L2 distance of known vectors matches expected value")
        void l2DistanceKnownVectors() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, embedding VECTOR(3))");
            executeDml("INSERT INTO " + fqn() + " VALUES"
                    + " (1, ARRAY[3.0, 0.0, 0.0]), (2, ARRAY[0.0, 4.0, 0.0])");
            List<TypedRow> rows = executeQuery(
                    "SELECT l2_distance(embedding, ARRAY[0.0, 0.0, 0.0]) AS dist"
                            + " FROM " + fqn()
                            + " ORDER BY l2_distance(embedding, ARRAY[0.0, 0.0, 0.0]) ASC");
            assertThat(rows).hasSize(2);
            assertThat(((Number) rows.get(0).getRaw("dist")).doubleValue())
                    .isCloseTo(3.0, within(0.01));
            assertThat(((Number) rows.get(1).getRaw("dist")).doubleValue())
                    .isCloseTo(4.0, within(0.01));
            executeDdl("DROP TABLE " + fqn());
        }

        @Test
        @DisplayName("Cosine distance of orthogonal vectors is approximately 1.0")
        void cosineDistanceOrthogonal() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, embedding VECTOR(3))");
            executeDml("INSERT INTO " + fqn() + " VALUES"
                    + " (1, ARRAY[1.0, 0.0, 0.0]), (2, ARRAY[0.0, 1.0, 0.0])");
            List<TypedRow> rows = executeQuery(
                    "SELECT cosine_distance(embedding, ARRAY[1.0, 0.0, 0.0]) AS dist"
                            + " FROM " + fqn() + " WHERE id = 2");
            assertThat(rows).hasSize(1);
            assertThat(((Number) rows.get(0).getRaw("dist")).doubleValue())
                    .isCloseTo(1.0, within(0.01));
            executeDdl("DROP TABLE " + fqn());
        }

        @Test
        @DisplayName("Cosine distance of identical vectors is approximately 0.0")
        void cosineDistanceIdentical() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, embedding VECTOR(3))");
            executeDml("INSERT INTO " + fqn() + " VALUES (1, ARRAY[0.6, 0.8, 0.0])");
            List<TypedRow> rows = executeQuery(
                    "SELECT cosine_distance(embedding, ARRAY[0.6, 0.8, 0.0]) AS dist"
                            + " FROM " + fqn() + " WHERE id = 1");
            assertThat(rows).hasSize(1);
            assertThat(((Number) rows.get(0).getRaw("dist")).doubleValue())
                    .isCloseTo(0.0, within(0.01));
            executeDdl("DROP TABLE " + fqn());
        }
    }

    private void assertDistancesAscending(List<TypedRow> rows) {
        for (int i = 0; i < rows.size() - 1; i++) {
            double d1 = ((Number) rows.get(i).getRaw("dist")).doubleValue();
            double d2 = ((Number) rows.get(i + 1).getRaw("dist")).doubleValue();
            assertThat(d1).isLessThanOrEqualTo(d2 + 1e-6);
        }
    }
}
