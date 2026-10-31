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

import org.pangrova.connector.spi.data.value.TypedRow;
import org.pangrova.testing.testkit.E2ETest;
import org.pangrova.testing.integration.e2e.PangrovaE2ETestBase;
import org.pangrova.testing.integration.e2e.E2ETestFramework;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Full-stack E2E tests for concurrent and large-data AI/vector workloads.
 *
 * <p>Validates that the coordinator + worker pipeline handles:
 * <ul>
 *   <li>Parallel vector search queries without corruption</li>
 *   <li>Concurrent read/write operations on vector tables</li>
 *   <li>Large vector datasets (1000+ rows)</li>
 *   <li>High-dimensional vector columns (128-d)</li>
 * </ul>
 */
@E2ETest
@DisplayName("AI Concurrent Load E2E")
class AiConcurrentLoadE2ETest extends PangrovaE2ETestBase {

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
        tableName = "t_ai_load_" + method + "_" + System.nanoTime();
    }

    private String fqn() {
        return FQN_PREFIX + tableName;
    }

    // -----------------------------------------------------------------------
    // 1. Concurrent Vector Queries
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Concurrent Vector Queries")
    class ConcurrentVectorQueries {

        @Test
        @DisplayName("Parallel vector searches all return correct results")
        void parallelVectorSearches() throws Exception {
            executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, embedding VECTOR(4))");

            // Insert 100 vectors
            for (int batch = 0; batch < 10; batch++) {
                StringBuilder sb = new StringBuilder("INSERT INTO " + fqn() + " VALUES ");
                for (int i = 0; i < 10; i++) {
                    int id = batch * 10 + i + 1;
                    if (i > 0) {
                        sb.append(", ");
                    }
                    sb.append("(").append(id).append(", ARRAY[");
                    for (int d = 0; d < 4; d++) {
                        if (d > 0) {
                            sb.append(",");
                        }
                        double val = Math.sin(id * 0.1 + d * 0.5);
                        sb.append(String.format("%.6f", val));
                    }
                    sb.append("])");
                }
                executeDml(sb.toString());
            }

            executeDdl("CREATE INDEX idx_conc ON " + fqn()
                    + "(embedding) USING VECTOR WITH ('algorithm'='HNSW', 'metric'='L2')");

            int concurrency = 5;
            List<CompletableFuture<Void>> futures = new ArrayList<>();

            for (int q = 0; q < concurrency; q++) {
                final int queryIdx = q;
                futures.add(CompletableFuture.runAsync(() -> {
                    StringBuilder queryVec = new StringBuilder("ARRAY[");
                    for (int d = 0; d < 4; d++) {
                        if (d > 0) {
                            queryVec.append(",");
                        }
                        queryVec.append(String.format("%.6f",
                                Math.sin(queryIdx * 0.3 + d * 0.5)));
                    }
                    queryVec.append("]");

                    String sql = "SELECT id, l2_distance(embedding, " + queryVec
                            + ") AS dist FROM " + fqn()
                            + " ORDER BY l2_distance(embedding, " + queryVec
                            + ") ASC LIMIT 5";
                    E2ETestFramework.QueryResult result = framework.executeQuery(sql);
                    assertThat(result.success())
                            .as("Concurrent vector query %s failed: %s", queryIdx,
                                    result.errorMessage())
                            .isTrue();
                    assertThat(result.rows()).hasSize(5);
                }));
            }

            CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
                    .get(120, TimeUnit.SECONDS);

            executeDdl("DROP INDEX idx_conc ON " + fqn());
            executeDdl("DROP TABLE " + fqn());
        }

        @Test
        @DisplayName("Concurrent reads while writes are happening")
        void readWriteConcurrency() throws Exception {
            executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, embedding VECTOR(4))");

            // Seed with initial data
            executeDml("INSERT INTO " + fqn() + " VALUES"
                    + " (1, ARRAY[1.0, 0.0, 0.0, 0.0]),"
                    + " (2, ARRAY[0.0, 1.0, 0.0, 0.0]),"
                    + " (3, ARRAY[0.0, 0.0, 1.0, 0.0])");

            // Writer thread: insert additional rows sequentially
            CompletableFuture<Void> writer = CompletableFuture.runAsync(() -> {
                for (int i = 4; i <= 8; i++) {
                    executeDml("INSERT INTO " + fqn() + " VALUES ("
                            + i + ", ARRAY["
                            + String.format("%.1f", i * 0.1)
                            + ", "
                            + String.format("%.1f", i * 0.05)
                            + ", "
                            + String.format("%.1f", i * 0.025)
                            + ", 0.0])");
                }
            });

            List<CompletableFuture<Void>> readers = new ArrayList<>();
            for (int q = 0; q < 3; q++) {
                readers.add(CompletableFuture.runAsync(() -> {
                    E2ETestFramework.QueryResult result = framework.executeQuery(
                            "SELECT id FROM " + fqn());
                    assertThat(result.success())
                            .as("Concurrent read failed: %s", result.errorMessage())
                            .isTrue();
                    assertThat(result.rows()).isNotEmpty();
                }));
            }

            List<CompletableFuture<Void>> operations = new ArrayList<>(readers);
            operations.add(writer);
            CompletableFuture.allOf(operations.toArray(CompletableFuture[]::new))
                    .get(120, TimeUnit.SECONDS);

            executeDdl("DROP TABLE " + fqn());
        }

        @Test
        @DisplayName("Concurrent queries during index existence all succeed")
        void concurrentIndexOperations() throws Exception {
            executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, embedding VECTOR(4))");

            // Insert enough data to exercise index paths
            StringBuilder sb = new StringBuilder("INSERT INTO " + fqn() + " VALUES ");
            for (int i = 1; i <= 30; i++) {
                if (i > 1) {
                    sb.append(", ");
                }
                sb.append("(").append(i).append(", ARRAY[");
                for (int d = 0; d < 4; d++) {
                    if (d > 0) {
                        sb.append(",");
                    }
                    sb.append(String.format("%.6f", Math.sin(i * 0.2 + d * 0.3)));
                }
                sb.append("])");
            }
            executeDml(sb.toString());

            executeDdl("CREATE INDEX idx_conc_ops ON " + fqn()
                    + "(embedding) USING VECTOR WITH ('algorithm'='HNSW', 'metric'='L2')");

            int concurrency = 5;
            List<CompletableFuture<Void>> futures = new ArrayList<>();

            for (int q = 0; q < concurrency; q++) {
                futures.add(CompletableFuture.runAsync(() -> {
                    E2ETestFramework.QueryResult result = framework.executeQuery(
                            "SELECT id, l2_distance(embedding, ARRAY[0.5, 0.5, 0.5, 0.5]) AS dist"
                                    + " FROM " + fqn()
                                    + " ORDER BY l2_distance(embedding, ARRAY[0.5, 0.5, 0.5, 0.5]) ASC"
                                    + " LIMIT 5");
                    assertThat(result.success())
                            .as("Concurrent indexed query failed: %s", result.errorMessage())
                            .isTrue();
                    assertThat(result.rows()).hasSize(5);
                }));
            }

            CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
                    .get(120, TimeUnit.SECONDS);

            executeDdl("DROP INDEX idx_conc_ops ON " + fqn());
            executeDdl("DROP TABLE " + fqn());
        }
    }

    // -----------------------------------------------------------------------
    // 2. Large Data Vector Search
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Large Data Vector Search")
    class LargeDataVectorSearch {

        @Test
        @DisplayName("Search across 1000 vectors returns correct top-K")
        void thousandVectorSearch() {
            executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, embedding VECTOR(4))");

            // Insert 1000 vectors in batches of 50
            for (int batch = 0; batch < 20; batch++) {
                StringBuilder sb = new StringBuilder("INSERT INTO " + fqn() + " VALUES ");
                for (int i = 0; i < 50; i++) {
                    int id = batch * 50 + i + 1;
                    if (i > 0) {
                        sb.append(", ");
                    }
                    sb.append("(").append(id).append(", ARRAY[");
                    for (int d = 0; d < 4; d++) {
                        if (d > 0) {
                            sb.append(",");
                        }
                        double val = Math.sin(id * 0.01 + d * 0.25);
                        sb.append(String.format("%.6f", val));
                    }
                    sb.append("])");
                }
                executeDml(sb.toString());
            }

            List<TypedRow> rows = executeQuery(
                    "SELECT id, l2_distance(embedding, ARRAY[0.0, 0.0, 0.0, 0.0]) AS dist"
                            + " FROM " + fqn()
                            + " ORDER BY l2_distance(embedding, ARRAY[0.0, 0.0, 0.0, 0.0]) ASC"
                            + " LIMIT 10");

            assertThat(rows)
                    .as("Top-10 from 1000 vectors should return 10 results")
                    .hasSize(10);

            // Verify distances are in ascending order
            for (int i = 0; i < rows.size() - 1; i++) {
                double d1 = ((Number) rows.get(i).getRaw("dist")).doubleValue();
                double d2 = ((Number) rows.get(i + 1).getRaw("dist")).doubleValue();
                assertThat(d1).isLessThanOrEqualTo(d2 + 1e-6);
            }

            // Verify all distances are non-negative
            for (TypedRow row : rows) {
                double dist = ((Number) row.getRaw("dist")).doubleValue();
                assertThat(dist).isGreaterThanOrEqualTo(0.0);
            }

            executeDdl("DROP TABLE " + fqn());
        }

        @Test
        @DisplayName("128-dimensional vectors are stored and queried correctly")
        void highDimensionVectors() {
            int dim = 128;
            executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, embedding VECTOR(" + dim + "))");

            // Insert 20 vectors with 128 dimensions
            StringBuilder sb = new StringBuilder("INSERT INTO " + fqn() + " VALUES ");
            for (int i = 1; i <= 20; i++) {
                if (i > 1) {
                    sb.append(", ");
                }
                sb.append("(").append(i).append(", ARRAY[");
                for (int d = 0; d < dim; d++) {
                    if (d > 0) {
                        sb.append(",");
                    }
                    // One-hot-ish: each vector has a peak at position (i-1) % dim
                    double val = (d == (i - 1) % dim) ? 1.0 : 0.0;
                    sb.append(val);
                }
                sb.append("])");
            }
            executeDml(sb.toString());

            // Build query vector: one-hot at position 0
            StringBuilder queryVec = new StringBuilder("ARRAY[");
            for (int d = 0; d < dim; d++) {
                if (d > 0) {
                    queryVec.append(",");
                }
                queryVec.append(d == 0 ? "1.0" : "0.0");
            }
            queryVec.append("]");

            List<TypedRow> rows = executeQuery(
                    "SELECT id, l2_distance(embedding, " + queryVec + ") AS dist"
                            + " FROM " + fqn()
                            + " ORDER BY l2_distance(embedding, " + queryVec + ") ASC"
                            + " LIMIT 5");

            assertThat(rows)
                    .as("High-dimensional query should return 5 results")
                    .hasSize(5);

            // The closest vector should be id=1 (one-hot at position 0)
            long closestId = ((Number) rows.get(0).getRaw("id")).longValue();
            assertThat(closestId).isEqualTo(1L);

            // Distance of exact match should be ~0
            double closestDist = ((Number) rows.get(0).getRaw("dist")).doubleValue();
            assertThat(closestDist).isLessThan(0.001);

            executeDdl("DROP TABLE " + fqn());
        }
    }
}
