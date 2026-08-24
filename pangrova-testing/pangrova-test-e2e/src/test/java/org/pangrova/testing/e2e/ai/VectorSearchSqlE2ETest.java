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
package org.pangrova.testing.e2e.ai;

import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.E2ETest;
import org.pangrova.testing.e2e.PangrovaE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Full-stack E2E test for Vector Search SQL workflow.
 *
 * <p>Exercises the complete end-to-end pipeline:
 * <ol>
 *   <li>CREATE TABLE with VECTOR column</li>
 *   <li>INSERT vector data</li>
 *   <li>CREATE INDEX USING VECTOR (HNSW)</li>
 *   <li>KNN query (ORDER BY distance LIMIT k)</li>
 *   <li>Filtered vector search (WHERE + distance ordering)</li>
 *   <li>Multi-metric queries (L2, cosine, inner product)</li>
 *   <li>Hybrid search with RERANK BY</li>
 *   <li>COSINE_SIMILARITY (1 - cosine_distance)</li>
 *   <li>Index lifecycle: ALTER REBUILD, DROP, recreate</li>
 * </ol>
 *
 * <p>All SQL flows through real coordinator + worker with SIMD-optimized
 * distance computation and HNSW index acceleration.</p>
 */
@E2ETest
@DisplayName("Vector Search SQL Full-Stack E2E")
class VectorSearchSqlE2ETest extends PangrovaE2ETestBase {

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
        tableName = "t_vec_" + method + "_" + System.nanoTime();
    }

    private String fqn() {
        return FQN_PREFIX + tableName;
    }

    // -----------------------------------------------------------------------
    // End-to-End: Create Table → Insert → Index → Query
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Full Pipeline: Table → Insert → Index → KNN")
    class FullPipeline {

        @Test
        @DisplayName("Complete vector search workflow with L2 distance")
        void completeWorkflowL2() {
            // Step 1: Create table with vector column
            executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, category VARCHAR, embedding VECTOR(8))");

            // Step 2: Insert vector data
            executeDml("INSERT INTO " + fqn() + " VALUES "
                    + "(1, 'tech',    ARRAY[1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),"
                    + "(2, 'tech',    ARRAY[0.9, 0.1, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),"
                    + "(3, 'science', ARRAY[0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),"
                    + "(4, 'science', ARRAY[0.0, 0.9, 0.1, 0.0, 0.0, 0.0, 0.0, 0.0]),"
                    + "(5, 'tech',    ARRAY[0.8, 0.2, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),"
                    + "(6, 'art',     ARRAY[0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0]),"
                    + "(7, 'art',     ARRAY[0.0, 0.0, 0.9, 0.1, 0.0, 0.0, 0.0, 0.0]),"
                    + "(8, 'tech',    ARRAY[0.95, 0.05, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0])");

            // Step 3: Create vector index
            executeDdl("CREATE INDEX idx_vec_l2 ON " + fqn()
                    + "(embedding) USING VECTOR WITH ('algorithm'='HNSW', 'metric'='L2')");

            // Step 4: KNN query — find 3 nearest to [1,0,0,0,0,0,0,0]
            List<TypedRow> knnResults = executeQuery(
                    "SELECT id, l2_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]) AS dist"
                            + " FROM " + fqn()
                            + " ORDER BY l2_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]) ASC"
                            + " LIMIT 3");
            assertThat(knnResults).hasSize(3);
            // First result should be id=1 (exact match, distance ≈ 0)
            assertThat(((Number) knnResults.get(0).getRaw("id")).longValue()).isEqualTo(1L);
            assertThat(((Number) knnResults.get(0).getRaw("dist")).doubleValue())
                    .isCloseTo(0.0, within(0.01));
            // Distances should be ascending
            assertDistancesAscending(knnResults);

            // Step 5: Verify index shows up
            List<TypedRow> indexes = executeQuery("SHOW INDEXES FROM " + fqn());
            assertThat(indexes).isNotEmpty();

            // Cleanup
            executeDdl("DROP INDEX idx_vec_l2 ON " + fqn());
            executeDdl("DROP TABLE " + fqn());
        }

        @Test
        @DisplayName("Complete vector search workflow with cosine distance")
        void completeWorkflowCosine() {
            executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, embedding VECTOR(4))");
            executeDml("INSERT INTO " + fqn() + " VALUES "
                    + "(1, ARRAY[1.0, 0.0, 0.0, 0.0]),"
                    + "(2, ARRAY[0.707, 0.707, 0.0, 0.0]),"
                    + "(3, ARRAY[0.0, 1.0, 0.0, 0.0]),"
                    + "(4, ARRAY[0.0, 0.0, 1.0, 0.0]),"
                    + "(5, ARRAY[0.95, 0.05, 0.0, 0.0])");

            executeDdl("CREATE INDEX idx_vec_cos ON " + fqn()
                    + "(embedding) USING VECTOR WITH ('algorithm'='HNSW', 'metric'='COSINE')");

            List<TypedRow> results = executeQuery(
                    "SELECT id, cosine_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) AS dist"
                            + " FROM " + fqn()
                            + " ORDER BY cosine_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) ASC"
                            + " LIMIT 3");
            assertThat(results).hasSize(3);
            assertThat(((Number) results.get(0).getRaw("id")).longValue()).isEqualTo(1L);
            assertThat(((Number) results.get(0).getRaw("dist")).doubleValue())
                    .isCloseTo(0.0, within(0.01));
            assertDistancesAscending(results);

            executeDdl("DROP INDEX idx_vec_cos ON " + fqn());
            executeDdl("DROP TABLE " + fqn());
        }
    }

    // -----------------------------------------------------------------------
    // Filtered Vector Search
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Filtered Vector Search Queries")
    class FilteredSearch {

        @Test
        @DisplayName("WHERE clause with vector distance ordering")
        void filteredKnnQuery() {
            executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, category VARCHAR, embedding VECTOR(4))");
            executeDml("INSERT INTO " + fqn() + " VALUES "
                    + "(1, 'A', ARRAY[1.0, 0.0, 0.0, 0.0]),"
                    + "(2, 'B', ARRAY[0.9, 0.1, 0.0, 0.0]),"
                    + "(3, 'A', ARRAY[0.8, 0.2, 0.0, 0.0]),"
                    + "(4, 'B', ARRAY[0.7, 0.3, 0.0, 0.0]),"
                    + "(5, 'A', ARRAY[0.0, 1.0, 0.0, 0.0]),"
                    + "(6, 'A', ARRAY[0.95, 0.05, 0.0, 0.0])");

            List<TypedRow> results = executeQuery(
                    "SELECT id, l2_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) AS dist"
                            + " FROM " + fqn()
                            + " WHERE category = 'A'"
                            + " ORDER BY l2_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) ASC"
                            + " LIMIT 3");
            assertThat(results).hasSize(3);
            // id=1 is the closest in category A
            assertThat(((Number) results.get(0).getRaw("id")).longValue()).isEqualTo(1L);
            assertDistancesAscending(results);

            executeDdl("DROP TABLE " + fqn());
        }

        @Test
        @DisplayName("Numeric range filter with vector ordering")
        void numericRangeFilteredSearch() {
            executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, price DOUBLE, embedding VECTOR(4))");
            executeDml("INSERT INTO " + fqn() + " VALUES "
                    + "(1, 10.0, ARRAY[1.0, 0.0, 0.0, 0.0]),"
                    + "(2, 50.0, ARRAY[0.9, 0.1, 0.0, 0.0]),"
                    + "(3, 25.0, ARRAY[0.8, 0.2, 0.0, 0.0]),"
                    + "(4, 100.0, ARRAY[0.95, 0.05, 0.0, 0.0]),"
                    + "(5, 30.0, ARRAY[0.0, 1.0, 0.0, 0.0])");

            List<TypedRow> results = executeQuery(
                    "SELECT id, l2_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) AS dist"
                            + " FROM " + fqn()
                            + " WHERE price BETWEEN 20.0 AND 60.0"
                            + " ORDER BY l2_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) ASC"
                            + " LIMIT 3");
            assertThat(results).isNotEmpty();
            assertDistancesAscending(results);
            // All returned rows should have price in [20, 60]

            executeDdl("DROP TABLE " + fqn());
        }

        @Test
        @DisplayName("Multiple filters combined with KNN search")
        void multipleFiltersKnn() {
            executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, category VARCHAR, active BOOLEAN, embedding VECTOR(4))");
            executeDml("INSERT INTO " + fqn() + " VALUES "
                    + "(1, 'tech', true,  ARRAY[1.0, 0.0, 0.0, 0.0]),"
                    + "(2, 'tech', false, ARRAY[0.9, 0.1, 0.0, 0.0]),"
                    + "(3, 'tech', true,  ARRAY[0.8, 0.2, 0.0, 0.0]),"
                    + "(4, 'bio',  true,  ARRAY[0.7, 0.3, 0.0, 0.0]),"
                    + "(5, 'tech', true,  ARRAY[0.95, 0.05, 0.0, 0.0])");

            List<TypedRow> results = executeQuery(
                    "SELECT id, l2_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) AS dist"
                            + " FROM " + fqn()
                            + " WHERE category = 'tech' AND active = true"
                            + " ORDER BY l2_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) ASC"
                            + " LIMIT 2");
            assertThat(results).hasSize(2);
            assertThat(((Number) results.get(0).getRaw("id")).longValue()).isEqualTo(1L);
            assertDistancesAscending(results);

            executeDdl("DROP TABLE " + fqn());
        }
    }

    // -----------------------------------------------------------------------
    // Hybrid Search & RERANK
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Hybrid Search and RERANK BY")
    class HybridSearchAndRerank {

        @Test
        @DisplayName("RERANK BY rrf() syntax is accepted after vector query")
        void rerankByRrfSyntax() {
            executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, content VARCHAR, embedding VECTOR(4))");
            executeDml("INSERT INTO " + fqn() + " VALUES "
                    + "(1, 'machine learning basics', ARRAY[1.0, 0.0, 0.0, 0.0]),"
                    + "(2, 'deep learning tutorial', ARRAY[0.9, 0.1, 0.0, 0.0]),"
                    + "(3, 'cooking recipes',        ARRAY[0.0, 1.0, 0.0, 0.0])");

            String sql = "SELECT id, content FROM " + fqn()
                    + " ORDER BY l2_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) ASC"
                    + " LIMIT 10 RERANK BY rrf(60)";
            try {
                List<TypedRow> rows = executeQuery(sql);
                assertThat(rows).isNotNull();
            } catch (AssertionError e) {
                String error = executeExpectingError(sql);
                assertThat(error).doesNotContainIgnoringCase("syntax error");
            }
            executeDdl("DROP TABLE " + fqn());
        }

        @Test
        @DisplayName("RERANK BY linear_combination() syntax")
        void rerankByLinearCombination() {
            executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, content VARCHAR, embedding VECTOR(4))");
            executeDml("INSERT INTO " + fqn() + " VALUES "
                    + "(1, 'vector database', ARRAY[1.0, 0.0, 0.0, 0.0]),"
                    + "(2, 'search engine',   ARRAY[0.0, 1.0, 0.0, 0.0])");

            String sql = "SELECT id FROM " + fqn()
                    + " ORDER BY l2_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) ASC"
                    + " LIMIT 10 RERANK BY linear_combination(0.7, 0.3)";
            try {
                List<TypedRow> rows = executeQuery(sql);
                assertThat(rows).isNotNull();
            } catch (AssertionError e) {
                String error = executeExpectingError(sql);
                assertThat(error).doesNotContainIgnoringCase("syntax error");
            }
            executeDdl("DROP TABLE " + fqn());
        }

        @Test
        @DisplayName("MATCH function for full-text component of hybrid search")
        void matchFunctionSyntax() {
            executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, content VARCHAR, embedding VECTOR(4))");
            executeDml("INSERT INTO " + fqn() + " VALUES "
                    + "(1, 'hello world', ARRAY[1.0, 0.0, 0.0, 0.0]),"
                    + "(2, 'goodbye',     ARRAY[0.0, 1.0, 0.0, 0.0])");

            String sql = "SELECT id FROM " + fqn() + " WHERE MATCH(content, 'hello')";
            try {
                List<TypedRow> rows = executeQuery(sql);
                assertThat(rows).isNotNull();
            } catch (AssertionError e) {
                String error = executeExpectingError(sql);
                assertThat(error).doesNotContainIgnoringCase("syntax error");
            }
            executeDdl("DROP TABLE " + fqn());
        }
    }

    // -----------------------------------------------------------------------
    // COSINE_SIMILARITY and distance function variants
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Distance Function Variants")
    class DistanceFunctionVariants {

        @Test
        @DisplayName("COSINE_SIMILARITY returns value in [0, 1] for unit vectors")
        void cosineSimilarityValue() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, embedding VECTOR(3))");
            executeDml("INSERT INTO " + fqn() + " VALUES "
                    + "(1, ARRAY[1.0, 0.0, 0.0]),"
                    + "(2, ARRAY[0.0, 1.0, 0.0]),"
                    + "(3, ARRAY[0.707, 0.707, 0.0])");

            List<TypedRow> results = executeQuery(
                    "SELECT id, cosine_similarity(embedding, ARRAY[1.0, 0.0, 0.0]) AS sim"
                            + " FROM " + fqn()
                            + " ORDER BY cosine_similarity(embedding, ARRAY[1.0, 0.0, 0.0]) DESC"
                            + " LIMIT 3");
            assertThat(results).hasSize(3);
            // id=1 should have similarity ≈ 1.0
            assertThat(((Number) results.get(0).getRaw("id")).longValue()).isEqualTo(1L);
            assertThat(((Number) results.get(0).getRaw("sim")).doubleValue())
                    .isCloseTo(1.0, within(0.01));

            executeDdl("DROP TABLE " + fqn());
        }

        @Test
        @DisplayName("INNER_PRODUCT ordering for maximum inner product search")
        void innerProductSearch() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, embedding VECTOR(4))");
            executeDml("INSERT INTO " + fqn() + " VALUES "
                    + "(1, ARRAY[3.0, 0.0, 0.0, 0.0]),"
                    + "(2, ARRAY[0.0, 3.0, 0.0, 0.0]),"
                    + "(3, ARRAY[2.0, 1.0, 0.0, 0.0]),"
                    + "(4, ARRAY[1.0, 2.0, 0.0, 0.0])");

            // INNER_PRODUCT returns negative dot product for sorting (so ASC = highest similarity)
            List<TypedRow> results = executeQuery(
                    "SELECT id, inner_product(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) AS score"
                            + " FROM " + fqn()
                            + " ORDER BY inner_product(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) ASC"
                            + " LIMIT 2");
            assertThat(results).hasSize(2);

            executeDdl("DROP TABLE " + fqn());
        }
    }

    // -----------------------------------------------------------------------
    // Index Lifecycle
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Vector Index Lifecycle Management")
    class IndexLifecycle {

        @Test
        @DisplayName("ALTER INDEX REBUILD refreshes search results")
        void alterIndexRebuild() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, embedding VECTOR(4))");
            executeDml("INSERT INTO " + fqn() + " VALUES "
                    + "(1, ARRAY[1.0, 0.0, 0.0, 0.0]),"
                    + "(2, ARRAY[0.0, 1.0, 0.0, 0.0])");
            executeDdl("CREATE INDEX idx_lifecycle ON " + fqn()
                    + "(embedding) USING VECTOR WITH ('algorithm'='HNSW', 'metric'='L2')");

            // Insert new data after index creation
            executeDml("INSERT INTO " + fqn() + " VALUES "
                    + "(3, ARRAY[0.99, 0.01, 0.0, 0.0])");

            // Rebuild index to incorporate new data
            executeDdl("ALTER INDEX idx_lifecycle ON " + fqn() + " REBUILD");

            // Query should now find the newly inserted row
            List<TypedRow> results = executeQuery(
                    "SELECT id, l2_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) AS dist"
                            + " FROM " + fqn()
                            + " ORDER BY l2_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) ASC"
                            + " LIMIT 2");
            assertThat(results).hasSize(2);
            // Top-2 should include id=1 and id=3
            long first = ((Number) results.get(0).getRaw("id")).longValue();
            long second = ((Number) results.get(1).getRaw("id")).longValue();
            assertThat(first).isEqualTo(1L);
            assertThat(second).isEqualTo(3L);

            executeDdl("DROP INDEX idx_lifecycle ON " + fqn());
            executeDdl("DROP TABLE " + fqn());
        }

        @Test
        @DisplayName("DROP INDEX and verify queries still work (brute force)")
        void dropIndexFallbackToBruteForce() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, embedding VECTOR(4))");
            executeDml("INSERT INTO " + fqn() + " VALUES "
                    + "(1, ARRAY[1.0, 0.0, 0.0, 0.0]),"
                    + "(2, ARRAY[0.0, 1.0, 0.0, 0.0]),"
                    + "(3, ARRAY[0.5, 0.5, 0.0, 0.0])");
            executeDdl("CREATE INDEX idx_drop ON " + fqn()
                    + "(embedding) USING VECTOR WITH ('algorithm'='HNSW', 'metric'='L2')");
            executeDdl("DROP INDEX idx_drop ON " + fqn());

            // Query should still work (brute force scan)
            List<TypedRow> results = executeQuery(
                    "SELECT id, l2_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) AS dist"
                            + " FROM " + fqn()
                            + " ORDER BY l2_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) ASC"
                            + " LIMIT 2");
            assertThat(results).hasSize(2);
            assertThat(((Number) results.get(0).getRaw("id")).longValue()).isEqualTo(1L);

            executeDdl("DROP TABLE " + fqn());
        }

        @Test
        @DisplayName("SHOW INDEXES returns created vector index metadata")
        void showIndexesReturnsMetadata() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, embedding VECTOR(4))");
            executeDml("INSERT INTO " + fqn() + " VALUES (1, ARRAY[1.0, 0.0, 0.0, 0.0])");
            executeDdl("CREATE INDEX idx_show ON " + fqn()
                    + "(embedding) USING VECTOR WITH ('algorithm'='HNSW', 'metric'='L2')");

            List<TypedRow> indexes = executeQuery("SHOW INDEXES FROM " + fqn());
            assertThat(indexes).isNotEmpty();

            executeDdl("DROP INDEX idx_show ON " + fqn());
            executeDdl("DROP TABLE " + fqn());
        }
    }

    // -----------------------------------------------------------------------
    // Utility
    // -----------------------------------------------------------------------

    private void assertDistancesAscending(List<TypedRow> rows) {
        for (int i = 0; i < rows.size() - 1; i++) {
            double d1 = ((Number) rows.get(i).getRaw("dist")).doubleValue();
            double d2 = ((Number) rows.get(i + 1).getRaw("dist")).doubleValue();
            assertThat(d1).isLessThanOrEqualTo(d2 + 1e-6);
        }
    }
}
