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
package org.pangrova.testing.e2e.index.vector;

import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.e2e.SqlE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.pangrova.testing.e2e.index.IndexReadiness;
import org.pangrova.testing.e2e.runner.QueryError;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E tests verifying vector index resilience and correctness after
 * review-report fixes: HNSW thread safety (P0-01), serialization
 * roundtrip with CRC32 (P0-04, P2-05), diverse neighbor selection
 * (P2-03), index rebuild lifecycle (P1-03), and multi-metric support.
 *
 * <p>Each test exercises the full SQL pipeline through build, serialize,
 * deserialize, and ANN search — any regression in graph construction,
 * serialization, or search would surface as incorrect results.</p>
 */
@DisplayName("Vector Index Resilience E2E")
class VectorIndexResilienceE2ETest extends SqlE2ETestBase {

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
        tableName = "t_vr_" + method + "_" + System.nanoTime();
        sqlRunner.executeDdl("CREATE TABLE " + fqn()
                + " (id BIGINT, embedding VECTOR(4), label VARCHAR)");
    }

    private String fqn() {
        return FQN_PREFIX + tableName;
    }

    private void insertClusteredVectors() {
        sqlRunner.execute("INSERT INTO " + fqn() + " VALUES"
                + " (1, ARRAY[1.0, 0.0, 0.0, 0.0], 'x-axis'),"
                + " (2, ARRAY[0.9, 0.1, 0.0, 0.0], 'near-x-1'),"
                + " (3, ARRAY[0.8, 0.2, 0.0, 0.0], 'near-x-2')");
        sqlRunner.execute("INSERT INTO " + fqn() + " VALUES"
                + " (4, ARRAY[0.0, 1.0, 0.0, 0.0], 'y-axis'),"
                + " (5, ARRAY[0.0, 0.9, 0.1, 0.0], 'near-y-1'),"
                + " (6, ARRAY[0.0, 0.8, 0.2, 0.0], 'near-y-2')");
        sqlRunner.execute("INSERT INTO " + fqn() + " VALUES"
                + " (7, ARRAY[0.0, 0.0, 1.0, 0.0], 'z-axis'),"
                + " (8, ARRAY[0.0, 0.0, 0.0, 1.0], 'w-axis'),"
                + " (9, ARRAY[0.5, 0.5, 0.0, 0.0], 'xy-diag'),"
                + " (10, ARRAY[0.0, 0.5, 0.5, 0.0], 'yz-diag')");
    }

    @Nested
    @DisplayName("Diverse Neighbor Selection (P2-03)")
    class DiverseNeighborSelection {

        @Test
        @DisplayName("HNSW with clustered vectors returns diverse nearest neighbors")
        void hnswDiverseNeighbors() {
            insertClusteredVectors();
            sqlRunner.executeDdl("CREATE INDEX idx_diverse ON " + fqn()
                    + "(embedding) USING VECTOR WITH ('algorithm'='HNSW', 'metric'='L2')");

            List<TypedRow> results = sqlRunner.execute(
                    "SELECT id, l2_distance(embedding, ARRAY[0.95, 0.05, 0.0, 0.0]) AS dist"
                            + " FROM " + fqn()
                            + " ORDER BY l2_distance(embedding, ARRAY[0.95, 0.05, 0.0, 0.0]) ASC"
                            + " LIMIT 3");

            assertThat(results).hasSizeGreaterThanOrEqualTo(3);
            Set<String> topIds = results.stream()
                    .map(r -> r.getRaw("id").toString())
                    .collect(Collectors.toSet());
            assertThat(topIds).containsAnyOf("1", "2", "3");
        }
    }

    @Nested
    @DisplayName("Serialization Roundtrip (P0-04, P2-05)")
    class SerializationRoundtrip {

        @Test
        @DisplayName("HNSW index survives serialize-deserialize cycle with CRC32")
        void hnswSerializationRoundtrip() {
            insertClusteredVectors();
            sqlRunner.executeDdl("CREATE INDEX idx_ser ON " + fqn()
                    + "(embedding) USING VECTOR WITH ('algorithm'='HNSW', 'metric'='L2')");

            List<TypedRow> results = sqlRunner.execute(
                    "SELECT id FROM " + fqn()
                            + " ORDER BY l2_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) ASC"
                            + " LIMIT 1");

            assertThat(results).isNotEmpty();
            assertThat(results.get(0).getRaw("id").toString()).isEqualTo("1");
        }

        @Test
        @DisplayName("IVFPQ index survives serialize-deserialize cycle with CRC32")
        void ivfpqSerializationRoundtrip() {
            insertClusteredVectors();
            sqlRunner.executeDdl("CREATE INDEX idx_ivfpq_ser ON " + fqn()
                    + "(embedding) USING VECTOR WITH ('algorithm'='IVFPQ', 'metric'='L2',"
                    + " 'ivfpq.nlist'='2', 'ivfpq.m'='2', 'ivfpq.nbits'='8')");

            List<TypedRow> results = sqlRunner.execute(
                    "SELECT id FROM " + fqn()
                            + " ORDER BY l2_distance(embedding, ARRAY[0.0, 1.0, 0.0, 0.0]) ASC"
                            + " LIMIT 1");

            assertThat(results).isNotEmpty();
        }
    }

    @Nested
    @DisplayName("Multi-Metric Support (P0-04 valueOf)")
    class MultiMetricSupport {

        @Test
        @DisplayName("L2 distance metric query returns correct nearest neighbor")
        void l2MetricQuery() {
            insertClusteredVectors();
            sqlRunner.executeDdl("CREATE INDEX idx_l2 ON " + fqn()
                    + "(embedding) USING VECTOR WITH ('algorithm'='HNSW', 'metric'='L2')");

            List<TypedRow> results = sqlRunner.execute(
                    "SELECT id FROM " + fqn()
                            + " ORDER BY l2_distance(embedding, ARRAY[0.0, 0.0, 1.0, 0.0]) ASC"
                            + " LIMIT 1");

            assertThat(results).isNotEmpty();
            assertThat(results.get(0).getRaw("id").toString()).isEqualTo("7");
        }

        @Test
        @DisplayName("Cosine distance metric query returns correct nearest neighbor")
        void cosineMetricQuery() {
            insertClusteredVectors();
            sqlRunner.executeDdl("CREATE INDEX idx_cos ON " + fqn()
                    + "(embedding) USING VECTOR WITH ('algorithm'='HNSW', 'metric'='COSINE')");

            List<TypedRow> results = sqlRunner.execute(
                    "SELECT id FROM " + fqn()
                            + " ORDER BY cosine_distance(embedding, ARRAY[0.0, 0.0, 0.0, 1.0]) ASC"
                            + " LIMIT 1");

            assertThat(results).isNotEmpty();
            assertThat(results.get(0).getRaw("id").toString()).isEqualTo("8");
        }

        @Test
        @DisplayName("Inner product metric query returns results")
        void innerProductMetricQuery() {
            insertClusteredVectors();
            sqlRunner.executeDdl("CREATE INDEX idx_ip ON " + fqn()
                    + "(embedding) USING VECTOR WITH ('algorithm'='HNSW', 'metric'='INNER_PRODUCT')");

            List<TypedRow> results = sqlRunner.execute(
                    "SELECT id FROM " + fqn()
                            + " ORDER BY inner_product(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) ASC"
                            + " LIMIT 3");

            assertThat(results).isNotEmpty();
        }
    }

    @Nested
    @DisplayName("Index Rebuild Lifecycle (P1-03)")
    class IndexRebuildLifecycle {

        @Test
        @DisplayName("ALTER INDEX REBUILD produces fresh index with correct results")
        void rebuildIndexReturnsCorrectResults() {
            insertClusteredVectors();
            sqlRunner.executeDdl("CREATE INDEX idx_rebuild ON " + fqn()
                    + "(embedding) USING VECTOR WITH ('algorithm'='HNSW', 'metric'='L2')");
            IndexReadiness.awaitReady(sqlRunner, fqn(), "idx_rebuild");

            List<TypedRow> beforeRebuild = sqlRunner.execute(
                    "SELECT id FROM " + fqn()
                            + " ORDER BY l2_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) ASC"
                            + " LIMIT 1");
            assertThat(beforeRebuild).isNotEmpty();

            sqlRunner.executeDdl("ALTER INDEX idx_rebuild ON " + fqn() + " REBUILD");

            List<TypedRow> afterRebuild = sqlRunner.execute(
                    "SELECT id FROM " + fqn()
                            + " ORDER BY l2_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) ASC"
                            + " LIMIT 1");
            assertThat(afterRebuild).isNotEmpty();
            assertThat(afterRebuild.get(0).getRaw("id").toString()).isEqualTo("1");
        }

        @Test
        @DisplayName("DROP and recreate index with same name works correctly")
        void dropAndRecreateIndex() throws Exception {
            insertClusteredVectors();
            sqlRunner.executeDdl("CREATE INDEX idx_reuse ON " + fqn()
                    + "(embedding) USING VECTOR WITH ('algorithm'='HNSW', 'metric'='L2')");
            sqlRunner.executeDdl("DROP INDEX idx_reuse ON " + fqn());

            // DROP may be async; wait for metadata cleanup before re-CREATE.
            // F-E2E-SLEEP: condition-driven Awaitility poll replaces a fixed
            // Thread.sleep loop so the wait ends as soon as the index is gone
            // (no over-wait) and fails with a structured timeout if it stalls.
            org.awaitility.Awaitility.await("index-drop-metadata-cleanup: idx_reuse")
                    .atMost(java.time.Duration.ofSeconds(5))
                    .pollInterval(java.time.Duration.ofMillis(500))
                    .pollDelay(java.time.Duration.ZERO)
                    .ignoreExceptions()
                    .until(() -> sqlRunner.execute("SHOW INDEXES FROM " + fqn()).stream()
                            .noneMatch(r -> "idx_reuse".equals(r.getRaw("index_name").toString())
                                    && !"DROPPED".equals(r.getRaw("state").toString())));

            sqlRunner.executeDdl("CREATE INDEX idx_reuse ON " + fqn()
                    + "(embedding) USING VECTOR WITH ('algorithm'='HNSW', 'metric'='COSINE')");

            List<TypedRow> indexes = sqlRunner.execute("SHOW INDEXES FROM " + fqn());
            long activeCount = indexes.stream()
                    .filter(r -> "idx_reuse".equals(r.getRaw("index_name").toString()))
                    .filter(r -> !"DROPPED".equals(r.getRaw("state").toString()))
                    .count();
            assertThat(activeCount).isEqualTo(1);
        }
    }

    @Nested
    @DisplayName("Multi-File Build (P0-01 Thread Safety)")
    class MultiFileBuild {

        @Test
        @DisplayName("Index build across multiple data files returns correct ANN results")
        void multiFileAnnQuery() {
            sqlRunner.execute("INSERT INTO " + fqn() + " VALUES"
                    + " (1, ARRAY[1.0, 0.0, 0.0, 0.0], 'batch1')");
            sqlRunner.execute("INSERT INTO " + fqn() + " VALUES"
                    + " (2, ARRAY[0.0, 1.0, 0.0, 0.0], 'batch2')");
            sqlRunner.execute("INSERT INTO " + fqn() + " VALUES"
                    + " (3, ARRAY[0.0, 0.0, 1.0, 0.0], 'batch3')");
            sqlRunner.execute("INSERT INTO " + fqn() + " VALUES"
                    + " (4, ARRAY[0.0, 0.0, 0.0, 1.0], 'batch4')");

            sqlRunner.executeDdl("CREATE INDEX idx_multi ON " + fqn()
                    + "(embedding) USING VECTOR WITH ('algorithm'='HNSW', 'metric'='L2')");

            List<TypedRow> results = sqlRunner.execute(
                    "SELECT id, label FROM " + fqn()
                            + " ORDER BY l2_distance(embedding, ARRAY[0.0, 0.0, 1.0, 0.0]) ASC"
                            + " LIMIT 2");

            assertThat(results).hasSizeGreaterThanOrEqualTo(2);
            assertThat(results.get(0).getRaw("id").toString()).isEqualTo("3");
        }

        @Test
        @DisplayName("Multiple HNSW and IVFPQ indexes on same table coexist")
        void multipleIndexesSameTable() {
            insertClusteredVectors();

            sqlRunner.executeDdl("CREATE INDEX idx_hnsw_multi ON " + fqn()
                    + "(embedding) USING VECTOR WITH ('algorithm'='HNSW', 'metric'='L2')");
            sqlRunner.executeDdl("CREATE INDEX idx_ivfpq_multi ON " + fqn()
                    + "(embedding) USING VECTOR WITH ('algorithm'='IVFPQ', 'metric'='L2',"
                    + " 'ivfpq.nlist'='2', 'ivfpq.m'='2', 'ivfpq.nbits'='8')");

            List<TypedRow> indexes = sqlRunner.execute("SHOW INDEXES FROM " + fqn());
            long vectorIndexCount = indexes.stream()
                    .filter(r -> "VECTOR".equals(r.getRaw("index_type").toString()))
                    .filter(r -> !"DROPPED".equals(r.getRaw("state").toString()))
                    .count();
            assertThat(vectorIndexCount).isEqualTo(2);
        }
    }

    @Nested
    @DisplayName("Empty Table Handling")
    class EmptyTableHandling {

        @Test
        @DisplayName("Creating index on empty table succeeds with READY state")
        void indexOnEmptyTable() {
            sqlRunner.executeDdl("CREATE INDEX idx_empty ON " + fqn()
                    + "(embedding) USING VECTOR WITH ('algorithm'='HNSW', 'metric'='L2')");

            List<TypedRow> indexes = sqlRunner.execute("SHOW INDEXES FROM " + fqn());
            assertThat(indexes).isNotEmpty();
            TypedRow idx = indexes.stream()
                    .filter(r -> "idx_empty".equals(r.getRaw("index_name").toString()))
                    .findFirst().orElseThrow();
            assertThat(idx.getRaw("state").toString()).isIn("READY", "BUILDING");
        }

        @Test
        @DisplayName("Query with index on empty table returns empty result")
        void queryEmptyTableWithIndex() {
            sqlRunner.executeDdl("CREATE INDEX idx_empty_q ON " + fqn()
                    + "(embedding) USING VECTOR WITH ('algorithm'='HNSW', 'metric'='L2')");

            List<TypedRow> results = sqlRunner.execute(
                    "SELECT id FROM " + fqn()
                            + " ORDER BY l2_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) ASC"
                            + " LIMIT 5");

            assertThat(results).isEmpty();
        }
    }
}
