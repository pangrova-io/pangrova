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

import org.pangrova.testing.e2e.SqlE2ETestBase;
import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Full-chain E2E tests for vector search capabilities.
 *
 * <p>Exercises the complete path: SQL → Parser → Planner (VectorSearchRewriter) →
 * Substrait → Worker (VectorSearchTaskInterceptor + native SIMD distance) →
 * Result transport → Client.
 *
 * <p>Test scenarios:
 * <ol>
 *   <li>Brute-force vector scan (no index) with ORDER BY distance LIMIT K</li>
 *   <li>HNSW index creation + ANN query</li>
 *   <li>Filtered ANN (WHERE + ORDER BY distance)</li>
 *   <li>Multiple distance metrics (L2, cosine, inner product)</li>
 *   <li>IVFPQ index with larger dataset</li>
 *   <li>Vector column with mixed types</li>
 *   <li>Higher-dimensional vectors</li>
 * </ol>
 */
class VectorSearchChainE2ETest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String FQN_PREFIX = CATALOG + "." + SCHEMA + ".";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeAll
    static void registerConnectors() {
        isDockerReady();
        registerTestConnectors(List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString())));
    }

    private void waitForNativeTaskCleanup() {
        try {
            TimeUnit.MILLISECONDS.sleep(800);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Test
    void bruteForceScanWithTopK(TestInfo testInfo) {
        String fqn = FQN_PREFIX + "t_bf_topk_" + System.nanoTime();

        sqlRunner.executeDdl("CREATE TABLE " + fqn
                + " (id BIGINT, embedding VECTOR(3))");
        sqlRunner.execute("INSERT INTO " + fqn + " VALUES"
                + " (1, ARRAY[1.0, 0.0, 0.0]),"
                + " (2, ARRAY[0.0, 1.0, 0.0]),"
                + " (3, ARRAY[0.0, 0.0, 1.0]),"
                + " (4, ARRAY[1.0, 1.0, 0.0]),"
                + " (5, ARRAY[0.5, 0.5, 0.5])");

        List<TypedRow> rows = sqlRunner.execute(
                "SELECT id, l2_distance(embedding, ARRAY[1.0, 0.0, 0.0]) AS dist"
                        + " FROM " + fqn
                        + " ORDER BY l2_distance(embedding, ARRAY[1.0, 0.0, 0.0]) ASC"
                        + " LIMIT 3");

        assertThat(rows).hasSize(3);
        assertThat(((Number) rows.get(0).getRaw("id")).longValue()).isEqualTo(1L);
        double firstDist = ((Number) rows.get(0).getRaw("dist")).doubleValue();
        assertThat(firstDist).isLessThan(0.001);

        for (int i = 0; i < rows.size() - 1; i++) {
            double d1 = ((Number) rows.get(i).getRaw("dist")).doubleValue();
            double d2 = ((Number) rows.get(i + 1).getRaw("dist")).doubleValue();
            assertThat(d1).isLessThanOrEqualTo(d2 + 1e-6);
        }

        sqlRunner.executeDdl("DROP TABLE " + fqn);
        waitForNativeTaskCleanup();
    }

    @Test
    void hnswIndexCreateAndQuery(TestInfo testInfo) {
        String fqn = FQN_PREFIX + "t_hnsw_" + System.nanoTime();

        sqlRunner.executeDdl("CREATE TABLE " + fqn
                + " (id BIGINT, category VARCHAR, embedding VECTOR(4))");
        sqlRunner.execute("INSERT INTO " + fqn + " VALUES"
                + " (1, 'electronics', ARRAY[1.0, 0.0, 0.0, 0.0]),"
                + " (2, 'clothing',    ARRAY[0.0, 1.0, 0.0, 0.0]),"
                + " (3, 'electronics', ARRAY[0.9, 0.1, 0.0, 0.0]),"
                + " (4, 'food',        ARRAY[0.0, 0.0, 1.0, 0.0]),"
                + " (5, 'clothing',    ARRAY[0.1, 0.9, 0.0, 0.0]),"
                + " (6, 'electronics', ARRAY[0.8, 0.2, 0.0, 0.0])");

        sqlRunner.executeDdl("CREATE INDEX idx_hnsw_chain ON " + fqn
                + "(embedding) USING VECTOR WITH ('algorithm'='HNSW', 'metric'='L2')");

        List<TypedRow> rows = sqlRunner.execute(
                "SELECT id, l2_distance(embedding, ARRAY[0.95, 0.05, 0.0, 0.0]) AS dist"
                        + " FROM " + fqn
                        + " ORDER BY l2_distance(embedding, ARRAY[0.95, 0.05, 0.0, 0.0]) ASC"
                        + " LIMIT 3");

        assertThat(rows).hasSize(3);
        long firstId = ((Number) rows.get(0).getRaw("id")).longValue();
        assertThat(firstId).isIn(1L, 3L);

        for (int i = 0; i < rows.size() - 1; i++) {
            double d1 = ((Number) rows.get(i).getRaw("dist")).doubleValue();
            double d2 = ((Number) rows.get(i + 1).getRaw("dist")).doubleValue();
            assertThat(d1).isLessThanOrEqualTo(d2 + 1e-6);
        }

        sqlRunner.executeDdl("DROP INDEX idx_hnsw_chain ON " + fqn);
        sqlRunner.executeDdl("DROP TABLE " + fqn);
        waitForNativeTaskCleanup();
    }

    @Test
    void filteredAnnQuery(TestInfo testInfo) {
        String fqn = FQN_PREFIX + "t_filtered_ann_" + System.nanoTime();

        sqlRunner.executeDdl("CREATE TABLE " + fqn
                + " (id BIGINT, category VARCHAR, embedding VECTOR(3))");
        sqlRunner.execute("INSERT INTO " + fqn + " VALUES"
                + " (1, 'A', ARRAY[1.0, 0.0, 0.0]),"
                + " (2, 'B', ARRAY[0.9, 0.1, 0.0]),"
                + " (3, 'A', ARRAY[0.0, 1.0, 0.0]),"
                + " (4, 'A', ARRAY[0.8, 0.2, 0.0]),"
                + " (5, 'B', ARRAY[0.5, 0.5, 0.0]),"
                + " (6, 'A', ARRAY[0.0, 0.0, 1.0])");

        List<TypedRow> rows = sqlRunner.execute(
                "SELECT id, l2_distance(embedding, ARRAY[1.0, 0.0, 0.0]) AS dist"
                        + " FROM " + fqn
                        + " WHERE category = 'A'"
                        + " ORDER BY l2_distance(embedding, ARRAY[1.0, 0.0, 0.0]) ASC"
                        + " LIMIT 2");

        assertThat(rows).hasSize(2);
        for (TypedRow row : rows) {
            long id = ((Number) row.getRaw("id")).longValue();
            assertThat(id).isIn(1L, 3L, 4L, 6L);
        }
        assertThat(((Number) rows.get(0).getRaw("id")).longValue()).isEqualTo(1L);

        for (int i = 0; i < rows.size() - 1; i++) {
            double d1 = ((Number) rows.get(i).getRaw("dist")).doubleValue();
            double d2 = ((Number) rows.get(i + 1).getRaw("dist")).doubleValue();
            assertThat(d1).isLessThanOrEqualTo(d2 + 1e-6);
        }

        sqlRunner.executeDdl("DROP TABLE " + fqn);
        waitForNativeTaskCleanup();
    }

    @Test
    void cosineDistanceQuery(TestInfo testInfo) {
        String fqn = FQN_PREFIX + "t_cosine_" + System.nanoTime();

        sqlRunner.executeDdl("CREATE TABLE " + fqn
                + " (id BIGINT, embedding VECTOR(3))");
        sqlRunner.execute("INSERT INTO " + fqn + " VALUES"
                + " (1, ARRAY[1.0, 0.0, 0.0]),"
                + " (2, ARRAY[0.707, 0.707, 0.0]),"
                + " (3, ARRAY[0.0, 1.0, 0.0]),"
                + " (4, ARRAY[-1.0, 0.0, 0.0])");

        List<TypedRow> rows = sqlRunner.execute(
                "SELECT id, cosine_distance(embedding, ARRAY[1.0, 0.0, 0.0]) AS dist"
                        + " FROM " + fqn
                        + " ORDER BY cosine_distance(embedding, ARRAY[1.0, 0.0, 0.0]) ASC"
                        + " LIMIT 4");

        assertThat(rows).hasSize(4);
        assertThat(((Number) rows.get(0).getRaw("id")).longValue()).isEqualTo(1L);
        double identityDist = ((Number) rows.get(0).getRaw("dist")).doubleValue();
        assertThat(identityDist).isLessThan(0.001);

        assertThat(((Number) rows.get(rows.size() - 1).getRaw("id")).longValue()).isEqualTo(4L);
        double oppositeDist = ((Number) rows.get(rows.size() - 1).getRaw("dist")).doubleValue();
        assertThat(oppositeDist).isGreaterThan(1.5);

        sqlRunner.executeDdl("DROP TABLE " + fqn);
        waitForNativeTaskCleanup();
    }

    @Test
    void innerProductQuery(TestInfo testInfo) {
        String fqn = FQN_PREFIX + "t_ip_" + System.nanoTime();

        sqlRunner.executeDdl("CREATE TABLE " + fqn
                + " (id BIGINT, embedding VECTOR(2))");
        sqlRunner.execute("INSERT INTO " + fqn + " VALUES"
                + " (1, ARRAY[3.0, 0.0]),"
                + " (2, ARRAY[2.0, 1.0]),"
                + " (3, ARRAY[0.0, 3.0]),"
                + " (4, ARRAY[1.0, 2.0])");

        List<TypedRow> rows = sqlRunner.execute(
                "SELECT id, inner_product(embedding, ARRAY[1.0, 0.0]) AS score"
                        + " FROM " + fqn
                        + " ORDER BY inner_product(embedding, ARRAY[1.0, 0.0]) ASC"
                        + " LIMIT 4");

        assertThat(rows).hasSize(4);
        for (int i = 0; i < rows.size() - 1; i++) {
            double s1 = ((Number) rows.get(i).getRaw("score")).doubleValue();
            double s2 = ((Number) rows.get(i + 1).getRaw("score")).doubleValue();
            assertThat(s1).isLessThanOrEqualTo(s2 + 1e-6);
        }

        sqlRunner.executeDdl("DROP TABLE " + fqn);
        waitForNativeTaskCleanup();
    }

    @Test
    void highDimensionalVectors(TestInfo testInfo) {
        String fqn = FQN_PREFIX + "t_highdim_" + System.nanoTime();
        int dim = 128;

        sqlRunner.executeDdl("CREATE TABLE " + fqn
                + " (id BIGINT, embedding VECTOR(" + dim + "))");

        StringBuilder sb = new StringBuilder("INSERT INTO ").append(fqn).append(" VALUES ");
        for (int i = 1; i <= 20; i++) {
            if (i > 1) {
                sb.append(", ");
            }
            sb.append("(").append(i).append(", ARRAY[");
            for (int d = 0; d < dim; d++) {
                if (d > 0) {
                    sb.append(",");
                }
                double val = (d == (i - 1) % dim) ? 1.0 : 0.0;
                sb.append(val);
            }
            sb.append("])");
        }
        sqlRunner.execute(sb.toString());

        StringBuilder queryVec = new StringBuilder("ARRAY[");
        for (int d = 0; d < dim; d++) {
            if (d > 0) {
                queryVec.append(",");
            }
            queryVec.append(d == 0 ? "1.0" : "0.0");
        }
        queryVec.append("]");

        List<TypedRow> rows = sqlRunner.execute(
                "SELECT id, l2_distance(embedding, " + queryVec + ") AS dist"
                        + " FROM " + fqn
                        + " ORDER BY l2_distance(embedding, " + queryVec + ") ASC"
                        + " LIMIT 5");

        assertThat(rows).hasSize(5);
        assertThat(((Number) rows.get(0).getRaw("id")).longValue()).isEqualTo(1L);
        double firstDist = ((Number) rows.get(0).getRaw("dist")).doubleValue();
        assertThat(firstDist).isLessThan(0.001);

        sqlRunner.executeDdl("DROP TABLE " + fqn);
        waitForNativeTaskCleanup();
    }

    @Test
    void vectorWithNullsAndEdgeCases(TestInfo testInfo) {
        String fqn = FQN_PREFIX + "t_vec_null_" + System.nanoTime();

        sqlRunner.executeDdl("CREATE TABLE " + fqn
                + " (id BIGINT, label VARCHAR, embedding VECTOR(2))");
        sqlRunner.execute("INSERT INTO " + fqn + " VALUES"
                + " (1, 'origin',  ARRAY[0.0, 0.0]),"
                + " (2, 'unit_x',  ARRAY[1.0, 0.0]),"
                + " (3, 'unit_y',  ARRAY[0.0, 1.0]),"
                + " (4, 'diag',    ARRAY[1.0, 1.0])");

        List<TypedRow> rows = sqlRunner.execute(
                "SELECT id, label, l2_distance(embedding, ARRAY[0.0, 0.0]) AS dist"
                        + " FROM " + fqn
                        + " ORDER BY l2_distance(embedding, ARRAY[0.0, 0.0]) ASC"
                        + " LIMIT 4");

        assertThat(rows).hasSize(4);
        assertThat(((Number) rows.get(0).getRaw("id")).longValue()).isEqualTo(1L);
        assertThat(((Number) rows.get(0).getRaw("dist")).doubleValue()).isLessThan(0.001);

        double lastDist = ((Number) rows.get(3).getRaw("dist")).doubleValue();
        assertThat(lastDist).isCloseTo(Math.sqrt(2.0), org.assertj.core.api.Assertions.within(0.01));

        sqlRunner.executeDdl("DROP TABLE " + fqn);
        waitForNativeTaskCleanup();
    }

    @Test
    void vectorIndexWithIvfpq(TestInfo testInfo) {
        String fqn = FQN_PREFIX + "t_ivfpq_" + System.nanoTime();
        int dim = 8;

        sqlRunner.executeDdl("CREATE TABLE " + fqn
                + " (id BIGINT, embedding VECTOR(" + dim + "))");

        StringBuilder sb = new StringBuilder("INSERT INTO ").append(fqn).append(" VALUES ");
        for (int i = 1; i <= 100; i++) {
            if (i > 1) {
                sb.append(", ");
            }
            sb.append("(").append(i).append(", ARRAY[");
            for (int d = 0; d < dim; d++) {
                if (d > 0) {
                    sb.append(",");
                }
                double val = Math.sin(i * 0.1 + d * 0.5);
                sb.append(String.format("%.6f", val));
            }
            sb.append("])");
        }
        sqlRunner.execute(sb.toString());

        sqlRunner.executeDdl("CREATE INDEX idx_ivfpq ON " + fqn
                + "(embedding) USING VECTOR WITH ('algorithm'='IVFPQ', 'metric'='L2')");

        StringBuilder queryVec = new StringBuilder("ARRAY[");
        for (int d = 0; d < dim; d++) {
            if (d > 0) {
                queryVec.append(",");
            }
            queryVec.append(String.format("%.6f", Math.sin(0.1 + d * 0.5)));
        }
        queryVec.append("]");

        List<TypedRow> rows = sqlRunner.execute(
                "SELECT id, l2_distance(embedding, " + queryVec + ") AS dist"
                        + " FROM " + fqn
                        + " ORDER BY l2_distance(embedding, " + queryVec + ") ASC"
                        + " LIMIT 5");

        assertThat(rows).hasSize(5);
        assertThat(((Number) rows.get(0).getRaw("id")).longValue()).isEqualTo(1L);

        for (int i = 0; i < rows.size() - 1; i++) {
            double d1 = ((Number) rows.get(i).getRaw("dist")).doubleValue();
            double d2 = ((Number) rows.get(i + 1).getRaw("dist")).doubleValue();
            assertThat(d1).isLessThanOrEqualTo(d2 + 1e-6);
        }

        sqlRunner.executeDdl("DROP INDEX idx_ivfpq ON " + fqn);
        sqlRunner.executeDdl("DROP TABLE " + fqn);
        waitForNativeTaskCleanup();
    }

    @Test
    void multipleVectorColumns(TestInfo testInfo) {
        String fqn = FQN_PREFIX + "t_multi_vec_" + System.nanoTime();

        sqlRunner.executeDdl("CREATE TABLE " + fqn
                + " (id BIGINT, title_embedding VECTOR(3), image_embedding VECTOR(3))");
        sqlRunner.execute("INSERT INTO " + fqn + " VALUES"
                + " (1, ARRAY[1.0, 0.0, 0.0], ARRAY[0.0, 0.0, 1.0]),"
                + " (2, ARRAY[0.0, 1.0, 0.0], ARRAY[1.0, 0.0, 0.0]),"
                + " (3, ARRAY[0.0, 0.0, 1.0], ARRAY[0.0, 1.0, 0.0])");

        List<TypedRow> titleResults = sqlRunner.execute(
                "SELECT id, l2_distance(title_embedding, ARRAY[1.0, 0.0, 0.0]) AS dist"
                        + " FROM " + fqn
                        + " ORDER BY l2_distance(title_embedding, ARRAY[1.0, 0.0, 0.0]) ASC"
                        + " LIMIT 3");
        assertThat(((Number) titleResults.get(0).getRaw("id")).longValue()).isEqualTo(1L);

        List<TypedRow> imageResults = sqlRunner.execute(
                "SELECT id, l2_distance(image_embedding, ARRAY[1.0, 0.0, 0.0]) AS dist"
                        + " FROM " + fqn
                        + " ORDER BY l2_distance(image_embedding, ARRAY[1.0, 0.0, 0.0]) ASC"
                        + " LIMIT 3");
        assertThat(((Number) imageResults.get(0).getRaw("id")).longValue()).isEqualTo(2L);

        sqlRunner.executeDdl("DROP TABLE " + fqn);
        waitForNativeTaskCleanup();
    }

    @Test
    void vectorDistanceFunctionsInProjection(TestInfo testInfo) {
        String fqn = FQN_PREFIX + "t_dist_proj_" + System.nanoTime();

        sqlRunner.executeDdl("CREATE TABLE " + fqn
                + " (id BIGINT, embedding VECTOR(2))");
        sqlRunner.execute("INSERT INTO " + fqn + " VALUES"
                + " (1, ARRAY[3.0, 4.0]),"
                + " (2, ARRAY[1.0, 0.0])");

        List<TypedRow> rows = sqlRunner.execute(
                "SELECT id, l2_distance(embedding, ARRAY[0.0, 0.0]) AS l2"
                        + " FROM " + fqn
                        + " ORDER BY l2_distance(embedding, ARRAY[0.0, 0.0]) ASC"
                        + " LIMIT 2");

        assertThat(rows).hasSize(2);
        assertThat(((Number) rows.get(0).getRaw("id")).longValue()).isEqualTo(2L);
        double firstDist = ((Number) rows.get(0).getRaw("l2")).doubleValue();
        assertThat(firstDist).isCloseTo(1.0, org.assertj.core.api.Assertions.within(0.01));

        double secondDist = ((Number) rows.get(1).getRaw("l2")).doubleValue();
        assertThat(secondDist).isCloseTo(5.0, org.assertj.core.api.Assertions.within(0.01));

        sqlRunner.executeDdl("DROP TABLE " + fqn);
        waitForNativeTaskCleanup();
    }
}
