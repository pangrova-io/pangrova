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
package org.pangrova.testing.e2e.index;

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
 * E2E tests for index lifecycle operations: CREATE INDEX, DROP INDEX,
 * index-accelerated queries, and index correctness invariants.
 *
 * <p>Exercises: Index DDL → durable canonical artifact build → Native ANN
 * retrieval → index lifecycle cleanup. No JVM ANN implementation participates.
 */
class IndexLifecycleE2ETest extends SqlE2ETestBase {

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
    void createAndDropVectorIndex(TestInfo testInfo) {
        String fqn = FQN_PREFIX + "t_idx_lifecycle_" + System.nanoTime();

        sqlRunner.executeDdl("CREATE TABLE " + fqn
                + " (id BIGINT, embedding VECTOR(4))");
        sqlRunner.execute("INSERT INTO " + fqn + " VALUES"
                + " (1, ARRAY[1.0, 0.0, 0.0, 0.0]),"
                + " (2, ARRAY[0.0, 1.0, 0.0, 0.0]),"
                + " (3, ARRAY[0.0, 0.0, 1.0, 0.0])");

        sqlRunner.executeDdl("CREATE INDEX idx_lc ON " + fqn
                + "(embedding) USING VECTOR WITH ('algorithm'='HNSW', 'metric'='L2')");

        List<TypedRow> rows = sqlRunner.execute(
                "SELECT id, l2_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) AS dist"
                        + " FROM " + fqn
                        + " ORDER BY l2_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) ASC"
                        + " LIMIT 2");
        assertThat(rows).hasSize(2);
        assertThat(((Number) rows.get(0).getRaw("id")).longValue()).isEqualTo(1L);

        sqlRunner.executeDdl("DROP INDEX idx_lc ON " + fqn);

        List<TypedRow> afterDrop = sqlRunner.execute(
                "SELECT id, l2_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) AS dist"
                        + " FROM " + fqn
                        + " ORDER BY l2_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) ASC"
                        + " LIMIT 2");
        assertThat(afterDrop).hasSize(2);
        assertThat(((Number) afterDrop.get(0).getRaw("id")).longValue()).isEqualTo(1L);

        sqlRunner.executeDdl("DROP TABLE " + fqn);
        waitForNativeTaskCleanup();
    }

    @Test
    void queryResultsConsistentWithAndWithoutIndex(TestInfo testInfo) {
        String fqn = FQN_PREFIX + "t_idx_consistency_" + System.nanoTime();

        sqlRunner.executeDdl("CREATE TABLE " + fqn
                + " (id BIGINT, embedding VECTOR(3))");
        sqlRunner.execute("INSERT INTO " + fqn + " VALUES"
                + " (1, ARRAY[1.0, 0.0, 0.0]),"
                + " (2, ARRAY[0.5, 0.5, 0.0]),"
                + " (3, ARRAY[0.0, 1.0, 0.0]),"
                + " (4, ARRAY[0.0, 0.5, 0.5]),"
                + " (5, ARRAY[0.0, 0.0, 1.0])");

        String query = "SELECT id, l2_distance(embedding, ARRAY[0.5, 0.5, 0.0]) AS dist"
                + " FROM " + fqn
                + " ORDER BY l2_distance(embedding, ARRAY[0.5, 0.5, 0.0]) ASC"
                + " LIMIT 5";

        List<TypedRow> withoutIndex = sqlRunner.execute(query);

        sqlRunner.executeDdl("CREATE INDEX idx_cons ON " + fqn
                + "(embedding) USING VECTOR WITH ('algorithm'='HNSW', 'metric'='L2')");
        List<TypedRow> withIndex = sqlRunner.execute(query);

        assertThat(withIndex).hasSameSizeAs(withoutIndex);
        for (int i = 0; i < withIndex.size(); i++) {
            long idWith = ((Number) withIndex.get(i).getRaw("id")).longValue();
            long idWithout = ((Number) withoutIndex.get(i).getRaw("id")).longValue();
            assertThat(idWith).isEqualTo(idWithout);

            double distWith = ((Number) withIndex.get(i).getRaw("dist")).doubleValue();
            double distWithout = ((Number) withoutIndex.get(i).getRaw("dist")).doubleValue();
            assertThat(distWith).isCloseTo(distWithout, org.assertj.core.api.Assertions.within(0.01));
        }

        sqlRunner.executeDdl("DROP INDEX idx_cons ON " + fqn);
        sqlRunner.executeDdl("DROP TABLE " + fqn);
        waitForNativeTaskCleanup();
    }

    @Test
    void insertAfterIndexCreation(TestInfo testInfo) {
        String fqn = FQN_PREFIX + "t_idx_post_insert_" + System.nanoTime();

        sqlRunner.executeDdl("CREATE TABLE " + fqn
                + " (id BIGINT, embedding VECTOR(2))");
        sqlRunner.execute("INSERT INTO " + fqn + " VALUES"
                + " (1, ARRAY[1.0, 0.0]),"
                + " (2, ARRAY[0.0, 1.0])");

        sqlRunner.executeDdl("CREATE INDEX idx_pi ON " + fqn
                + "(embedding) USING VECTOR WITH ('algorithm'='HNSW', 'metric'='L2')");

        sqlRunner.execute("INSERT INTO " + fqn + " VALUES"
                + " (3, ARRAY[0.9, 0.1]),"
                + " (4, ARRAY[0.99, 0.01])");

        List<TypedRow> rows = sqlRunner.execute(
                "SELECT id FROM " + fqn
                        + " ORDER BY l2_distance(embedding, ARRAY[1.0, 0.0]) ASC"
                        + " LIMIT 2");

        assertThat(rows).hasSize(2);
        long closestId = ((Number) rows.get(0).getRaw("id")).longValue();
        assertThat(closestId).isIn(1L, 4L);

        sqlRunner.executeDdl("DROP INDEX idx_pi ON " + fqn);
        sqlRunner.executeDdl("DROP TABLE " + fqn);
        waitForNativeTaskCleanup();
    }

}
