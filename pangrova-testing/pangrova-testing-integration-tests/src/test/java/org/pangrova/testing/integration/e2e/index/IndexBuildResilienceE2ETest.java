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
package org.pangrova.testing.integration.e2e.index;

import org.pangrova.connector.spi.data.value.TypedRow;
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
 * E2E tests verifying index build resilience and DDL lifecycle
 * correctness after review-report fixes: build coordinator TOCTOU
 * (P1-03), DDL executor generation handling, concurrent DDL operations,
 * IF NOT EXISTS/IF EXISTS idempotency, and data correctness after
 * index lifecycle operations.
 */
@DisplayName("Index Build Resilience E2E")
class IndexBuildResilienceE2ETest extends SqlE2ETestBase {

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
    void createTestTable(TestInfo info) {
        String method = info.getTestMethod().map(m -> m.getName()).orElse("unknown");
        tableName = "t_br_" + method + "_" + System.nanoTime();
        sqlRunner.executeDdl("CREATE TABLE " + fqn()
                + " (user_id BIGINT, description VARCHAR, amount DOUBLE, status VARCHAR)");
        sqlRunner.execute("INSERT INTO " + fqn()
                + " (user_id, description, amount, status) VALUES"
                + " (1, 'hello world', 100.0, 'ACTIVE'),"
                + " (2, 'pangrova engine', 200.0, 'INACTIVE'),"
                + " (3, 'search test', 300.0, 'ACTIVE')");
    }

    private String fqn() {
        return FQN_PREFIX + tableName;
    }

    @Nested
    @DisplayName("Build Coordinator Lifecycle (P1-03)")
    class BuildCoordinatorLifecycle {

        @Test
        @DisplayName("CREATE INDEX followed by immediate query returns correct data")
        void createAndQueryImmediately() {
            sqlRunner.executeDdl("CREATE INDEX idx_bloom ON " + fqn()
                    + "(user_id) USING BLOOM_FILTER");

            List<TypedRow> results = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE user_id = 2");
            assertThat(results).isNotEmpty();
            assertThat(results.get(0).getRaw("description").toString())
                    .isEqualTo("pangrova engine");
        }

        @Test
        @DisplayName("ALTER INDEX REBUILD followed by query returns correct data")
        void rebuildAndQuery() {
            sqlRunner.executeDdl("CREATE INDEX idx_rebuild ON " + fqn()
                    + "(user_id) USING BLOOM_FILTER");
            IndexReadiness.awaitReady(sqlRunner, fqn(), "idx_rebuild");

            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (user_id, description, amount, status) VALUES"
                    + " (4, 'new record', 400.0, 'ACTIVE')");

            sqlRunner.executeDdl("ALTER INDEX idx_rebuild ON " + fqn() + " REBUILD");

            List<TypedRow> results = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE user_id = 4");
            assertThat(results).isNotEmpty();
            assertThat(results.get(0).getRaw("description").toString())
                    .isEqualTo("new record");
        }

        @Test
        @DisplayName("DROP INDEX during or after build does not corrupt query results")
        void dropIndexQueryStillWorks() {
            sqlRunner.executeDdl("CREATE INDEX idx_drop_build ON " + fqn()
                    + "(status) USING BLOOM_FILTER");
            sqlRunner.executeDdl("DROP INDEX idx_drop_build ON " + fqn());

            List<TypedRow> results = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE status = 'ACTIVE'");
            assertThat(results).hasSize(2);
        }
    }

    @Nested
    @DisplayName("DDL Idempotency")
    class DdlIdempotency {

        @Test
        @DisplayName("CREATE INDEX IF NOT EXISTS is idempotent")
        void createIfNotExistsIdempotent() {
            sqlRunner.executeDdl("CREATE INDEX idx_idem ON " + fqn()
                    + "(user_id) USING BLOOM_FILTER");
            sqlRunner.executeDdl("CREATE INDEX IF NOT EXISTS idx_idem ON " + fqn()
                    + "(user_id) USING BLOOM_FILTER");

            List<TypedRow> indexes = sqlRunner.execute("SHOW INDEXES FROM " + fqn());
            long count = indexes.stream()
                    .filter(r -> "idx_idem".equals(r.getRaw("index_name").toString()))
                    .filter(r -> !"DROPPED".equals(r.getRaw("state").toString()))
                    .count();
            assertThat(count).isEqualTo(1);
        }

        @Test
        @DisplayName("DROP INDEX IF EXISTS on non-existent index succeeds silently")
        void dropIfExistsNonExistent() {
            sqlRunner.executeDdl("DROP INDEX IF EXISTS idx_ghost ON " + fqn());

            List<TypedRow> results = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE user_id = 1");
            assertThat(results).isNotEmpty();
        }

        @Test
        @DisplayName("CREATE INDEX without IF NOT EXISTS on existing name fails")
        void duplicateCreateFails() {
            sqlRunner.executeDdl("CREATE INDEX idx_dup ON " + fqn()
                    + "(user_id) USING BLOOM_FILTER");

            QueryError error = sqlRunner.executeExpectingError(
                    "CREATE INDEX idx_dup ON " + fqn()
                            + "(user_id) USING BLOOM_FILTER");
            assertThat(error.message().toLowerCase())
                    .containsAnyOf("already exists", "duplicate", "exists");
        }
    }

    @Nested
    @DisplayName("Multiple Index Types on Same Table")
    class MultipleIndexTypes {

        @Test
        @DisplayName("BLOOM_FILTER and INVERTED indexes coexist on same table")
        void multipleIndexTypesCoexist() {
            sqlRunner.executeDdl("CREATE INDEX idx_bloom_multi ON " + fqn()
                    + "(user_id) USING BLOOM_FILTER");
            sqlRunner.executeDdl("CREATE INDEX idx_inv_multi ON " + fqn()
                    + "(description) USING INVERTED");

            List<TypedRow> indexes = sqlRunner.execute("SHOW INDEXES FROM " + fqn());
            long activeCount = indexes.stream()
                    .filter(r -> !"DROPPED".equals(r.getRaw("state").toString()))
                    .count();
            assertThat(activeCount).isGreaterThanOrEqualTo(2);

            List<TypedRow> byId = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE user_id = 1");
            assertThat(byId).isNotEmpty();
        }

        @Test
        @DisplayName("Dropping one index does not affect another on same table")
        void dropOneIndexKeepsOther() {
            sqlRunner.executeDdl("CREATE INDEX idx_keep ON " + fqn()
                    + "(user_id) USING BLOOM_FILTER");
            sqlRunner.executeDdl("CREATE INDEX idx_remove ON " + fqn()
                    + "(status) USING BLOOM_FILTER");
            sqlRunner.executeDdl("DROP INDEX idx_remove ON " + fqn());

            List<TypedRow> indexes = sqlRunner.execute("SHOW INDEXES FROM " + fqn());
            boolean keepExists = indexes.stream()
                    .anyMatch(r -> "idx_keep".equals(r.getRaw("index_name").toString())
                            && !"DROPPED".equals(r.getRaw("state").toString()));
            assertThat(keepExists).isTrue();

            List<TypedRow> results = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE user_id = 3");
            assertThat(results).isNotEmpty();
        }
    }

    @Nested
    @DisplayName("Data Correctness After Index Operations")
    class DataCorrectnessAfterOps {

        @Test
        @DisplayName("Inserting data after index creation returns correct results")
        void insertAfterIndexCreation() {
            sqlRunner.executeDdl("CREATE INDEX idx_post ON " + fqn()
                    + "(user_id) USING BLOOM_FILTER");

            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (user_id, description, amount, status) VALUES"
                    + " (10, 'post-index', 1000.0, 'NEW')");

            List<TypedRow> results = sqlRunner.execute(
                    "SELECT description FROM " + fqn() + " WHERE user_id = 10");
            assertThat(results).isNotEmpty();
            assertThat(results.get(0).getRaw("description").toString())
                    .isEqualTo("post-index");
        }

        @Test
        @DisplayName("Full table scan is unaffected by index lifecycle")
        void fullScanUnaffectedByIndex() {
            sqlRunner.executeDdl("CREATE INDEX idx_life ON " + fqn()
                    + "(status) USING BLOOM_FILTER");
            IndexReadiness.awaitReady(sqlRunner, fqn(), "idx_life");
            sqlRunner.executeDdl("ALTER INDEX idx_life ON " + fqn() + " REBUILD");
            sqlRunner.executeDdl("DROP INDEX idx_life ON " + fqn());

            List<TypedRow> all = sqlRunner.execute("SELECT * FROM " + fqn());
            assertThat(all).hasSize(3);
        }
    }
}
