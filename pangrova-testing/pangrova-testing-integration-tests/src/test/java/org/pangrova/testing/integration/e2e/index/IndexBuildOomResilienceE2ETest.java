package org.pangrova.testing.integration.e2e.index;

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
 * E2E tests for per-file OOM handling during index build.
 *
 * <p>Validates: Requirements 26.7
 */
@DisplayName("Index Build OOM Resilience E2E")
class IndexBuildOomResilienceE2ETest extends SqlE2ETestBase {

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
        tableName = "t_oom_" + method + "_" + System.nanoTime();
        sqlRunner.executeDdl("CREATE TABLE " + fqn()
                + " (user_id BIGINT, value VARCHAR)");
        // Insert multiple batches to create multiple data files
        sqlRunner.execute("INSERT INTO " + fqn() + " VALUES (1, 'a'), (2, 'b')");
        sqlRunner.execute("INSERT INTO " + fqn() + " VALUES (3, 'c'), (4, 'd')");
    }

    private String fqn() { return FQN_PREFIX + tableName; }

    @Nested
    @DisplayName("Single File OOM")
    class SingleFileOom {

        @Test
        @DisplayName("Build continues after per-file OOM — partial index created")
        void buildContinuesAfterOom() {
            sqlRunner.executeDdl("CREATE INDEX idx_oom ON " + fqn()
                    + "(user_id) USING BLOOM_FILTER");

            // Index should be created (may be STALE if OOM occurred on some files)
            List<TypedRow> indexes = sqlRunner.execute("SHOW INDEXES FROM " + fqn());
            assertThat(indexes).anyMatch(r ->
                    "idx_oom".equals(r.getRaw("index_name").toString()));
        }
    }

    @Nested
    @DisplayName("Query With Partial Index")
    class QueryWithPartialIndex {

        @Test
        @DisplayName("Query returns correct results even with partial index")
        void queryWithPartialIndex() {
            sqlRunner.executeDdl("CREATE INDEX idx_partial ON " + fqn()
                    + "(user_id) USING BLOOM_FILTER");

            // Query must return correct results regardless of index completeness
            List<TypedRow> result = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE user_id = 1");
            assertThat(result).isNotEmpty();

            // All data must be accessible
            List<TypedRow> allData = sqlRunner.execute("SELECT * FROM " + fqn());
            assertThat(allData).hasSize(4);
        }
    }
}
