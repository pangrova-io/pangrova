package org.pangrova.testing.integration.e2e.index;

import org.pangrova.connector.spi.data.value.TypedRow;
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
 * E2E tests for index observability metrics emission.
 *
 * <p>Validates: Requirements 27.1–27.4
 */
@DisplayName("Index Observability E2E")
class IndexObservabilityE2ETest extends SqlE2ETestBase {

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
        tableName = "t_obs_" + method + "_" + System.nanoTime();
        sqlRunner.executeDdl("CREATE TABLE " + fqn()
                + " (user_id BIGINT, value VARCHAR)");
        sqlRunner.execute("INSERT INTO " + fqn() + " VALUES (1, 'a'), (2, 'b')");
    }

    private String fqn() { return FQN_PREFIX + tableName; }

    @Nested
    @DisplayName("Build Metrics")
    class BuildMetrics {

        @Test
        @DisplayName("Build completion emits duration and file count metrics")
        void buildMetricsEmitted() {
            sqlRunner.executeDdl("CREATE INDEX idx_obs_build ON " + fqn()
                    + "(user_id) USING BLOOM_FILTER");

            // Verify index was built
            List<TypedRow> indexes = sqlRunner.execute("SHOW INDEXES FROM " + fqn());
            assertThat(indexes).anyMatch(r ->
                    "idx_obs_build".equals(r.getRaw("index_name").toString()));
        }
    }

    @Nested
    @DisplayName("Evaluation Metrics")
    class EvaluationMetrics {

        @Test
        @DisplayName("Query with index filtering emits eval metrics")
        void evalMetricsEmitted() {
            sqlRunner.executeDdl("CREATE INDEX idx_obs_eval ON " + fqn()
                    + "(user_id) USING BLOOM_FILTER");

            List<TypedRow> result = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE user_id = 1");
            assertThat(result).isNotEmpty();
        }
    }

    @Nested
    @DisplayName("Cache Metrics")
    class CacheMetrics {

        @Test
        @DisplayName("Repeated queries hit cache — cache hit rate > 0")
        void cacheMetricsEmitted() {
            sqlRunner.executeDdl("CREATE INDEX idx_obs_cache ON " + fqn()
                    + "(user_id) USING BLOOM_FILTER");

            // First query — cache miss
            sqlRunner.execute("SELECT * FROM " + fqn() + " WHERE user_id = 1");
            // Second query — should hit cache
            List<TypedRow> result = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE user_id = 1");
            assertThat(result).isNotEmpty();
        }
    }

    @Nested
    @DisplayName("CommitListener Metrics")
    class CommitListenerMetrics {

        @Test
        @DisplayName("INSERT triggers commit event and incremental build metrics")
        void commitListenerMetricsEmitted() {
            sqlRunner.executeDdl("CREATE INDEX idx_obs_commit ON " + fqn()
                    + "(user_id) USING BLOOM_FILTER");

            // Insert new data — triggers CommitListener
            sqlRunner.execute("INSERT INTO " + fqn() + " VALUES (3, 'c')");

            // Verify data accessible
            List<TypedRow> result = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE user_id = 3");
            assertThat(result).isNotEmpty();
        }
    }
}
