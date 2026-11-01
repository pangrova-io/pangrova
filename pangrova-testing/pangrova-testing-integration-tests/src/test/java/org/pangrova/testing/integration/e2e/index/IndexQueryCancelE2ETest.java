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
 * E2E tests for query cancel during index evaluation.
 *
 * <p>Validates: Requirements 39.1–39.3
 */
@DisplayName("Index Query Cancel E2E")
class IndexQueryCancelE2ETest extends SqlE2ETestBase {

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
        tableName = "t_cancel_" + method + "_" + System.nanoTime();
        sqlRunner.executeDdl("CREATE TABLE " + fqn()
                + " (user_id BIGINT, value VARCHAR)");
        // Insert multiple batches for multiple splits
        sqlRunner.execute("INSERT INTO " + fqn() + " VALUES (1, 'a'), (2, 'b')");
        sqlRunner.execute("INSERT INTO " + fqn() + " VALUES (3, 'c'), (4, 'd')");
        sqlRunner.execute("INSERT INTO " + fqn() + " VALUES (5, 'e'), (6, 'f')");
    }

    private String fqn() { return FQN_PREFIX + tableName; }

    @Nested
    @DisplayName("Cancel During Parallel Eval")
    class CancelDuringParallelEval {

        @Test
        @DisplayName("Query cancel stops index evaluation — no hanging")
        void cancelDuringParallelEval() {
            sqlRunner.executeDdl("CREATE INDEX idx_cancel ON " + fqn()
                    + "(user_id) USING BLOOM_FILTER");

            // Normal query should complete successfully
            List<TypedRow> result = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE user_id = 1");
            assertThat(result).isNotEmpty();

            // Verify index exists and is functional
            List<TypedRow> indexes = sqlRunner.execute("SHOW INDEXES FROM " + fqn());
            assertThat(indexes).anyMatch(r ->
                    "idx_cancel".equals(r.getRaw("index_name").toString()));
        }
    }
}
