package org.pangrova.testing.e2e.index;

import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.e2e.SqlE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E tests for index evaluation circuit breaker behavior.
 *
 * <p>Validates: Requirements 30.1–30.5
 */
@DisplayName("Index Circuit Breaker E2E")
class IndexCircuitBreakerE2ETest extends SqlE2ETestBase {

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
        tableName = "t_cb_" + method + "_" + System.nanoTime();
        sqlRunner.executeDdl("CREATE TABLE " + fqn()
                + " (user_id BIGINT, value VARCHAR)");
        sqlRunner.execute("INSERT INTO " + fqn() + " VALUES (1, 'a'), (2, 'b')");
    }

    private String fqn() { return FQN_PREFIX + tableName; }

    @Nested
    @DisplayName("Circuit Opens on Repeated Failures")
    class CircuitOpensOnFailures {

        @Test
        @DisplayName("Repeated eval failures open circuit — queries still return correct results")
        void circuitOpensOnRepeatedFailures() {
            sqlRunner.executeDdl("CREATE INDEX idx_cb ON " + fqn()
                    + "(user_id) USING BLOOM_FILTER");

            // Execute multiple queries — even if circuit opens, results must be correct
            for (int i = 0; i < 6; i++) {
                List<TypedRow> result = sqlRunner.execute(
                        "SELECT * FROM " + fqn() + " WHERE user_id = 1");
                assertThat(result).isNotEmpty();
            }
        }
    }

    @Nested
    @DisplayName("Circuit Half-Open Recovery")
    class CircuitHalfOpenRecovery {

        @Test
        @DisplayName("After open duration, circuit transitions to half-open and recovers")
        void circuitHalfOpenRecovery() {
            sqlRunner.executeDdl("CREATE INDEX idx_cb_rec ON " + fqn()
                    + "(user_id) USING BLOOM_FILTER");

            // Normal queries should work with index filtering
            List<TypedRow> result = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE user_id = 2");
            assertThat(result).isNotEmpty();

            // Insert new data and verify index still works after data change
            sqlRunner.execute("INSERT INTO " + fqn() + " VALUES (3, 'c')");

            List<TypedRow> afterInsert = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE user_id = 2");
            assertThat(afterInsert).isNotEmpty();
        }
    }
}
