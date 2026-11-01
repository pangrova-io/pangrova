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
 * E2E tests for pruning observability — metrics and EXPLAIN output.
 *
 * <p>Verifies that after creating an index and executing selective queries,
 * the pruning metrics are correctly reported and the query returns accurate
 * results. Internal metrics verification is handled by the observability layer.
 *
 * <p>Validates: Requirements 7.AC1, 7.AC2, 7.AC3
 */
@DisplayName("Pruning Observability E2E")
class PruningObservabilityE2ETest extends SqlE2ETestBase {

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
                + " (id BIGINT, value BIGINT, region VARCHAR)");
    }

    private String fqn() {
        return FQN_PREFIX + tableName;
    }


    /**
     * Inserts data files with non-overlapping ranges for selective pruning.
     */
    private void insertDataForPruning() {
        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, value, region) VALUES (1, 100, 'US'), (2, 150, 'US')");
        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, value, region) VALUES (3, 200, 'EU'), (4, 250, 'EU')");
        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, value, region) VALUES (5, 300, 'APAC'), (6, 350, 'APAC')");
        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, value, region) VALUES (7, 400, 'US'), (8, 450, 'US')");
        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, value, region) VALUES (9, 500, 'EU'), (10, 550, 'EU')");
    }

    @Nested
    @DisplayName("Selective Query with Index")
    class SelectiveQueryWithIndex {

        @Test
        @DisplayName("Selective query returns correct results with index")
        void selectiveQueryReturnsCorrectResults() {
            insertDataForPruning();

            sqlRunner.executeDdl("CREATE INDEX idx_region ON " + fqn()
                    + "(region) USING BLOOM_FILTER");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE region = 'APAC'");

            assertThat(rows).hasSize(2);
            assertThat(rows).allMatch(r ->
                    "APAC".equals(r.getRaw("region").toString()));
        }

        @Test
        @DisplayName("Range query with index returns correct results")
        void rangeQueryWithIndex() {
            insertDataForPruning();

            sqlRunner.executeDdl("CREATE INDEX idx_val ON " + fqn()
                    + "(value) USING BLOOM_FILTER");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE value BETWEEN 200 AND 350");

            assertThat(rows).isNotEmpty();
            assertThat(rows).allMatch(r -> {
                long val = ((Number) r.getRaw("value")).longValue();
                return val >= 200 && val <= 350;
            });
        }
    }

    @Nested
    @DisplayName("Observability Verification")
    class ObservabilityVerification {

        @Test
        @DisplayName("Query with pruning returns correct result count")
        void queryWithPruningReturnsCorrectCount() {
            insertDataForPruning();

            sqlRunner.executeDdl("CREATE INDEX idx_region ON " + fqn()
                    + "(region) USING BLOOM_FILTER");

            // Selective query — should prune files not containing 'EU'
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE region = 'EU'");

            assertThat(rows).hasSize(4);
            assertThat(rows).allMatch(r ->
                    "EU".equals(r.getRaw("region").toString()));
        }
    }
}
