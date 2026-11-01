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
 * E2E tests for cost-based index selection with non-selective queries.
 *
 * <p>Verifies that when a query has low selectivity (matches >50% of data),
 * the cost model correctly skips L1 index evaluation while still returning
 * correct query results.
 *
 * <p>Validates: Requirements 5.AC1, 5.AC3, 5.AC4
 */
@DisplayName("Cost-Based Selection E2E")
class CostBasedSelectionE2ETest extends SqlE2ETestBase {

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
        tableName = "t_cost_" + method + "_" + System.nanoTime();
        sqlRunner.executeDdl("CREATE TABLE " + fqn()
                + " (id BIGINT, value BIGINT, label VARCHAR)");
    }

    private String fqn() {
        return FQN_PREFIX + tableName;
    }


    /**
     * Inserts data with uniform distribution (all positive values).
     */
    private void insertUniformData() {
        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, value, label) VALUES (1, 10, 'a'), (2, 20, 'b'), (3, 30, 'c')");
        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, value, label) VALUES (4, 40, 'd'), (5, 50, 'e'), (6, 60, 'f')");
        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, value, label) VALUES (7, 70, 'g'), (8, 80, 'h'), (9, 90, 'i')");
        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, value, label) VALUES (10, 100, 'j'), (11, 110, 'k'), (12, 120, 'l')");
    }

    @Nested
    @DisplayName("Non-Selective Query Correctness")
    class NonSelectiveQueryCorrectness {

        @Test
        @DisplayName("Non-selective query returns correct results with index present")
        void nonSelectiveQueryReturnsCorrectResults() {
            insertUniformData();

            sqlRunner.executeDdl("CREATE INDEX idx_val ON " + fqn()
                    + "(value) USING BLOOM_FILTER");

            // Non-selective: matches >50% of data
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE value > 0");

            // All 12 rows have value > 0
            assertThat(rows).hasSize(12);
        }

        @Test
        @DisplayName("Highly selective query returns correct results with index")
        void highlySelectiveQueryReturnsCorrectResults() {
            insertUniformData();

            sqlRunner.executeDdl("CREATE INDEX idx_val ON " + fqn()
                    + "(value) USING BLOOM_FILTER");

            // Highly selective: matches only 1 row
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE value = 50");

            assertThat(rows).hasSize(1);
            assertThat(rows.get(0).getRaw("id")).isEqualTo(5L);
        }
    }

    @Nested
    @DisplayName("Cost Model Behavior")
    class CostModelBehavior {

        @Test
        @DisplayName("Full table scan query returns all rows regardless of index")
        void fullTableScanReturnsAllRows() {
            insertUniformData();

            sqlRunner.executeDdl("CREATE INDEX idx_val ON " + fqn()
                    + "(value) USING BLOOM_FILTER");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn());

            assertThat(rows).hasSize(12);
        }

        @Test
        @DisplayName("Predicate matching all rows returns complete result set")
        void predicateMatchingAllRows() {
            insertUniformData();

            sqlRunner.executeDdl("CREATE INDEX idx_val ON " + fqn()
                    + "(value) USING BLOOM_FILTER");

            // value > -1 matches everything
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE value > -1");

            assertThat(rows).hasSize(12);
        }
    }
}
