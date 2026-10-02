package org.pangrova.testing.e2e.read;

import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.e2e.SqlE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E tests for GROUPING SETS / CUBE / ROLLUP SQL features.
 *
 * <p>Validates end-to-end correctness through the full pipeline:
 * SQL → Parser → Planner → Substrait → Native (Velox) → Result.
 * GROUPING SETS queries produce multi-level aggregation results
 * (subtotals + grand total) in a single query execution.</p>
 *
 * <p>Requirements: 3.1, 3.2, 3.3, 3.4, 8.1</p>
 */
@DisplayName("GROUPING SETS / CUBE / ROLLUP - E2E Tests")
class GroupingSetsTest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final int SCALE_FACTOR = 1;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeAll
    static void loadTestData() {
        isDockerReady();
        registerTestConnectors(List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString())));
        dataSetup.loadTpcHDataViaConnector(
                org.pangrova.testing.e2e.setup.TestConnectorFactory.createIcebergConnector(icebergWarehouse()),
                SCALE_FACTOR);
    }

    @Nested
    @DisplayName("GROUPING SETS basic")
    class GroupingSetsBasic {

        @Test
        @DisplayName("GROUPING SETS produces subtotals and grand total")
        void groupingSetsProducesSubtotalsAndGrandTotal() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT o_orderstatus, SUM(o_totalprice) AS total "
                    + "FROM orders "
                    + "GROUP BY GROUPING SETS ((o_orderstatus), ())");

            assertThat(rows).hasSizeGreaterThanOrEqualTo(2);
        }
    }

    @Nested
    @DisplayName("CUBE")
    class CubeTests {

        @Test
        @DisplayName("CUBE produces all subset combinations")
        void cubeProducesAllSubsetCombinations() {
            List<TypedRow> cubeRows = sqlRunner.execute(
                    "SELECT o_orderstatus, o_orderpriority, COUNT(*) AS cnt "
                    + "FROM orders "
                    + "GROUP BY CUBE(o_orderstatus, o_orderpriority)");

            assertThat(cubeRows).hasSizeGreaterThanOrEqualTo(4);
        }
    }

    @Nested
    @DisplayName("ROLLUP")
    class RollupTests {

        @Test
        @DisplayName("ROLLUP produces hierarchical aggregation")
        void rollupProducesHierarchicalAggregation() {
            List<TypedRow> rollupRows = sqlRunner.execute(
                    "SELECT o_orderstatus, COUNT(*) AS cnt "
                    + "FROM orders "
                    + "GROUP BY ROLLUP(o_orderstatus)");

            assertThat(rollupRows).hasSizeGreaterThanOrEqualTo(2);

            List<TypedRow> totalRows = sqlRunner.execute(
                    "SELECT COUNT(*) AS cnt FROM orders");
            long expectedTotal = ((Number) totalRows.get(0).getRaw("cnt")).longValue();

            long rollupTotal = rollupRows.stream()
                    .mapToLong(r -> ((Number) r.getRaw("cnt")).longValue())
                    .sum();
            assertThat(rollupTotal).isEqualTo(2 * expectedTotal);
        }
    }
}
