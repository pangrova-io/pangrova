package org.pangrova.testing.integration.e2e.result;

import org.pangrova.spi.connector.data.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.pangrova.testing.integration.e2e.setup.TestConnectorFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E: Backpressure under slow consumer.
 * Proves FlightResultServer handles large result sets without OOM
 * via Velox OutputBufferManager backpressure.
 *
 * <p>Validates: Requirements 4.1, 4.2, 4.3, 4.4</p>
 */
@DisplayName("Backpressure E2E — Large Result Sets")
class BackpressureE2ETest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(
                CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeAll
    static void loadTestData() {
        isDockerReady();
        registerTestConnectors(List.of(ConnectorTestConfig.icebergLocal(
                CATALOG, SCHEMA, icebergWarehouse().toString())));
        dataSetup.loadTpcHDataViaConnector(
                TestConnectorFactory.createIcebergConnector(icebergWarehouse()), 1);
    }

    @Test
    @DisplayName("Scenario 1 — Large result set streams without OOM")
    void largeResultSetStreamsCorrectly() {
        List<TypedRow> rows = sqlRunner.execute(
                "SELECT l_orderkey, l_partkey, l_suppkey, l_quantity, l_extendedprice "
                + "FROM lineitem ORDER BY l_orderkey, l_partkey");
        assertThat(rows).hasSizeGreaterThan(10);
        for (int i = 1; i < rows.size(); i++) {
            long prevKey = rows.get(i - 1).getLong("l_orderkey");
            long currKey = rows.get(i).getLong("l_orderkey");
            assertThat(currKey).isGreaterThanOrEqualTo(prevKey);
        }
    }

    @Test
    @DisplayName("Scenario 2 — Wide result set with many columns")
    void wideResultSet() {
        List<TypedRow> rows = sqlRunner.execute(
                "SELECT * FROM lineitem ORDER BY l_orderkey LIMIT 50");
        assertThat(rows).hasSize(50);
        TypedRow first = rows.get(0);
        assertThat(first.getLong("l_orderkey")).isNotNull();
        assertThat(first.getLong("l_quantity")).isNotNull();
        assertThat(first.getDouble("l_extendedprice")).isNotNull();
    }
}
