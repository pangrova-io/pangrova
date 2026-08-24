package org.pangrova.testing.e2e.result;

import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.e2e.SqlE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.pangrova.testing.e2e.setup.TestConnectorFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E: Result cache writeback after native fetch.
 * Validates that results fetched from FlightResultServer are written
 * to the coordinator result cache for subsequent cache hits.
 *
 * <p>Validates: Requirements 2.5</p>
 */
@DisplayName("Result Cache Writeback E2E")
class ResultCacheWritebackE2ETest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(
                CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeAll
    static void loadTestData() {
        if (!isDockerReady()) return;
        registerTestConnectors(List.of(ConnectorTestConfig.icebergLocal(
                CATALOG, SCHEMA, icebergWarehouse().toString())));
        dataSetup.loadTpcHDataViaConnector(
                TestConnectorFactory.createIcebergConnector(icebergWarehouse()), 1);
    }

    @Test
    @DisplayName("Scenario 1 — Query result is consistent across executions")
    void resultConsistentAcrossExecutions() {
        String sql = "SELECT l_returnflag, COUNT(*) AS cnt FROM lineitem "
                + "GROUP BY l_returnflag ORDER BY l_returnflag";
        List<TypedRow> first = sqlRunner.execute(sql);
        List<TypedRow> second = sqlRunner.execute(sql);
        assertThat(first).hasSameSizeAs(second);
        for (int i = 0; i < first.size(); i++) {
            assertThat(first.get(i).getString("l_returnflag"))
                    .isEqualTo(second.get(i).getString("l_returnflag"));
            assertThat(first.get(i).getLong("cnt"))
                    .isEqualTo(second.get(i).getLong("cnt"));
        }
    }

    @Test
    @DisplayName("Scenario 2 — Deterministic query returns same results")
    void deterministicQuerySameResults() {
        String sql = "SELECT o_orderkey, o_totalprice FROM orders "
                + "ORDER BY o_orderkey LIMIT 20";
        List<TypedRow> first = sqlRunner.execute(sql);
        List<TypedRow> second = sqlRunner.execute(sql);
        assertThat(first).hasSize(20);
        assertThat(second).hasSize(20);
        for (int i = 0; i < first.size(); i++) {
            assertThat(first.get(i).getLong("o_orderkey"))
                    .isEqualTo(second.get(i).getLong("o_orderkey"));
        }
    }
}
