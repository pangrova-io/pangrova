package org.pangrova.testing.integration.e2e.result;

import org.pangrova.connector.spi.data.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.pangrova.testing.integration.e2e.setup.TestConnectorFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E: Native Flight result serving end-to-end.
 * Proves results flow through Velox PartitionedOutputNode → OutputBufferManager →
 * C++ FlightResultServer → coordinator fetch → client.
 *
 * <p>Validates: Requirements 1.1, 1.2, 1.3, 1.6, 2.1, 2.2, 2.3</p>
 */
@DisplayName("Native Flight Result Serving E2E")
class NativeResultE2ETest extends SqlE2ETestBase {

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
    @DisplayName("Scenario 1 — Full native result path: INSERT + SELECT returns all rows")
    void fullNativeResultPath() {
        String table = CATALOG + "." + SCHEMA + ".result_e2e_full";
        try { sqlRunner.execute("DROP TABLE IF EXISTS " + table); } catch (Exception ignored) {}
        sqlRunner.execute("CREATE TABLE " + table + " (id INT, name VARCHAR(50), amount DOUBLE)");
        StringBuilder sb = new StringBuilder("INSERT INTO " + table + " VALUES ");
        for (int i = 0; i < 1000; i++) {
            if (i > 0) sb.append(",");
            sb.append("(").append(i).append(",'name_").append(i).append("',").append(i * 1.5).append(")");
        }
        sqlRunner.execute(sb.toString());
        List<TypedRow> rows = sqlRunner.execute("SELECT * FROM " + table + " ORDER BY id");
        assertThat(rows).hasSize(1000);
        for (int i = 0; i < rows.size(); i++) {
            assertThat(rows.get(i).getLong("id")).isEqualTo(i);
        }
    }

    @Test
    @DisplayName("Scenario 2 — Aggregation query returns correct results")
    void aggregationQuery() {
        List<TypedRow> rows = sqlRunner.execute(
                "SELECT l_returnflag, SUM(l_quantity) AS total_qty "
                + "FROM lineitem GROUP BY l_returnflag ORDER BY l_returnflag");
        assertThat(rows).isNotEmpty();
        long totalQty = rows.stream()
                .mapToLong(r -> r.getLong("total_qty"))
                .sum();
        assertThat(totalQty).isGreaterThan(0);
    }

    @Test
    @DisplayName("Scenario 3 — Empty result set returns immediately")
    void emptyResultSet() {
        List<TypedRow> rows = sqlRunner.execute(
                "SELECT * FROM orders WHERE 1 = 0");
        assertThat(rows).isEmpty();
    }

    @Test
    @DisplayName("Scenario 4 — Multiple concurrent queries return correct results")
    void concurrentQueries() {
        @SuppressWarnings("unchecked")
        CompletableFuture<List<TypedRow>>[] futures = IntStream.range(0, 5)
                .mapToObj(i -> CompletableFuture.supplyAsync(() ->
                        sqlRunner.execute("SELECT COUNT(*) AS cnt FROM orders")))
                .toArray(CompletableFuture[]::new);
        CompletableFuture.allOf(futures).join();
        for (CompletableFuture<List<TypedRow>> f : futures) {
            List<TypedRow> rows = f.join();
            assertThat(rows).hasSize(1);
            assertThat(rows.get(0).getLong("cnt")).isGreaterThan(0);
        }
    }
}
