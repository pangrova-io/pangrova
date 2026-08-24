package org.pangrova.testing.e2e.shuffle;

import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E test for distributed deployment: 1 Coordinator + 2 Workers.
 *
 * <p>Validates multi-stage query execution with shuffle data flowing
 * between workers. Covers the full pipeline:
 * SQL → Parser → Planner → Substrait → Native → Transport → Client.
 *
 * <p>Implements: R9.2
 */
@Tag("multi-worker")
@DisplayName("Distributed E2E — 1 Coordinator + 2 Workers")
class DistributedE2ETest extends MultiWorkerTestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final int ORDER_COUNT = 100;
    private static final int CUSTOMER_COUNT = 20;

    private String ordersTable;
    private String customersTable;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(
                CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeEach
    void setupTables() {
        String suffix = Long.toHexString(System.nanoTime());
        ordersTable = "dist_orders_" + suffix;
        customersTable = "dist_customers_" + suffix;

        dataSetup.createTestTable(CATALOG, SCHEMA, ordersTable,
                "id BIGINT, customer_id BIGINT, amount BIGINT, region VARCHAR");
        dataSetup.createTestTable(CATALOG, SCHEMA, customersTable,
                "id BIGINT, name VARCHAR");

        insertOrders();
        insertCustomers();
    }

    // -----------------------------------------------------------------------
    // Multi-stage shuffle: JOIN triggers hash-partitioned shuffle
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Multi-Stage Shuffle Query")
    class MultiStageShuffleQuery {

        @Test
        @DisplayName("JOIN + GROUP BY executes across 2 workers with correct results")
        void joinGroupByAcrossTwoWorkers() {
            String sql = String.format(
                    "SELECT c.name, SUM(o.amount) AS total_amount, COUNT(*) AS order_count "
                    + "FROM %s.%s.%s o "
                    + "INNER JOIN %s.%s.%s c ON o.customer_id = c.id "
                    + "GROUP BY c.name "
                    + "ORDER BY total_amount DESC",
                    CATALOG, SCHEMA, ordersTable,
                    CATALOG, SCHEMA, customersTable);

            List<TypedRow> rows = sqlRunner.execute(sql);

            assertThat(rows).hasSize(CUSTOMER_COUNT);
            long totalOrders = rows.stream()
                    .mapToLong(r -> r.getLong("order_count"))
                    .sum();
            assertThat(totalOrders).isEqualTo(ORDER_COUNT);

            long totalAmount = rows.stream()
                    .mapToLong(r -> r.getLong("total_amount"))
                    .sum();
            assertThat(totalAmount).isEqualTo(expectedTotalAmount());

            assertDescendingOrder(rows, "total_amount");
        }

        @Test
        @DisplayName("Multi-stage aggregation preserves row count across shuffle")
        void multiStageAggregationPreservesCount() {
            String sql = String.format(
                    "SELECT region, COUNT(*) AS cnt, SUM(amount) AS total "
                    + "FROM %s.%s.%s "
                    + "GROUP BY region "
                    + "ORDER BY region",
                    CATALOG, SCHEMA, ordersTable);

            List<TypedRow> rows = sqlRunner.execute(sql);

            assertThat(rows).isNotEmpty();
            long totalCount = rows.stream()
                    .mapToLong(r -> r.getLong("cnt"))
                    .sum();
            assertThat(totalCount).isEqualTo(ORDER_COUNT);
        }
    }

    // -----------------------------------------------------------------------
    // Shuffle data flow verification
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Shuffle Data Flow")
    class ShuffleDataFlow {

        @Test
        @DisplayName("Hash shuffle distributes data correctly for JOIN")
        void hashShuffleDistributesForJoin() {
            String sql = String.format(
                    "SELECT o.id, c.name "
                    + "FROM %s.%s.%s o "
                    + "INNER JOIN %s.%s.%s c ON o.customer_id = c.id "
                    + "ORDER BY o.id",
                    CATALOG, SCHEMA, ordersTable,
                    CATALOG, SCHEMA, customersTable);

            List<TypedRow> rows = sqlRunner.execute(sql);

            assertThat(rows).hasSize(ORDER_COUNT);
            for (int i = 0; i < rows.size(); i++) {
                long id = rows.get(i).getLong("id");
                assertThat(id).isEqualTo(i + 1);
                assertThat(rows.get(i).getString("name")).isNotNull();
            }
        }

        @Test
        @DisplayName("Distributed DISTINCT eliminates duplicates across workers")
        void distributedDistinctEliminatesDuplicates() {
            String sql = String.format(
                    "SELECT DISTINCT region FROM %s.%s.%s ORDER BY region",
                    CATALOG, SCHEMA, ordersTable);

            List<TypedRow> rows = sqlRunner.execute(sql);

            assertThat(rows).hasSize(5);
            for (int i = 0; i < rows.size(); i++) {
                assertThat(rows.get(i).getString("region"))
                        .isEqualTo("region_" + i);
            }
        }
    }

    // -----------------------------------------------------------------------
    // Data helpers
    // -----------------------------------------------------------------------

    private void insertOrders() {
        for (int i = 0; i < ORDER_COUNT; i++) {
            long id = i + 1;
            long customerId = (i % CUSTOMER_COUNT) + 1;
            long amount = (i * 7 + 3) % 500;
            String region = "region_" + (i % 5);
            sqlRunner.execute(String.format(
                    "INSERT INTO %s.%s.%s (id, customer_id, amount, region) "
                    + "VALUES (%d, %d, %d, '%s')",
                    CATALOG, SCHEMA, ordersTable, id, customerId, amount, region));
        }
    }

    private void insertCustomers() {
        for (int i = 0; i < CUSTOMER_COUNT; i++) {
            long id = i + 1;
            sqlRunner.execute(String.format(
                    "INSERT INTO %s.%s.%s (id, name) VALUES (%d, 'customer_%d')",
                    CATALOG, SCHEMA, customersTable, id, id));
        }
    }

    private long expectedTotalAmount() {
        return IntStream.range(0, ORDER_COUNT)
                .mapToLong(i -> (i * 7 + 3) % 500)
                .sum();
    }

    private void assertDescendingOrder(List<TypedRow> rows, String column) {
        for (int i = 1; i < rows.size(); i++) {
            long prev = rows.get(i - 1).getLong(column);
            long curr = rows.get(i).getLong(column);
            assertThat(prev).as("row %d vs %d", i - 1, i)
                    .isGreaterThanOrEqualTo(curr);
        }
    }
}
