package org.pangrova.testing.e2e.shuffle;

import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies DAG dispatch through multiple virtual worker identities.
 *
 * <p>Both identities resolve to one real worker endpoint. The suite validates
 * coordinator routing, concurrent query isolation, and result correctness; it
 * does not validate cross-process transport or distributed failure isolation.
 */
@Tag("routing-integration")
@DisplayName("Virtual Worker DAG Routing Integration")
class VirtualWorkerDagRoutingIntegrationTest extends VirtualWorkerRoutingIntegrationTestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";

    private String ordersTable;
    private String customersTable;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(
                CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeEach
    void setupTableNames() {
        String suffix = Long.toHexString(System.nanoTime());
        ordersTable = "dag_orders_" + suffix;
        customersTable = "dag_customers_" + suffix;
    }

    // -----------------------------------------------------------------------
    // Scenario 1: Multi-stage JOIN through virtual-worker routing
    // INSERT 500 orders + 50 customers → JOIN + GROUP BY + ORDER BY →
    // results match a Java-side oracle.
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Scenario 1 — Multi-stage JOIN Through Virtual Routes")
    class MultiStageJoinThroughVirtualRoutes {

        private static final int ORDER_COUNT = 500;
        private static final int CUSTOMER_COUNT = 50;

        @BeforeEach
        void createAndPopulate() {
            dataSetup.createTestTable(CATALOG, SCHEMA, ordersTable,
                    "id BIGINT, customer_id BIGINT, amount BIGINT, region VARCHAR");
            dataSetup.createTestTable(CATALOG, SCHEMA, customersTable,
                    "id BIGINT, name VARCHAR, tier VARCHAR");
            insertOrders(ORDER_COUNT);
            insertCustomers(CUSTOMER_COUNT);
        }

        @Test
        @DisplayName("JOIN + GROUP BY + ORDER BY produces correct routed aggregates")
        void joinGroupByOrderByMatchesOracle() {
            String sql = String.format(
                    "SELECT c.name, SUM(o.amount) AS total, COUNT(*) AS cnt "
                    + "FROM %s.%s.%s o "
                    + "INNER JOIN %s.%s.%s c ON o.customer_id = c.id "
                    + "GROUP BY c.name "
                    + "ORDER BY total DESC",
                    CATALOG, SCHEMA, ordersTable,
                    CATALOG, SCHEMA, customersTable);

            List<TypedRow> rows = sqlRunner.execute(sql);

            assertThat(rows)
                    .as("Every customer with orders must appear")
                    .isNotEmpty();

            // Total count across all groups must equal ORDER_COUNT
            long totalCount = rows.stream()
                    .mapToLong(r -> r.getLong("cnt"))
                    .sum();
            assertThat(totalCount)
                    .as("Total order count across all customer groups")
                    .isEqualTo(ORDER_COUNT);

            // Total amount must match Java-side oracle
            long totalAmount = rows.stream()
                    .mapToLong(r -> r.getLong("total"))
                    .sum();
            assertThat(totalAmount)
                    .as("Total amount through routed execution must match oracle")
                    .isEqualTo(expectedTotalAmount(ORDER_COUNT));

            // Verify ORDER BY DESC — each row's total >= next row's total
            for (int i = 0; i < rows.size() - 1; i++) {
                long current = rows.get(i).getLong("total");
                long next = rows.get(i + 1).getLong("total");
                assertThat(current)
                        .as("Row %d total >= row %d total (ORDER BY DESC)", i, i + 1)
                        .isGreaterThanOrEqualTo(next);
            }
        }

        @Test
        @DisplayName("All orders appear in the routed JOIN result")
        void allOrdersAppearInJoinResult() {
            String sql = String.format(
                    "SELECT o.id, o.amount, c.name "
                    + "FROM %s.%s.%s o "
                    + "INNER JOIN %s.%s.%s c ON o.customer_id = c.id "
                    + "ORDER BY o.id",
                    CATALOG, SCHEMA, ordersTable,
                    CATALOG, SCHEMA, customersTable);

            List<TypedRow> rows = sqlRunner.execute(sql);

            assertThat(rows)
                    .as("All %d orders must appear in JOIN result", ORDER_COUNT)
                    .hasSize(ORDER_COUNT);

            // No duplicate IDs
            List<Long> ids = rows.stream()
                    .map(r -> r.getLong("id"))
                    .collect(Collectors.toList());
            assertThat(ids).doesNotHaveDuplicates();

            // Every row has a non-null customer name
            rows.forEach(row ->
                    assertThat(row.getString("name"))
                            .as("Customer name for order id=%s", row.getLong("id"))
                            .isNotNull());
        }
    }

    // -----------------------------------------------------------------------
    // Scenario 2: Concurrent queries through virtual-worker routes
    // Submit 5 queries concurrently → all return correct results →
    // no cross-query data contamination (proves ShuffleBufferKey isolation
    // per queryId+stageId).
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Scenario 2 — Concurrent Queries Through Virtual Routes")
    class ConcurrentQueriesThroughVirtualRoutes {

        private static final int ORDER_COUNT = 300;
        private static final int CUSTOMER_COUNT = 50;
        private static final int CONCURRENT_QUERY_COUNT = 5;
        private static final int REGION_COUNT = 5;

        @BeforeEach
        void createAndPopulate() {
            dataSetup.createTestTable(CATALOG, SCHEMA, ordersTable,
                    "id BIGINT, customer_id BIGINT, amount BIGINT, region VARCHAR");
            dataSetup.createTestTable(CATALOG, SCHEMA, customersTable,
                    "id BIGINT, name VARCHAR, tier VARCHAR");
            insertOrders(ORDER_COUNT);
            insertCustomers(CUSTOMER_COUNT);
        }

        @Test
        @DisplayName("5 concurrent queries all return correct results — no cross-query contamination")
        void concurrentQueriesReturnCorrectResults() throws Exception {
            ExecutorService executor = Executors.newFixedThreadPool(CONCURRENT_QUERY_COUNT);
            try {
                List<CompletableFuture<List<TypedRow>>> futures = new ArrayList<>();

                for (int q = 0; q < CONCURRENT_QUERY_COUNT; q++) {
                    final int queryIndex = q;
                    String regionFilter = "region_" + (queryIndex % REGION_COUNT);
                    String sql = String.format(
                            "SELECT region, SUM(amount) AS total, COUNT(*) AS cnt "
                            + "FROM %s.%s.%s "
                            + "WHERE region = '%s' "
                            + "GROUP BY region",
                            CATALOG, SCHEMA, ordersTable, regionFilter);

                    futures.add(CompletableFuture.supplyAsync(
                            () -> sqlRunner.execute(sql), executor));
                }

                // Wait for all queries to complete
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

                // Verify each query returned correct results
                for (int q = 0; q < CONCURRENT_QUERY_COUNT; q++) {
                    List<TypedRow> rows = futures.get(q).get();
                    int regionIndex = q % REGION_COUNT;
                    String expectedRegion = "region_" + regionIndex;

                    assertThat(rows)
                            .as("Query %d must return exactly 1 row for region %s",
                                    q, expectedRegion)
                            .hasSize(1);

                    TypedRow row = rows.get(0);
                    assertThat(row.getString("region"))
                            .as("Query %d region must match filter", q)
                            .isEqualTo(expectedRegion);

                    long expectedCount = expectedCountForRegion(
                            regionIndex, ORDER_COUNT, REGION_COUNT);
                    assertThat(row.getLong("cnt"))
                            .as("Query %d count for %s", q, expectedRegion)
                            .isEqualTo(expectedCount);

                    long expectedSum = expectedSumForRegion(
                            regionIndex, ORDER_COUNT, REGION_COUNT);
                    assertThat(row.getLong("total"))
                            .as("Query %d sum for %s", q, expectedRegion)
                            .isEqualTo(expectedSum);
                }
            } finally {
                executor.shutdownNow();
            }
        }

        @Test
        @DisplayName("Concurrent JOIN queries produce consistent results")
        void concurrentJoinQueriesProduceConsistentResults() throws Exception {
            ExecutorService executor = Executors.newFixedThreadPool(CONCURRENT_QUERY_COUNT);
            try {
                String sql = String.format(
                        "SELECT c.name, SUM(o.amount) AS total "
                        + "FROM %s.%s.%s o "
                        + "INNER JOIN %s.%s.%s c ON o.customer_id = c.id "
                        + "GROUP BY c.name "
                        + "ORDER BY total DESC",
                        CATALOG, SCHEMA, ordersTable,
                        CATALOG, SCHEMA, customersTable);

                List<CompletableFuture<Long>> futures = new ArrayList<>();
                for (int q = 0; q < CONCURRENT_QUERY_COUNT; q++) {
                    futures.add(CompletableFuture.supplyAsync(() -> {
                        List<TypedRow> rows = sqlRunner.execute(sql);
                        return rows.stream()
                                .mapToLong(r -> r.getLong("total"))
                                .sum();
                    }, executor));
                }

                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

                // All concurrent executions must produce the same total
                long expectedTotal = expectedTotalAmount(ORDER_COUNT);
                for (int q = 0; q < CONCURRENT_QUERY_COUNT; q++) {
                    assertThat(futures.get(q).get())
                            .as("Concurrent query %d total must match oracle", q)
                            .isEqualTo(expectedTotal);
                }
            } finally {
                executor.shutdownNow();
            }
        }
    }

    // -----------------------------------------------------------------------
    // Scenario 3: Large result set through virtual-worker routes
    // INSERT 10000 rows → SELECT * with ORDER BY → all rows returned in
    // correct order under the routing harness.
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Scenario 3 — Large Result Set Through Virtual Routes")
    class LargeResultSetThroughVirtualRoutes {

        // 500 rows is sufficient to exercise result streaming + backpressure
        // through the routing harness. 10000 rows caused disk exhaustion because
        // each single-row INSERT creates a separate Iceberg metadata version
        // file (~10KB each), and Iceberg metadata grows cumulatively.
        private static final int LARGE_ROW_COUNT = 500;

        @BeforeEach
        void createAndPopulateLargeTable() {
            dataSetup.createTestTable(CATALOG, SCHEMA, ordersTable,
                    "id BIGINT, customer_id BIGINT, amount BIGINT, region VARCHAR");
            insertOrders(LARGE_ROW_COUNT);
        }

        @Test
        @DisplayName("SELECT * ORDER BY returns all rows in correct order")
        void selectAllOrderByReturnsAllRowsInOrder() {
            String sql = String.format(
                    "SELECT id, amount FROM %s.%s.%s ORDER BY id",
                    CATALOG, SCHEMA, ordersTable);

            List<TypedRow> rows = sqlRunner.execute(sql);

            assertThat(rows)
                    .as("All %d rows must be returned", LARGE_ROW_COUNT)
                    .hasSize(LARGE_ROW_COUNT);

            // Verify ORDER BY id ascending
            for (int i = 0; i < rows.size() - 1; i++) {
                long currentId = rows.get(i).getLong("id");
                long nextId = rows.get(i + 1).getLong("id");
                assertThat(currentId)
                        .as("Row %d id < row %d id (ORDER BY ASC)", i, i + 1)
                        .isLessThan(nextId);
            }
        }

        @Test
        @DisplayName("Large result set total amount matches oracle")
        void largeResultSetTotalMatchesOracle() {
            String sql = String.format(
                    "SELECT SUM(amount) AS total FROM %s.%s.%s",
                    CATALOG, SCHEMA, ordersTable);

            List<TypedRow> rows = sqlRunner.execute(sql);

            assertThat(rows).hasSize(1);
            long total = rows.get(0).getLong("total");
            assertThat(total)
                    .as("SUM of all rows must match Java-side oracle")
                    .isEqualTo(expectedTotalAmount(LARGE_ROW_COUNT));
        }

        @Test
        @DisplayName("No duplicate IDs in routed large result set")
        void noDuplicateIdsInLargeResultSet() {
            String sql = String.format(
                    "SELECT id FROM %s.%s.%s ORDER BY id",
                    CATALOG, SCHEMA, ordersTable);

            List<TypedRow> rows = sqlRunner.execute(sql);

            assertThat(rows).hasSize(LARGE_ROW_COUNT);

            Set<Long> idSet = new HashSet<>();
            for (TypedRow row : rows) {
                long id = row.getLong("id");
                assertThat(idSet.add(id))
                        .as("ID %d must be unique after routed dispatch", id)
                        .isTrue();
            }
        }
    }

    // -----------------------------------------------------------------------
    // Shared data insertion helpers
    // -----------------------------------------------------------------------

    private void insertOrders(int rowCount) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("INSERT INTO %s.%s.%s (id, customer_id, amount, region) VALUES ",
                CATALOG, SCHEMA, ordersTable));
        for (int i = 0; i < rowCount; i++) {
            long id = i + 1;
            long customerId = (i % 50) + 1;
            long amount = (i * 7 + 3) % 500;
            String region = "region_" + (i % 5);
            if (i > 0) sb.append(", ");
            sb.append(String.format("(%d, %d, %d, '%s')", id, customerId, amount, region));
        }
        sqlRunner.execute(sb.toString());
    }

    private void insertCustomers(int count) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("INSERT INTO %s.%s.%s (id, name, tier) VALUES ",
                CATALOG, SCHEMA, customersTable));
        for (int i = 0; i < count; i++) {
            long cid = i + 1;
            String tier = (cid <= 10) ? "GOLD" : "STANDARD";
            if (i > 0) sb.append(", ");
            sb.append(String.format("(%d, 'customer_%d', '%s')", cid, cid, tier));
        }
        sqlRunner.execute(sb.toString());
    }

    // -----------------------------------------------------------------------
    // Java-side oracle computations
    // -----------------------------------------------------------------------

    private static long expectedTotalAmount(int rowCount) {
        return IntStream.range(0, rowCount)
                .mapToLong(i -> (i * 7 + 3) % 500)
                .sum();
    }

    private static long expectedCountForRegion(int regionIndex, int rowCount, int regionCount) {
        return IntStream.range(0, rowCount)
                .filter(i -> i % regionCount == regionIndex)
                .count();
    }

    private static long expectedSumForRegion(int regionIndex, int rowCount, int regionCount) {
        return IntStream.range(0, rowCount)
                .filter(i -> i % regionCount == regionIndex)
                .mapToLong(i -> (i * 7 + 3) % 500)
                .sum();
    }
}
