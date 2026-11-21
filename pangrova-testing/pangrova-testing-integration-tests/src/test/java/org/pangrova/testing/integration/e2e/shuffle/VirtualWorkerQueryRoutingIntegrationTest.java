package org.pangrova.testing.integration.e2e.shuffle;

import org.pangrova.types.value.TypedRow;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies query correctness while the coordinator routes tasks among virtual
 * worker identities backed by one worker endpoint.
 *
 * <p>This is a routing integration test. It does not validate process isolation,
 * remote transport, or worker-to-worker data exchange.
 */
@Tag("routing-integration")
@DisplayName("Virtual Worker Query Routing Integration")
class VirtualWorkerQueryRoutingIntegrationTest extends VirtualWorkerRoutingIntegrationTestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final int ROW_COUNT = 200;

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
        ordersTable = "orders_equiv_" + suffix;
        customersTable = "customers_equiv_" + suffix;

        createOrdersTable();
        createCustomersTable();
        insertOrdersData();
        insertCustomersData();
    }

    // -----------------------------------------------------------------------
    // R6.1: ORDER BY global sort correctness
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("ORDER BY remains globally sorted through virtual-worker routing")
    void routedOrderByIsGloballySorted() {
        String sql = String.format(
                "SELECT id, amount FROM %s.%s.%s ORDER BY amount ASC",
                CATALOG, SCHEMA, ordersTable);

        List<TypedRow> rows = routingSqlRunner.execute(sql);

        assertThat(rows).hasSize(ROW_COUNT);
        assertSortedAscending(rows, "amount");
    }

    // -----------------------------------------------------------------------
    // R6.2: SELECT * row count preservation
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("Routed SELECT preserves total row count")
    void routedScanPreservesRowCount() {
        String sql = String.format(
                "SELECT * FROM %s.%s.%s", CATALOG, SCHEMA, ordersTable);

        List<TypedRow> rows = routingSqlRunner.execute(sql);

        assertThat(rows).hasSize(ROW_COUNT);
        assertNoDuplicateIds(rows, "id");
    }

    // -----------------------------------------------------------------------
    // R6.3: JOIN correctness
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("Routed JOIN produces correct results")
    void routedJoinProducesExpectedRows() {
        String sql = String.format(
                "SELECT o.id, o.amount, c.name "
                + "FROM %s.%s.%s o "
                + "INNER JOIN %s.%s.%s c ON o.customer_id = c.id",
                CATALOG, SCHEMA, ordersTable,
                CATALOG, SCHEMA, customersTable);

        List<TypedRow> rows = routingSqlRunner.execute(sql);

        assertThat(rows).hasSize(ROW_COUNT);
        rows.forEach(row -> {
            assertThat(row.containsColumn("id")).isTrue();
            assertThat(row.containsColumn("amount")).isTrue();
            assertThat(row.containsColumn("name")).isTrue();
            assertThat(row.getString("name")).isNotNull();
        });
    }

    // -----------------------------------------------------------------------
    // R6.4: Aggregation correctness
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("Routed aggregation produces correct results")
    void routedAggregationProducesExpectedResult() {
        long expectedSum = computeExpectedAmountSum();
        String sql = String.format(
                "SELECT SUM(amount) AS total, COUNT(*) AS cnt "
                + "FROM %s.%s.%s",
                CATALOG, SCHEMA, ordersTable);

        List<TypedRow> rows = routingSqlRunner.execute(sql);

        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getLong("cnt"))
                .isEqualTo(ROW_COUNT);
        assertThat(rows.get(0).getLong("total"))
                .isEqualTo(expectedSum);
    }

    // -----------------------------------------------------------------------
    // Assertion helpers
    // -----------------------------------------------------------------------

    private void assertSortedAscending(
            List<TypedRow> rows, String column) {
        for (int i = 1; i < rows.size(); i++) {
            long prev = rows.get(i - 1).getLong(column);
            long curr = rows.get(i).getLong(column);
            assertThat(prev).as("row %d vs %d", i - 1, i)
                    .isLessThanOrEqualTo(curr);
        }
    }

    private void assertNoDuplicateIds(
            List<TypedRow> rows, String idColumn) {
        List<Long> ids = rows.stream()
                .map(r -> r.getLong(idColumn))
                .collect(Collectors.toList());
        assertThat(ids).doesNotHaveDuplicates();
    }

    // -----------------------------------------------------------------------
    // Table creation helpers
    // -----------------------------------------------------------------------

    private void createOrdersTable() {
        setupData().createTestTable(CATALOG, SCHEMA, ordersTable,
                "id BIGINT, customer_id BIGINT, amount BIGINT, category VARCHAR");
    }

    private void createCustomersTable() {
        setupData().createTestTable(CATALOG, SCHEMA, customersTable,
                "id BIGINT, name VARCHAR, region VARCHAR");
    }

    // -----------------------------------------------------------------------
    // Data insertion helpers
    // -----------------------------------------------------------------------

    private void insertOrdersData() {
        int batchSize = 20;
        for (int start = 0; start < ROW_COUNT; start += batchSize) {
            int end = Math.min(start + batchSize, ROW_COUNT);
            insertOrdersBatch(start, end);
        }
    }

    private void insertOrdersBatch(int startInclusive, int endExclusive) {
        StringBuilder sql = new StringBuilder("INSERT INTO ")
                .append(CATALOG).append('.').append(SCHEMA).append('.').append(ordersTable)
                .append(" (id, customer_id, amount, category) VALUES ");
        for (int i = startInclusive; i < endExclusive; i++) {
            long id = i + 1;
            long customerId = (i % 50) + 1;
            long amount = (i * 7 + 13) % 1000;
            String category = "cat_" + (i % 5);
            if (i > startInclusive) sql.append(',');
            sql.append('(').append(id).append(',').append(customerId).append(',')
                    .append(amount).append(",'").append(category).append("')");
        }
        setupRunner().execute(sql.toString());
    }

    private void insertCustomersData() {
        StringBuilder sql = new StringBuilder("INSERT INTO ")
                .append(CATALOG).append('.').append(SCHEMA).append('.').append(customersTable)
                .append(" (id, name, region) VALUES ");
        for (int i = 0; i < 50; i++) {
            long id = i + 1;
            String name = "customer_" + id;
            String region = "region_" + (i % 5);
            if (i > 0) sql.append(',');
            sql.append('(').append(id).append(",'").append(name).append("','")
                    .append(region).append("')");
        }
        setupRunner().execute(sql.toString());
    }

    // -----------------------------------------------------------------------
    // Expected value computation (Java-side oracle)
    // -----------------------------------------------------------------------

    private long computeExpectedAmountSum() {
        return IntStream.range(0, ROW_COUNT)
                .mapToLong(i -> (i * 7 + 13) % 1000)
                .sum();
    }
}
