package org.pangrova.testing.e2e.shuffle;

import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E test proving native Arrow Flight shuffle transport moves data between
 * workers correctly.
 *
 * <p>Extends {@link MultiWorkerTestBase} (1 coordinator + 2 virtual workers).
 * Two scenarios verify: (1) native shuffle correctness with single-stage
 * GROUP BY queries, and (2) multi-stage DAG (TPC-H Q3 style) with native
 * shuffle across multiple shuffle stages.</p>
 *
 * <p>All data uses deterministic formulas so Java-side oracles can compute
 * expected values without querying the database.</p>
 */
@Tag("multi-worker")
@DisplayName("Native Flight Shuffle End-to-End")
class NativeShuffleE2ETest extends MultiWorkerTestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";

    // 500 orders is sufficient to exercise native shuffle correctness.
    // 10000 rows caused disk exhaustion: each single-row INSERT creates a
    // separate Iceberg metadata version file, and 7 tests × 10000+ rows
    // per @BeforeEach overwhelms the tempDir filesystem.
    private static final int ORDER_COUNT = 500;
    private static final int CUSTOMER_COUNT = 50;
    private static final int REGION_COUNT = 5;
    private static final String[] REGIONS = {"AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE_EAST"};

    private String ordersTable;
    private String customersTable;
    private String lineitemTable;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(
                CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeEach
    void setupTables() {
        String suffix = Long.toHexString(System.nanoTime());
        ordersTable = "nfs_orders_" + suffix;
        customersTable = "nfs_customers_" + suffix;
        lineitemTable = "nfs_lineitem_" + suffix;

        dataSetup.createTestTable(CATALOG, SCHEMA, ordersTable,
                "id BIGINT, customer_id BIGINT, amount BIGINT, region VARCHAR");
        dataSetup.createTestTable(CATALOG, SCHEMA, customersTable,
                "id BIGINT, name VARCHAR, segment VARCHAR");
        dataSetup.createTestTable(CATALOG, SCHEMA, lineitemTable,
                "id BIGINT, order_id BIGINT, quantity BIGINT, price BIGINT");

        insertOrders();
        insertCustomers();
        insertLineitems();
    }

    // -----------------------------------------------------------------------
    // Scenario 1: Native shuffle correctness
    // GROUP BY queries → verify SUM/COUNT/ORDER BY/HAVING via native shuffle
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Scenario 1 — Native Shuffle Correctness")
    class NativeShuffleCorrectness {

        @Test
        @DisplayName("GROUP BY with SUM produces correct aggregates via native shuffle")
        void groupBySumMatchesExpected() {
            String sql = String.format(
                    "SELECT region, SUM(amount) AS total, COUNT(*) AS cnt "
                    + "FROM %s.%s.%s "
                    + "GROUP BY region "
                    + "ORDER BY region",
                    CATALOG, SCHEMA, ordersTable);

            List<TypedRow> rows = sqlRunner.execute(sql);

            assertThat(rows)
                    .as("Must have one row per region")
                    .hasSize(REGION_COUNT);

            // Verify each region's SUM and COUNT against Java oracle
            for (TypedRow row : rows) {
                String region = row.getString("region");
                long total = row.getLong("total");
                long cnt = row.getLong("cnt");

                int regionIdx = regionIndex(region);
                assertThat(regionIdx)
                        .as("Region '%s' must be recognized", region)
                        .isGreaterThanOrEqualTo(0);

                assertThat(total)
                        .as("SUM(amount) for region %s", region)
                        .isEqualTo(expectedSumForRegion(regionIdx));
                assertThat(cnt)
                        .as("COUNT(*) for region %s", region)
                        .isEqualTo(expectedCountForRegion(regionIdx));
            }

            // Cross-check: total across all regions equals global sum
            long globalSum = rows.stream()
                    .mapToLong(r -> r.getLong("total"))
                    .sum();
            assertThat(globalSum)
                    .as("Global SUM across all regions")
                    .isEqualTo(expectedTotalOrderAmount());
        }

        @Test
        @DisplayName("Per-customer GROUP BY produces deterministic results via native shuffle")
        void perCustomerGroupByMatchesOracle() {
            String sql = String.format(
                    "SELECT customer_id, SUM(amount) AS total "
                    + "FROM %s.%s.%s "
                    + "GROUP BY customer_id "
                    + "ORDER BY customer_id",
                    CATALOG, SCHEMA, ordersTable);

            List<TypedRow> rows = sqlRunner.execute(sql);

            // Every customer that has orders must appear
            Map<Long, Long> expectedTotals = customerTotalAmounts();
            assertThat(rows)
                    .as("One row per customer with orders")
                    .hasSize(expectedTotals.size());

            for (TypedRow row : rows) {
                long custId = row.getLong("customer_id");
                long total = row.getLong("total");
                assertThat(total)
                        .as("SUM(amount) for customer_id=%d", custId)
                        .isEqualTo(expectedTotals.get(custId));
            }
        }

        @Test
        @DisplayName("GROUP BY with ORDER BY DESC produces correct sort order via native shuffle")
        void groupByWithOrderByDescMatchesOracle() {
            String sql = String.format(
                    "SELECT region, SUM(amount) AS total "
                    + "FROM %s.%s.%s "
                    + "GROUP BY region "
                    + "ORDER BY total DESC",
                    CATALOG, SCHEMA, ordersTable);

            List<TypedRow> rows = sqlRunner.execute(sql);

            assertThat(rows)
                    .as("Must have one row per region")
                    .hasSize(REGION_COUNT);

            long globalSum = rows.stream()
                    .mapToLong(r -> r.getLong("total"))
                    .sum();
            assertThat(globalSum)
                    .as("Global SUM must match oracle")
                    .isEqualTo(expectedTotalOrderAmount());

            // ORDER BY DESC correctness
            for (int i = 0; i < rows.size() - 1; i++) {
                long current = rows.get(i).getLong("total");
                long next = rows.get(i + 1).getLong("total");
                assertThat(current)
                        .as("Row %d total >= row %d total (ORDER BY DESC)", i, i + 1)
                        .isGreaterThanOrEqualTo(next);
            }
        }

        @Test
        @DisplayName("GROUP BY with HAVING filters correctly via native shuffle")
        void groupByWithHavingMatchesOracle() {
            String sql = String.format(
                    "SELECT customer_id, SUM(amount) AS total, COUNT(*) AS cnt "
                    + "FROM %s.%s.%s "
                    + "GROUP BY customer_id "
                    + "HAVING COUNT(*) > 1 "
                    + "ORDER BY total DESC",
                    CATALOG, SCHEMA, ordersTable);

            List<TypedRow> rows = sqlRunner.execute(sql);

            // Every returned customer must have count > 1
            rows.forEach(row -> {
                long cnt = row.getLong("cnt");
                assertThat(cnt)
                        .as("Customer %s must have count > 1",
                                row.getLong("customer_id"))
                        .isGreaterThan(1);
            });

            // Verify count matches oracle
            Map<Long, Long> expectedTotals = customerTotalAmounts();
            long expectedCustomersWithMultipleOrders = expectedTotals.entrySet().stream()
                    .filter(e -> customerOrderCount(e.getKey()) > 1)
                    .count();
            assertThat(rows)
                    .as("Customers with multiple orders")
                    .hasSize((int) expectedCustomersWithMultipleOrders);
        }
    }

    // -----------------------------------------------------------------------
    // Scenario 2: Multi-stage DAG with native shuffle (TPC-H Q3 style)
    // JOIN + GROUP BY + ORDER BY → multiple shuffle stages → correct results
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Scenario 2 — Multi-Stage DAG With Native Shuffle (TPC-H Q3 Style)")
    class MultiStageDagNativeShuffle {

        @Test
        @DisplayName("TPC-H Q3 style: JOIN + GROUP BY + ORDER BY across multiple shuffle stages")
        void tpchQ3StyleQueryMatchesBaseline() {
            // TPC-H Q3: revenue from orders joined with customers filtered by
            // segment, grouped by order, sorted by revenue descending.
            // This triggers: hash shuffle for JOIN → partial agg → final agg → sort.
            String sql = String.format(
                    "SELECT o.id AS order_id, c.segment, "
                    + "SUM(l.quantity * l.price) AS revenue "
                    + "FROM %s.%s.%s o "
                    + "INNER JOIN %s.%s.%s c ON o.customer_id = c.id "
                    + "INNER JOIN %s.%s.%s l ON o.id = l.order_id "
                    + "WHERE c.segment = 'BUILDING' "
                    + "GROUP BY o.id, c.segment "
                    + "ORDER BY revenue DESC",
                    CATALOG, SCHEMA, ordersTable,
                    CATALOG, SCHEMA, customersTable,
                    CATALOG, SCHEMA, lineitemTable);

            List<TypedRow> rows = sqlRunner.execute(sql);

            // Only orders belonging to BUILDING-segment customers should appear
            assertThat(rows).as("At least one BUILDING-segment order").isNotEmpty();

            rows.forEach(row -> {
                String segment = row.getString("segment");
                assertThat(segment)
                        .as("All rows must have segment=BUILDING")
                        .isEqualTo("BUILDING");
            });

            // ORDER BY revenue DESC correctness
            for (int i = 0; i < rows.size() - 1; i++) {
                long current = rows.get(i).getLong("revenue");
                long next = rows.get(i + 1).getLong("revenue");
                assertThat(current)
                        .as("Row %d revenue >= row %d revenue (DESC)", i, i + 1)
                        .isGreaterThanOrEqualTo(next);
            }

            // Verify total revenue matches Java oracle for BUILDING segment
            long actualTotalRevenue = rows.stream()
                    .mapToLong(r -> r.getLong("revenue"))
                    .sum();
            assertThat(actualTotalRevenue)
                    .as("Total revenue for BUILDING segment")
                    .isEqualTo(expectedBuildingSegmentRevenue());

            // Verify order count matches oracle
            long expectedOrderCount = expectedBuildingSegmentOrderCount();
            assertThat(rows)
                    .as("Number of BUILDING-segment orders with lineitems")
                    .hasSize((int) expectedOrderCount);
        }

        @Test
        @DisplayName("Multi-stage JOIN with aggregation: all segments produce correct totals")
        void allSegmentsProduceCorrectTotals() {
            String sql = String.format(
                    "SELECT c.segment, COUNT(DISTINCT o.id) AS order_count, "
                    + "SUM(l.quantity * l.price) AS total_revenue "
                    + "FROM %s.%s.%s o "
                    + "INNER JOIN %s.%s.%s c ON o.customer_id = c.id "
                    + "INNER JOIN %s.%s.%s l ON o.id = l.order_id "
                    + "GROUP BY c.segment "
                    + "ORDER BY total_revenue DESC",
                    CATALOG, SCHEMA, ordersTable,
                    CATALOG, SCHEMA, customersTable,
                    CATALOG, SCHEMA, lineitemTable);

            List<TypedRow> rows = sqlRunner.execute(sql);

            assertThat(rows)
                    .as("Must have one row per segment")
                    .hasSize(SEGMENTS.length);

            // Total revenue across all segments must equal global oracle
            long globalRevenue = rows.stream()
                    .mapToLong(r -> r.getLong("total_revenue"))
                    .sum();
            assertThat(globalRevenue)
                    .as("Global revenue across all segments")
                    .isEqualTo(expectedTotalRevenue());

            // ORDER BY DESC correctness
            for (int i = 0; i < rows.size() - 1; i++) {
                long current = rows.get(i).getLong("total_revenue");
                long next = rows.get(i + 1).getLong("total_revenue");
                assertThat(current).isGreaterThanOrEqualTo(next);
            }
        }

        @Test
        @DisplayName("Multi-stage with LIMIT: top revenue orders across shuffle stages")
        void topRevenueOrdersWithLimit() {
            String sql = String.format(
                    "SELECT o.id AS order_id, "
                    + "SUM(l.quantity * l.price) AS revenue "
                    + "FROM %s.%s.%s o "
                    + "INNER JOIN %s.%s.%s l ON o.id = l.order_id "
                    + "GROUP BY o.id "
                    + "ORDER BY revenue DESC "
                    + "LIMIT 20",
                    CATALOG, SCHEMA, ordersTable,
                    CATALOG, SCHEMA, lineitemTable);

            List<TypedRow> rows = sqlRunner.execute(sql);

            assertThat(rows)
                    .as("Exactly 20 rows returned")
                    .hasSize(20);

            // Verify descending order
            for (int i = 0; i < rows.size() - 1; i++) {
                long current = rows.get(i).getLong("revenue");
                long next = rows.get(i + 1).getLong("revenue");
                assertThat(current)
                        .as("Row %d revenue >= row %d revenue (DESC)", i, i + 1)
                        .isGreaterThanOrEqualTo(next);
            }

            // The 20th row's revenue must be >= the global 20th-largest
            long twentyThLargest = expectedNthLargestOrderRevenue(20);
            long lastReturned = rows.get(19).getLong("revenue");
            assertThat(lastReturned)
                    .as("20th returned revenue must be >= global 20th-largest")
                    .isGreaterThanOrEqualTo(twentyThLargest);
        }
    }

    // -----------------------------------------------------------------------
    // Data insertion helpers (deterministic formulas)
    // -----------------------------------------------------------------------

    private static final String[] SEGMENTS = {"AUTOMOBILE", "BUILDING", "FURNITURE", "HOUSEHOLD", "MACHINERY"};

    private void insertOrders() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format(
                "INSERT INTO %s.%s.%s (id, customer_id, amount, region) VALUES ",
                CATALOG, SCHEMA, ordersTable));
        for (int i = 0; i < ORDER_COUNT; i++) {
            long id = i + 1;
            long customerId = (i % CUSTOMER_COUNT) + 1;
            long amount = orderAmount(i);
            String region = REGIONS[i % REGION_COUNT];
            if (i > 0) sb.append(", ");
            sb.append(String.format("(%d, %d, %d, '%s')", id, customerId, amount, region));
        }
        sqlRunner.execute(sb.toString());
    }

    private void insertCustomers() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format(
                "INSERT INTO %s.%s.%s (id, name, segment) VALUES ",
                CATALOG, SCHEMA, customersTable));
        for (int i = 0; i < CUSTOMER_COUNT; i++) {
            long id = i + 1;
            String segment = SEGMENTS[i % SEGMENTS.length];
            if (i > 0) sb.append(", ");
            sb.append(String.format("(%d, 'customer_%d', '%s')", id, id, segment));
        }
        sqlRunner.execute(sb.toString());
    }

    private void insertLineitems() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format(
                "INSERT INTO %s.%s.%s (id, order_id, quantity, price) VALUES ",
                CATALOG, SCHEMA, lineitemTable));
        boolean first = true;
        for (int i = 0; i < ORDER_COUNT; i++) {
            long orderId = i + 1;
            int lineitemCount = (i % 3) + 1;
            for (int j = 0; j < lineitemCount; j++) {
                long lineitemId = i * 3L + j + 1;
                long quantity = lineitemQuantity(i, j);
                long price = lineitemPrice(i, j);
                if (!first) sb.append(", ");
                sb.append(String.format("(%d, %d, %d, %d)",
                        lineitemId, orderId, quantity, price));
                first = false;
            }
        }
        sqlRunner.execute(sb.toString());
    }

    // -----------------------------------------------------------------------
    // Deterministic data formulas
    // -----------------------------------------------------------------------

    private static long orderAmount(int i) {
        return (i * 13 + 7) % 1000;
    }

    private static long lineitemQuantity(int orderIdx, int lineIdx) {
        return ((orderIdx * 7 + lineIdx * 3 + 1) % 10) + 1;
    }

    private static long lineitemPrice(int orderIdx, int lineIdx) {
        return ((orderIdx * 11 + lineIdx * 5 + 2) % 100) + 10;
    }

    private static String customerSegment(int customerIdx) {
        return SEGMENTS[customerIdx % SEGMENTS.length];
    }

    private static int regionIndex(String region) {
        for (int i = 0; i < REGIONS.length; i++) {
            if (REGIONS[i].equals(region)) {
                return i;
            }
        }
        return -1;
    }

    // -----------------------------------------------------------------------
    // Java-side oracle computations
    // -----------------------------------------------------------------------

    private static long expectedTotalOrderAmount() {
        return IntStream.range(0, ORDER_COUNT)
                .mapToLong(NativeShuffleE2ETest::orderAmount)
                .sum();
    }

    private static long expectedCountForRegion(int regionIdx) {
        return IntStream.range(0, ORDER_COUNT)
                .filter(i -> i % REGION_COUNT == regionIdx)
                .count();
    }

    private static long expectedSumForRegion(int regionIdx) {
        return IntStream.range(0, ORDER_COUNT)
                .filter(i -> i % REGION_COUNT == regionIdx)
                .mapToLong(NativeShuffleE2ETest::orderAmount)
                .sum();
    }

    private static Map<Long, Long> customerTotalAmounts() {
        Map<Long, Long> totals = new HashMap<>();
        for (int i = 0; i < ORDER_COUNT; i++) {
            long custId = (i % CUSTOMER_COUNT) + 1;
            totals.merge(custId, orderAmount(i), Long::sum);
        }
        return totals;
    }

    private static long customerOrderCount(long customerId) {
        return IntStream.range(0, ORDER_COUNT)
                .filter(i -> (i % CUSTOMER_COUNT) + 1 == customerId)
                .count();
    }

    /** Revenue for a single order = SUM(quantity * price) across its lineitems. */
    private static long orderRevenue(int orderIdx) {
        int lineitemCount = (orderIdx % 3) + 1;
        long revenue = 0;
        for (int j = 0; j < lineitemCount; j++) {
            revenue += lineitemQuantity(orderIdx, j) * lineitemPrice(orderIdx, j);
        }
        return revenue;
    }

    /** Total revenue for all orders belonging to BUILDING-segment customers. */
    private static long expectedBuildingSegmentRevenue() {
        long total = 0;
        for (int i = 0; i < ORDER_COUNT; i++) {
            int custIdx = i % CUSTOMER_COUNT;
            if ("BUILDING".equals(customerSegment(custIdx))) {
                total += orderRevenue(i);
            }
        }
        return total;
    }

    /** Count of distinct orders belonging to BUILDING-segment customers. */
    private static long expectedBuildingSegmentOrderCount() {
        return IntStream.range(0, ORDER_COUNT)
                .filter(i -> "BUILDING".equals(customerSegment(i % CUSTOMER_COUNT)))
                .count();
    }

    /** Total revenue across all orders (all segments). */
    private static long expectedTotalRevenue() {
        return IntStream.range(0, ORDER_COUNT)
                .mapToLong(NativeShuffleE2ETest::orderRevenue)
                .sum();
    }

    /** The Nth largest order revenue (1-indexed). */
    private static long expectedNthLargestOrderRevenue(int n) {
        long[] revenues = IntStream.range(0, ORDER_COUNT)
                .mapToLong(NativeShuffleE2ETest::orderRevenue)
                .toArray();
        java.util.Arrays.sort(revenues);
        return revenues[ORDER_COUNT - n];
    }
}
