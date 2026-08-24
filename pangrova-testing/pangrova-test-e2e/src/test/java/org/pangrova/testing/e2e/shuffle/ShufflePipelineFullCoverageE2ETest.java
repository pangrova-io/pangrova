package org.pangrova.testing.e2e.shuffle;

import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E test exercising every DAG stage type through the full shuffle pipeline:
 * scan → hash shuffle → partial agg → final agg → sort → output.
 *
 * <p>Extends {@link MultiWorkerTestBase} (1 coordinator + 2 virtual workers)
 * with 3 tables (orders, customers, products) and 8 scenarios covering
 * multi-table JOIN, subquery exchange, UNION ALL, window functions, HAVING,
 * INSERT SELECT, distributed LIMIT, and self-JOIN.
 *
 * <p>All data uses deterministic formulas so Java-side oracles can compute
 * expected values without querying the database.
 */
@Tag("multi-worker")
@DisplayName("Full Shuffle Pipeline Coverage — Every DAG Stage Type")
class ShufflePipelineFullCoverageE2ETest extends MultiWorkerTestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";

    private static final int ORDER_COUNT = 1000;
    private static final int CUSTOMER_COUNT = 100;
    private static final int PRODUCT_COUNT = 50;
    private static final int REGION_COUNT = 5;
    private static final String[] TIERS = {"GOLD", "SILVER", "BRONZE"};
    private static final String[] CATEGORIES = {"ELECTRONICS", "CLOTHING", "FOOD", "BOOKS", "TOYS"};

    private String ordersTable;
    private String customersTable;
    private String productsTable;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(
                CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeEach
    void setupTables() {
        String suffix = Long.toHexString(System.nanoTime());
        ordersTable = "pipe_orders_" + suffix;
        customersTable = "pipe_customers_" + suffix;
        productsTable = "pipe_products_" + suffix;

        dataSetup.createTestTable(CATALOG, SCHEMA, ordersTable,
                "id BIGINT, customer_id BIGINT, product_id BIGINT, amount BIGINT, region VARCHAR");
        dataSetup.createTestTable(CATALOG, SCHEMA, customersTable,
                "id BIGINT, name VARCHAR, tier VARCHAR");
        dataSetup.createTestTable(CATALOG, SCHEMA, productsTable,
                "id BIGINT, name VARCHAR, category VARCHAR");

        insertOrders();
        insertCustomers();
        insertProducts();
    }

    // -----------------------------------------------------------------------
    // Scenario 1: 3-table JOIN (triggers 2 hash shuffle stages)
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Scenario 1 — 3-Table JOIN")
    class ThreeTableJoin {

        @Test
        @DisplayName("3-table JOIN with GROUP BY produces correct aggregates")
        void threeTableJoinGroupByMatchesOracle() {
            String sql = String.format(
                    "SELECT c.name, p.category, SUM(o.amount) AS total "
                    + "FROM %s.%s.%s o "
                    + "INNER JOIN %s.%s.%s c ON o.customer_id = c.id "
                    + "INNER JOIN %s.%s.%s p ON o.product_id = p.id "
                    + "GROUP BY c.name, p.category "
                    + "ORDER BY total DESC",
                    CATALOG, SCHEMA, ordersTable,
                    CATALOG, SCHEMA, customersTable,
                    CATALOG, SCHEMA, productsTable);

            List<TypedRow> rows = sqlRunner.execute(sql);

            assertThat(rows)
                    .as("At least one name×category combination must exist")
                    .isNotEmpty();

            // Total SUM across all groups must equal the oracle total
            long totalAmount = rows.stream()
                    .mapToLong(r -> r.getLong("total"))
                    .sum();
            assertThat(totalAmount)
                    .as("Total amount through 3-table JOIN shuffle")
                    .isEqualTo(expectedTotalOrderAmount());

            // ORDER BY DESC correctness
            for (int i = 0; i < rows.size() - 1; i++) {
                long current = rows.get(i).getLong("total");
                long next = rows.get(i + 1).getLong("total");
                assertThat(current)
                        .as("Row %d total >= row %d total (ORDER BY DESC)", i, i + 1)
                        .isGreaterThanOrEqualTo(next);
            }

            // Every row must have non-null name and category
            rows.forEach(row -> {
                assertThat(row.getString("name")).as("customer name").isNotNull();
                assertThat(row.getString("category")).as("product category").isNotNull();
            });
        }
    }

    // -----------------------------------------------------------------------
    // Scenario 2: Subquery with shuffle (triggers broadcast or hash exchange)
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Scenario 2 — Subquery With Shuffle")
    class SubqueryWithShuffle {

        @Test
        @DisplayName("IN subquery returns only GOLD-tier customer orders")
        void inSubqueryFiltersCorrectly() {
            String sql = String.format(
                    "SELECT o.id, o.amount, o.customer_id "
                    + "FROM %s.%s.%s o "
                    + "WHERE o.customer_id IN ("
                    + "  SELECT c.id FROM %s.%s.%s c WHERE c.tier = 'GOLD'"
                    + ") "
                    + "ORDER BY o.id",
                    CATALOG, SCHEMA, ordersTable,
                    CATALOG, SCHEMA, customersTable);

            List<TypedRow> rows = sqlRunner.execute(sql);

            long expectedCount = expectedGoldTierOrderCount();
            assertThat(rows)
                    .as("Only GOLD-tier customer orders returned")
                    .hasSize((int) expectedCount);

            // Every returned order must belong to a GOLD-tier customer
            Set<Long> goldCustomerIds = goldCustomerIds();
            rows.forEach(row -> {
                long custId = row.getLong("customer_id");
                assertThat(goldCustomerIds)
                        .as("Order customer_id=%d must be GOLD tier", custId)
                        .contains(custId);
            });
        }
    }

    // -----------------------------------------------------------------------
    // Scenario 3: UNION ALL across shuffle stages
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Scenario 3 — UNION ALL Across Shuffle Stages")
    class UnionAllAcrossShuffleStages {

        @Test
        @DisplayName("UNION ALL combines region aggregates and tier counts")
        void unionAllCombinesResults() {
            String sql = String.format(
                    "SELECT region AS label, SUM(amount) AS total "
                    + "FROM %s.%s.%s GROUP BY region "
                    + "UNION ALL "
                    + "SELECT tier AS label, COUNT(*) AS total "
                    + "FROM %s.%s.%s GROUP BY tier",
                    CATALOG, SCHEMA, ordersTable,
                    CATALOG, SCHEMA, customersTable);

            List<TypedRow> rows = sqlRunner.execute(sql);

            int expectedRegionRows = REGION_COUNT;
            int expectedTierRows = TIERS.length;
            assertThat(rows)
                    .as("UNION ALL must have %d region rows + %d tier rows",
                            expectedRegionRows, expectedTierRows)
                    .hasSize(expectedRegionRows + expectedTierRows);

            // Sum of region totals must equal total order amount
            long regionSum = 0;
            int regionRowCount = 0;
            for (TypedRow row : rows) {
                String label = row.getString("label");
                if (label.startsWith("region_")) {
                    regionSum += row.getLong("total");
                    regionRowCount++;
                }
            }
            assertThat(regionRowCount).isEqualTo(REGION_COUNT);
            assertThat(regionSum)
                    .as("Sum of region totals must equal total order amount")
                    .isEqualTo(expectedTotalOrderAmount());

            // Sum of tier counts must equal CUSTOMER_COUNT
            long tierCountSum = 0;
            int tierRowCount = 0;
            for (TypedRow row : rows) {
                String label = row.getString("label");
                if (!label.startsWith("region_")) {
                    tierCountSum += row.getLong("total");
                    tierRowCount++;
                }
            }
            assertThat(tierRowCount).isEqualTo(TIERS.length);
            assertThat(tierCountSum)
                    .as("Sum of tier counts must equal CUSTOMER_COUNT")
                    .isEqualTo(CUSTOMER_COUNT);
        }
    }

    // -----------------------------------------------------------------------
    // Scenario 4: Window function with PARTITION BY
    // (triggers range/hash repartition)
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Scenario 4 — Window Function With PARTITION BY")
    class WindowFunctionPartitionBy {

        @Test
        @DisplayName("ROW_NUMBER partitioned by customer_id orders by amount DESC")
        void rowNumberPartitionedByCustomer() {
            String sql = String.format(
                    "SELECT id, customer_id, amount, "
                    + "ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY amount DESC) AS rn "
                    + "FROM %s.%s.%s",
                    CATALOG, SCHEMA, ordersTable);

            List<TypedRow> rows = sqlRunner.execute(sql);

            assertThat(rows)
                    .as("Total rows must equal ORDER_COUNT")
                    .hasSize(ORDER_COUNT);

            // Group by customer_id and verify row numbering
            Map<Long, List<TypedRow>> byCustomer = rows.stream()
                    .collect(Collectors.groupingBy(
                            r -> r.getLong("customer_id")));

            byCustomer.forEach((custId, custRows) -> {
                // Row numbers must be 1..N for each customer
                Set<Long> rowNumbers = custRows.stream()
                        .map(r -> r.getLong("rn"))
                        .collect(Collectors.toSet());
                for (long rn = 1; rn <= custRows.size(); rn++) {
                    assertThat(rowNumbers)
                            .as("Customer %d must have rn=%d", custId, rn)
                            .contains(rn);
                }

                // Within each customer, rn=1 should have the highest amount
                custRows.sort((a, b) -> Long.compare(
                        a.getLong("rn"),
                        b.getLong("rn")));
                for (int i = 0; i < custRows.size() - 1; i++) {
                    long currentAmt = custRows.get(i).getLong("amount");
                    long nextAmt = custRows.get(i + 1).getLong("amount");
                    assertThat(currentAmt)
                            .as("Customer %d: rn %d amount >= rn %d amount",
                                    custId, i + 1, i + 2)
                            .isGreaterThanOrEqualTo(nextAmt);
                }
            });
        }
    }

    // -----------------------------------------------------------------------
    // Scenario 5: HAVING filter after distributed aggregation
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Scenario 5 — HAVING Filter After Distributed Aggregation")
    class HavingFilterDistributedAgg {

        private static final long HAVING_THRESHOLD = 5000;

        @Test
        @DisplayName("HAVING SUM > threshold returns only high-value customers")
        void havingFilterReturnsHighValueCustomers() {
            String sql = String.format(
                    "SELECT customer_id, SUM(amount) AS total "
                    + "FROM %s.%s.%s "
                    + "GROUP BY customer_id "
                    + "HAVING SUM(amount) > %d "
                    + "ORDER BY total DESC",
                    CATALOG, SCHEMA, ordersTable, HAVING_THRESHOLD);

            List<TypedRow> rows = sqlRunner.execute(sql);

            // Every returned row must have total > threshold
            rows.forEach(row -> {
                long total = row.getLong("total");
                assertThat(total)
                        .as("Customer %s total must exceed %d",
                                row.getLong("customer_id"), HAVING_THRESHOLD)
                        .isGreaterThan(HAVING_THRESHOLD);
            });

            // ORDER BY DESC correctness
            for (int i = 0; i < rows.size() - 1; i++) {
                long current = rows.get(i).getLong("total");
                long next = rows.get(i + 1).getLong("total");
                assertThat(current).isGreaterThanOrEqualTo(next);
            }

            // Verify against Java oracle: count customers whose sum > threshold
            long expectedCount = expectedCustomersAboveThreshold(HAVING_THRESHOLD);
            assertThat(rows)
                    .as("Number of customers with total > %d", HAVING_THRESHOLD)
                    .hasSize((int) expectedCount);

            // Sum of returned totals must match oracle
            long returnedSum = rows.stream()
                    .mapToLong(r -> r.getLong("total"))
                    .sum();
            assertThat(returnedSum)
                    .as("Sum of high-value customer totals")
                    .isEqualTo(expectedSumAboveThreshold(HAVING_THRESHOLD));
        }
    }

    // -----------------------------------------------------------------------
    // Scenario 6: INSERT SELECT with multi-stage shuffle
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Scenario 6 — INSERT SELECT With Multi-Stage Shuffle")
    class InsertSelectMultiStageShuffle {

        @Test
        @DisplayName("INSERT SELECT preserves data through write path")
        void insertSelectPreservesData() {
            String suffix = Long.toHexString(System.nanoTime());
            String archiveTable = "pipe_archive_" + suffix;
            dataSetup.createTestTable(CATALOG, SCHEMA, archiveTable,
                    "id BIGINT, customer_id BIGINT, product_id BIGINT, amount BIGINT, region VARCHAR");

            // INSERT SELECT with JOIN filter — only GOLD-tier customer orders
            String insertSql = String.format(
                    "INSERT INTO %s.%s.%s "
                    + "SELECT o.id, o.customer_id, o.product_id, o.amount, o.region "
                    + "FROM %s.%s.%s o "
                    + "INNER JOIN %s.%s.%s c ON o.customer_id = c.id "
                    + "WHERE c.tier = 'GOLD'",
                    CATALOG, SCHEMA, archiveTable,
                    CATALOG, SCHEMA, ordersTable,
                    CATALOG, SCHEMA, customersTable);
            sqlRunner.execute(insertSql);

            // Verify row count in archive matches expected
            String countSql = String.format(
                    "SELECT COUNT(*) AS cnt FROM %s.%s.%s",
                    CATALOG, SCHEMA, archiveTable);
            List<TypedRow> countRows = sqlRunner.execute(countSql);
            long archiveCount = countRows.get(0).getLong("cnt");

            long expectedCount = expectedGoldTierOrderCount();
            assertThat(archiveCount)
                    .as("Archive table row count must match GOLD-tier order count")
                    .isEqualTo(expectedCount);

            // Verify data integrity — SUM of amounts must match
            String sumSql = String.format(
                    "SELECT SUM(amount) AS total FROM %s.%s.%s",
                    CATALOG, SCHEMA, archiveTable);
            List<TypedRow> sumRows = sqlRunner.execute(sumSql);
            long archiveSum = sumRows.get(0).getLong("total");

            assertThat(archiveSum)
                    .as("Archive SUM(amount) must match GOLD-tier order amount sum")
                    .isEqualTo(expectedGoldTierOrderAmountSum());
        }
    }

    // -----------------------------------------------------------------------
    // Scenario 7: Distributed LIMIT with ORDER BY
    // (triggers TopK + final merge)
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Scenario 7 — Distributed LIMIT With ORDER BY")
    class DistributedLimitOrderBy {

        private static final int LIMIT = 10;

        @Test
        @DisplayName("ORDER BY DESC LIMIT returns global top-10, not per-partition")
        void orderByLimitReturnsGlobalTopK() {
            String sql = String.format(
                    "SELECT id, amount FROM %s.%s.%s ORDER BY amount DESC LIMIT %d",
                    CATALOG, SCHEMA, ordersTable, LIMIT);

            List<TypedRow> rows = sqlRunner.execute(sql);

            assertThat(rows)
                    .as("Exactly %d rows returned", LIMIT)
                    .hasSize(LIMIT);

            // Verify descending order
            for (int i = 0; i < rows.size() - 1; i++) {
                long current = rows.get(i).getLong("amount");
                long next = rows.get(i + 1).getLong("amount");
                assertThat(current)
                        .as("Row %d amount >= row %d amount (DESC)", i, i + 1)
                        .isGreaterThanOrEqualTo(next);
            }

            // The smallest amount in top-10 must be >= the global 10th-largest
            long tenthLargest = expectedNthLargestAmount(LIMIT);
            long lastReturnedAmount = rows.get(LIMIT - 1).getLong("amount");
            assertThat(lastReturnedAmount)
                    .as("10th returned amount must be >= global 10th-largest")
                    .isGreaterThanOrEqualTo(tenthLargest);
        }
    }

    // -----------------------------------------------------------------------
    // Scenario 8: Self-JOIN (same table shuffled on different keys)
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Scenario 8 — Self-JOIN On Different Keys")
    class SelfJoinDifferentKeys {

        @Test
        @DisplayName("Self-JOIN on amount with id < constraint finds matching pairs")
        void selfJoinOnAmountFindsMatchingPairs() {
            String sql = String.format(
                    "SELECT a.id AS id_a, b.id AS id_b, a.amount "
                    + "FROM %s.%s.%s a "
                    + "INNER JOIN %s.%s.%s b "
                    + "ON a.amount = b.amount AND a.id < b.id "
                    + "ORDER BY a.id, b.id",
                    CATALOG, SCHEMA, ordersTable,
                    CATALOG, SCHEMA, ordersTable);

            List<TypedRow> rows = sqlRunner.execute(sql);

            // Every returned pair must satisfy: same amount, id_a < id_b
            rows.forEach(row -> {
                long idA = row.getLong("id_a");
                long idB = row.getLong("id_b");
                assertThat(idA)
                        .as("id_a must be < id_b")
                        .isLessThan(idB);
            });

            // No duplicate pairs
            Set<String> pairKeys = new HashSet<>();
            rows.forEach(row -> {
                long idA = row.getLong("id_a");
                long idB = row.getLong("id_b");
                assertThat(pairKeys.add(idA + ":" + idB))
                        .as("Pair (%d, %d) must be unique", idA, idB)
                        .isTrue();
            });

            // Verify count matches Java oracle
            long expectedPairs = expectedSelfJoinPairCount();
            assertThat(rows)
                    .as("Self-JOIN pair count must match oracle")
                    .hasSize((int) expectedPairs);
        }
    }

    // -----------------------------------------------------------------------
    // Data insertion helpers (deterministic formulas)
    // -----------------------------------------------------------------------

    private void insertOrders() {
        for (int i = 0; i < ORDER_COUNT; i++) {
            long id = i + 1;
            long customerId = (i % CUSTOMER_COUNT) + 1;
            long productId = (i % PRODUCT_COUNT) + 1;
            long amount = orderAmount(i);
            String region = "region_" + (i % REGION_COUNT);
            sqlRunner.execute(String.format(
                    "INSERT INTO %s.%s.%s (id, customer_id, product_id, amount, region) "
                    + "VALUES (%d, %d, %d, %d, '%s')",
                    CATALOG, SCHEMA, ordersTable,
                    id, customerId, productId, amount, region));
        }
    }

    private void insertCustomers() {
        for (int i = 0; i < CUSTOMER_COUNT; i++) {
            long id = i + 1;
            String tier = customerTier(i);
            sqlRunner.execute(String.format(
                    "INSERT INTO %s.%s.%s (id, name, tier) VALUES (%d, 'customer_%d', '%s')",
                    CATALOG, SCHEMA, customersTable, id, id, tier));
        }
    }

    private void insertProducts() {
        for (int i = 0; i < PRODUCT_COUNT; i++) {
            long id = i + 1;
            String category = CATEGORIES[i % CATEGORIES.length];
            sqlRunner.execute(String.format(
                    "INSERT INTO %s.%s.%s (id, name, category) VALUES (%d, 'product_%d', '%s')",
                    CATALOG, SCHEMA, productsTable, id, id, category));
        }
    }

    // -----------------------------------------------------------------------
    // Deterministic data formulas
    // -----------------------------------------------------------------------

    /** Amount for order at index i. */
    private static long orderAmount(int i) {
        return (i * 7 + 3) % 500;
    }

    /** Tier for customer at index i. */
    private static String customerTier(int i) {
        return TIERS[i % TIERS.length];
    }

    // -----------------------------------------------------------------------
    // Java-side oracle computations
    // -----------------------------------------------------------------------

    private static long expectedTotalOrderAmount() {
        return IntStream.range(0, ORDER_COUNT)
                .mapToLong(ShufflePipelineFullCoverageE2ETest::orderAmount)
                .sum();
    }

    /** Set of customer IDs with GOLD tier. */
    private static Set<Long> goldCustomerIds() {
        Set<Long> ids = new HashSet<>();
        for (int i = 0; i < CUSTOMER_COUNT; i++) {
            if ("GOLD".equals(customerTier(i))) {
                ids.add((long) (i + 1));
            }
        }
        return ids;
    }

    /** Count of orders belonging to GOLD-tier customers. */
    private static long expectedGoldTierOrderCount() {
        Set<Long> goldIds = goldCustomerIds();
        return IntStream.range(0, ORDER_COUNT)
                .filter(i -> goldIds.contains((long) ((i % CUSTOMER_COUNT) + 1)))
                .count();
    }

    /** Sum of amounts for orders belonging to GOLD-tier customers. */
    private static long expectedGoldTierOrderAmountSum() {
        Set<Long> goldIds = goldCustomerIds();
        return IntStream.range(0, ORDER_COUNT)
                .filter(i -> goldIds.contains((long) ((i % CUSTOMER_COUNT) + 1)))
                .mapToLong(ShufflePipelineFullCoverageE2ETest::orderAmount)
                .sum();
    }

    /** Count of customers whose total order amount exceeds the threshold. */
    private static long expectedCustomersAboveThreshold(long threshold) {
        return customerTotalAmounts().values().stream()
                .filter(total -> total > threshold)
                .count();
    }

    /** Sum of totals for customers above the threshold. */
    private static long expectedSumAboveThreshold(long threshold) {
        return customerTotalAmounts().values().stream()
                .filter(total -> total > threshold)
                .mapToLong(Long::longValue)
                .sum();
    }

    /** Map of customer_id → total order amount. */
    private static Map<Long, Long> customerTotalAmounts() {
        Map<Long, Long> totals = new HashMap<>();
        for (int i = 0; i < ORDER_COUNT; i++) {
            long custId = (i % CUSTOMER_COUNT) + 1;
            totals.merge(custId, orderAmount(i), Long::sum);
        }
        return totals;
    }

    /** The Nth largest order amount (1-indexed). */
    private static long expectedNthLargestAmount(int n) {
        long[] amounts = IntStream.range(0, ORDER_COUNT)
                .mapToLong(ShufflePipelineFullCoverageE2ETest::orderAmount)
                .toArray();
        java.util.Arrays.sort(amounts);
        // nth largest = amounts[ORDER_COUNT - n]
        return amounts[ORDER_COUNT - n];
    }

    /** Count of (id_a, id_b) pairs where amount_a == amount_b and id_a < id_b. */
    private static long expectedSelfJoinPairCount() {
        // Group orders by amount, count C(k,2) for each group of size k
        Map<Long, Long> amountCounts = new HashMap<>();
        for (int i = 0; i < ORDER_COUNT; i++) {
            amountCounts.merge(orderAmount(i), 1L, Long::sum);
        }
        return amountCounts.values().stream()
                .mapToLong(k -> k * (k - 1) / 2)
                .sum();
    }
}
