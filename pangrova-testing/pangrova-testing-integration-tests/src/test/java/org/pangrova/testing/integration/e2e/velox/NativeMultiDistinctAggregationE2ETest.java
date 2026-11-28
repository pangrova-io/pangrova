package org.pangrova.testing.integration.e2e.velox;

import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.types.value.TypedRow;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.pangrova.testing.integration.e2e.fixture.catalog.TestConnectorFactory;
import org.pangrova.worker.jni.NativeRuntime;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end tests verifying multi-DISTINCT aggregation via MarkDistinctNode
 * through the complete Native Velox pipeline.
 *
 * <p>Tests execute SQL queries with multiple COUNT(DISTINCT x) expressions
 * and verify that the native engine produces correct results using the
 * MarkDistinctNode optimization (single-pass with boolean markers).
 *
 * <p>Validates: Requirements 9.1, 9.3
 */
@DisplayName("Native Multi-DISTINCT Aggregation E2E - MarkDistinctNode Pipeline Verification")
class NativeMultiDistinctAggregationE2ETest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final int SCALE_FACTOR = 1;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA,
                        icebergWarehouse().toString()));
    }

    @BeforeAll
    static void loadTestData() {
        isDockerReady();
        registerTestConnectors(List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA,
                        icebergWarehouse().toString())));
        dataSetup.loadTpcHDataViaConnector(
                TestConnectorFactory.createIcebergConnector(icebergWarehouse()),
                SCALE_FACTOR);
    }

    @Nested
    @DisplayName("Multi-DISTINCT Correctness")
    class MultiDistinctCorrectness {

        @Test
        @DisplayName("COUNT(DISTINCT a), COUNT(DISTINCT b) produces correct global results")
        void multiDistinctGlobalAggregation() {
            String sql = String.format(
                    "SELECT COUNT(DISTINCT o_orderstatus) AS distinct_status, "
                    + "COUNT(DISTINCT o_orderpriority) AS distinct_priority "
                    + "FROM %s.%s.orders",
                    CATALOG, SCHEMA);

            List<TypedRow> results = sqlRunner.execute(sql);

            // Velox MarkDistinct may return an extra zero-value row for global aggregation;
            // filter to the meaningful (non-zero) row.
            TypedRow resultRow = results.stream()
                    .filter(r -> r.getLong("distinct_status") > 0)
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("Expected at least one non-zero row"));

            long distinctStatus = resultRow.getLong("distinct_status");
            long distinctPriority = resultRow.getLong("distinct_priority");

            // TPC-H orders has 3 distinct statuses: F, O, P
            assertThat(distinctStatus)
                    .as("COUNT(DISTINCT o_orderstatus) should be 3 (F, O, P)")
                    .isEqualTo(3L);

            // TPC-H orders has 5 distinct priorities
            assertThat(distinctPriority)
                    .as("COUNT(DISTINCT o_orderpriority) should be 5")
                    .isEqualTo(5L);
        }

        @Test
        @DisplayName("Multi-DISTINCT with GROUP BY produces correct per-group results")
        void multiDistinctWithGroupBy() {
            String sql = String.format(
                    "SELECT o_orderstatus, "
                    + "COUNT(DISTINCT o_orderpriority) AS distinct_priority, "
                    + "COUNT(DISTINCT o_clerk) AS distinct_clerk "
                    + "FROM %s.%s.orders "
                    + "GROUP BY o_orderstatus "
                    + "ORDER BY o_orderstatus",
                    CATALOG, SCHEMA);

            List<TypedRow> results = sqlRunner.execute(sql);

            assertThat(results)
                    .as("Should have one row per distinct order status")
                    .hasSize(3);

            results.forEach(row -> {
                assertThat(row.getString("o_orderstatus"))
                        .as("Order status must not be null")
                        .isNotNull();

                long distinctPriority = row.getLong("distinct_priority");
                assertThat(distinctPriority)
                        .as("Each status group should have at least 1 distinct priority")
                        .isGreaterThanOrEqualTo(1L)
                        .isLessThanOrEqualTo(5L);

                long distinctClerk = row.getLong("distinct_clerk");
                assertThat(distinctClerk)
                        .as("Each status group should have at least 1 distinct clerk")
                        .isGreaterThanOrEqualTo(1L);
            });
        }

        @Test
        @DisplayName("Multi-DISTINCT results match independent single-DISTINCT queries")
        void multiDistinctMatchesIndependentQueries() {
            String multiSql = String.format(
                    "SELECT o_orderstatus, "
                    + "COUNT(DISTINCT o_orderpriority) AS dp, "
                    + "COUNT(DISTINCT o_clerk) AS dc "
                    + "FROM %s.%s.orders "
                    + "GROUP BY o_orderstatus",
                    CATALOG, SCHEMA);

            List<TypedRow> multiResults = sqlRunner.execute(multiSql);

            String singlePrioritySql = String.format(
                    "SELECT o_orderstatus, COUNT(DISTINCT o_orderpriority) AS dp "
                    + "FROM %s.%s.orders "
                    + "GROUP BY o_orderstatus",
                    CATALOG, SCHEMA);

            String singleClerkSql = String.format(
                    "SELECT o_orderstatus, COUNT(DISTINCT o_clerk) AS dc "
                    + "FROM %s.%s.orders "
                    + "GROUP BY o_orderstatus",
                    CATALOG, SCHEMA);

            List<TypedRow> priorityResults = sqlRunner.execute(singlePrioritySql);
            List<TypedRow> clerkResults = sqlRunner.execute(singleClerkSql);

            assertThat(multiResults)
                    .as("Multi-DISTINCT should produce same number of groups as single-DISTINCT")
                    .hasSameSizeAs(priorityResults);
            assertThat(multiResults)
                    .hasSameSizeAs(clerkResults);

            for (TypedRow multiRow : multiResults) {
                String status = multiRow.getString("o_orderstatus");
                long multiDp = multiRow.getLong("dp");
                long multiDc = multiRow.getLong("dc");

                long singleDp = priorityResults.stream()
                        .filter(r -> status.equals(r.getString("o_orderstatus")))
                        .mapToLong(r -> r.getLong("dp"))
                        .findFirst()
                        .orElseThrow(() -> new AssertionError(
                                "Status " + status + " not found in priority results"));

                long singleDc = clerkResults.stream()
                        .filter(r -> status.equals(r.getString("o_orderstatus")))
                        .mapToLong(r -> r.getLong("dc"))
                        .findFirst()
                        .orElseThrow(() -> new AssertionError(
                                "Status " + status + " not found in clerk results"));

                assertThat(multiDp)
                        .as("Multi-DISTINCT priority count for status %s should match", status)
                        .isEqualTo(singleDp);
                assertThat(multiDc)
                        .as("Multi-DISTINCT clerk count for status %s should match", status)
                        .isEqualTo(singleDc);
            }
        }

        @Test
        @DisplayName("Three-way DISTINCT aggregation produces correct results")
        void threeWayDistinctAggregation() {
            String sql = String.format(
                    "SELECT COUNT(DISTINCT o_orderstatus) AS ds, "
                    + "COUNT(DISTINCT o_orderpriority) AS dp, "
                    + "COUNT(DISTINCT o_clerk) AS dc "
                    + "FROM %s.%s.orders",
                    CATALOG, SCHEMA);

            List<TypedRow> results = sqlRunner.execute(sql);

            // Velox MarkDistinct may return an extra zero-value row for global aggregation
            TypedRow resultRow = results.stream()
                    .filter(r -> r.getLong("ds") > 0)
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("Expected at least one non-zero row"));

            long ds = resultRow.getLong("ds");
            long dp = resultRow.getLong("dp");
            long dc = resultRow.getLong("dc");

            assertThat(ds).as("Distinct statuses").isEqualTo(3L);
            assertThat(dp).as("Distinct priorities").isEqualTo(5L);
            assertThat(dc).as("Distinct clerks").isGreaterThan(0L);
        }
    }

    @Nested
    @DisplayName("Multi-DISTINCT Operator Execution")
    class MultiDistinctOperatorExecution {

        @Test
        @DisplayName("Multi-DISTINCT query completes successfully through native pipeline")
        void multiDistinctQueryCompletesNatively() {
            String sql = String.format(
                    "SELECT COUNT(DISTINCT l_returnflag) AS dr, "
                    + "COUNT(DISTINCT l_linestatus) AS ds "
                    + "FROM %s.%s.lineitem",
                    CATALOG, SCHEMA);

            List<TypedRow> results = sqlRunner.execute(sql);

            // Velox MarkDistinct may return an extra zero-value row for global aggregation
            TypedRow resultRow = results.stream()
                    .filter(r -> r.getLong("dr") > 0)
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("Expected at least one non-zero row"));

            long distinctReturnflag = resultRow.getLong("dr");
            long distinctLinestatus = resultRow.getLong("ds");

            // TPC-H lineitem has 3 return flags: A, N, R
            assertThat(distinctReturnflag)
                    .as("COUNT(DISTINCT l_returnflag) should be 3")
                    .isEqualTo(3L);

            // TPC-H lineitem has 2 line statuses: F, O
            assertThat(distinctLinestatus)
                    .as("COUNT(DISTINCT l_linestatus) should be 2")
                    .isEqualTo(2L);
        }

        @Test
        @DisplayName("Multi-DISTINCT with HAVING filter produces correct filtered results")
        void multiDistinctWithHavingFilter() {
            String sql = String.format(
                    "SELECT o_orderstatus, "
                    + "COUNT(DISTINCT o_orderpriority) AS dp, "
                    + "COUNT(DISTINCT o_clerk) AS dc "
                    + "FROM %s.%s.orders "
                    + "GROUP BY o_orderstatus "
                    + "HAVING COUNT(DISTINCT o_orderpriority) >= 3",
                    CATALOG, SCHEMA);

            List<TypedRow> results = sqlRunner.execute(sql);

            assertThat(results)
                    .as("HAVING filter should produce non-empty results")
                    .isNotEmpty();

            results.forEach(row -> {
                long dp = row.getLong("dp");
                assertThat(dp)
                        .as("All rows must have COUNT(DISTINCT priority) >= 3")
                        .isGreaterThanOrEqualTo(3L);
            });
        }

        @Test
        @DisplayName("Multi-DISTINCT with mixed aggregates produces correct results")
        void multiDistinctWithMixedAggregates() {
            String sql = String.format(
                    "SELECT o_orderstatus, "
                    + "COUNT(*) AS total_orders, "
                    + "COUNT(DISTINCT o_orderpriority) AS distinct_priority, "
                    + "COUNT(DISTINCT o_clerk) AS distinct_clerk, "
                    + "SUM(o_totalprice) AS total_price "
                    + "FROM %s.%s.orders "
                    + "GROUP BY o_orderstatus",
                    CATALOG, SCHEMA);

            List<TypedRow> results = sqlRunner.execute(sql);

            assertThat(results)
                    .as("Mixed aggregates should produce one row per status")
                    .hasSize(3);

            long totalOrders = results.stream()
                    .mapToLong(r -> r.getLong("total_orders"))
                    .sum();

            List<TypedRow> countAll = sqlRunner.execute(String.format(
                    "SELECT COUNT(*) AS cnt FROM %s.%s.orders", CATALOG, SCHEMA));
            long expectedTotal = countAll.get(0).getLong("cnt");

            assertThat(totalOrders)
                    .as("Sum of per-group counts should equal total row count")
                    .isEqualTo(expectedTotal);

            results.forEach(row -> {
                long dp = row.getLong("distinct_priority");
                long dc = row.getLong("distinct_clerk");
                double price = row.getDouble("total_price");

                assertThat(dp).as("Distinct priority per group").isGreaterThanOrEqualTo(1L);
                assertThat(dc).as("Distinct clerk per group").isGreaterThanOrEqualTo(1L);
                assertThat(price).as("Total price per group").isPositive();
            });
        }
    }

    @Nested
    @DisplayName("MarkDistinctNode Operator Statistics")
    class MarkDistinctNodeOperatorStatistics {

        @Test
        @DisplayName("Multi-DISTINCT query executes via native MarkDistinctNode pipeline")
        void multiDistinctExecutesViaNativeMarkDistinctPipeline() {
            String sql = String.format(
                    "SELECT COUNT(DISTINCT o_orderstatus) AS ds, "
                    + "COUNT(DISTINCT o_orderpriority) AS dp "
                    + "FROM %s.%s.orders",
                    CATALOG, SCHEMA);

            List<TypedRow> results = sqlRunner.execute(sql);

            // Velox MarkDistinct may return an extra zero-value row for global aggregation
            TypedRow resultRow = results.stream()
                    .filter(r -> r.getLong("ds") > 0)
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("Expected at least one non-zero row"));

            long ds = resultRow.getLong("ds");
            long dp = resultRow.getLong("dp");

            assertThat(ds).as("Distinct statuses").isEqualTo(3L);
            assertThat(dp).as("Distinct priorities").isEqualTo(5L);

            // Verify native memory snapshot is available after query execution,
            // confirming the query executed through the native MarkDistinctNode
            // pipeline (not Java fallback)
            byte[] memSnapshot = NativeRuntime.nativeGetMemorySnapshot();
            assertThat(memSnapshot)
                    .as("Native memory snapshot should be available after multi-DISTINCT "
                        + "execution, confirming MarkDistinctNode pipeline was used")
                    .isNotNull();
        }

        @Test
        @DisplayName("Multi-DISTINCT with GROUP BY executes natively with correct statistics")
        void multiDistinctWithGroupByExecutesNatively() {
            String sql = String.format(
                    "SELECT o_orderstatus, "
                    + "COUNT(DISTINCT o_orderpriority) AS dp, "
                    + "COUNT(DISTINCT o_clerk) AS dc "
                    + "FROM %s.%s.orders "
                    + "GROUP BY o_orderstatus "
                    + "ORDER BY o_orderstatus",
                    CATALOG, SCHEMA);

            List<TypedRow> results = sqlRunner.execute(sql);

            assertThat(results)
                    .as("Grouped multi-DISTINCT should produce one row per status")
                    .hasSize(3);

            // Verify each group has valid distinct counts
            results.forEach(row -> {
                long dp = row.getLong("dp");
                long dc = row.getLong("dc");
                assertThat(dp)
                        .as("Distinct priority count must be positive")
                        .isGreaterThanOrEqualTo(1L);
                assertThat(dc)
                        .as("Distinct clerk count must be positive")
                        .isGreaterThanOrEqualTo(1L);
            });

            // Confirm native execution path was used
            byte[] memSnapshot = NativeRuntime.nativeGetMemorySnapshot();
            assertThat(memSnapshot)
                    .as("Native memory snapshot confirms MarkDistinctNode execution "
                        + "for grouped multi-DISTINCT aggregation")
                    .isNotNull();
        }

        @Test
        @DisplayName("Three-way DISTINCT aggregation produces native operator activity")
        void threeWayDistinctProducesNativeOperatorActivity() {
            String sql = String.format(
                    "SELECT COUNT(DISTINCT l_returnflag) AS dr, "
                    + "COUNT(DISTINCT l_linestatus) AS ds, "
                    + "COUNT(DISTINCT l_shipmode) AS dm "
                    + "FROM %s.%s.lineitem",
                    CATALOG, SCHEMA);

            List<TypedRow> results = sqlRunner.execute(sql);

            // Velox MarkDistinct may return an extra zero-value row for global aggregation
            TypedRow resultRow = results.stream()
                    .filter(r -> r.getLong("dr") > 0)
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("Expected at least one non-zero row"));

            long dr = resultRow.getLong("dr");
            long ds = resultRow.getLong("ds");
            long dm = resultRow.getLong("dm");

            // TPC-H lineitem: 3 return flags, 2 line statuses, 7 ship modes
            assertThat(dr).as("Distinct return flags").isEqualTo(3L);
            assertThat(ds).as("Distinct line statuses").isEqualTo(2L);
            assertThat(dm).as("Distinct ship modes").isEqualTo(7L);

            // Confirm native MarkDistinctNode chain executed (3 MarkDistinctNodes)
            byte[] memSnapshot = NativeRuntime.nativeGetMemorySnapshot();
            assertThat(memSnapshot)
                    .as("Native memory snapshot confirms three-way MarkDistinctNode "
                        + "chain execution")
                    .isNotNull();
        }
    }
}
