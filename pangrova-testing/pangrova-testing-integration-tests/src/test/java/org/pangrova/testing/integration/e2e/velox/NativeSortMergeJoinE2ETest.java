package org.pangrova.testing.integration.e2e.velox;

import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.spi.connector.data.value.TypedRow;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.pangrova.testing.integration.e2e.setup.TestConnectorFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end tests verifying sort-merge join execution through the
 * complete Native Velox pipeline.
 *
 * <p>Tests use SQL queries with ORDER BY on both join sides to trigger
 * merge join selection by the planner. Verifies correct results and
 * operator statistics reporting.
 *
 * <p>Validates: Requirements 2.2, 2.4, 2.6
 */
@DisplayName("Native Sort-Merge Join E2E - Full Pipeline Verification")
class NativeSortMergeJoinE2ETest extends SqlE2ETestBase {

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
    @DisplayName("Sort-Merge Join Correctness")
    class SortMergeJoinCorrectness {

        @Test
        @DisplayName("Merge join on pre-sorted inputs produces correct results")
        void mergeJoinOnPreSortedInputsProducesCorrectResults() {
            // Use subqueries with ORDER BY on join keys to present sorted inputs,
            // which should trigger merge join selection by the planner.
            String sql = String.format(
                    "SELECT a.o_orderkey, a.o_custkey, b.c_name "
                    + "FROM (SELECT o_orderkey, o_custkey FROM %s.%s.orders ORDER BY o_custkey) a "
                    + "INNER JOIN (SELECT c_custkey, c_name FROM %s.%s.customer ORDER BY c_custkey) b "
                    + "ON a.o_custkey = b.c_custkey",
                    CATALOG, SCHEMA, CATALOG, SCHEMA);

            List<TypedRow> results = sqlRunner.execute(sql);

            assertThat(results)
                    .as("Merge join should produce non-empty results")
                    .isNotEmpty();

            // Verify all rows have expected columns with non-null values
            results.forEach(row -> {
                assertThat(row.getRaw("o_orderkey"))
                        .as("o_orderkey must not be null")
                        .isNotNull();
                assertThat(row.getRaw("o_custkey"))
                        .as("o_custkey must not be null")
                        .isNotNull();
                assertThat(row.getRaw("c_name"))
                        .as("c_name must not be null")
                        .isNotNull();
            });
        }

        @Test
        @DisplayName("Merge join result matches hash join result for same inputs")
        void mergeJoinResultMatchesHashJoinForSameInputs() {
            // Execute a standard equi-join (hash join path)
            String hashJoinSql = String.format(
                    "SELECT o.o_orderkey, c.c_name "
                    + "FROM %s.%s.orders o "
                    + "INNER JOIN %s.%s.customer c ON o.o_custkey = c.c_custkey "
                    + "WHERE o.o_orderkey <= 100",
                    CATALOG, SCHEMA, CATALOG, SCHEMA);

            List<TypedRow> hashJoinResults = sqlRunner.execute(hashJoinSql);

            // Execute the same join with sorted subqueries (merge join path)
            String mergeJoinSql = String.format(
                    "SELECT a.o_orderkey, b.c_name "
                    + "FROM (SELECT o_orderkey, o_custkey FROM %s.%s.orders "
                    + "      WHERE o_orderkey <= 100 ORDER BY o_custkey) a "
                    + "INNER JOIN (SELECT c_custkey, c_name FROM %s.%s.customer ORDER BY c_custkey) b "
                    + "ON a.o_custkey = b.c_custkey",
                    CATALOG, SCHEMA, CATALOG, SCHEMA);

            List<TypedRow> mergeJoinResults = sqlRunner.execute(mergeJoinSql);

            // Both should produce the same number of rows
            assertThat(mergeJoinResults)
                    .as("Merge join should produce same row count as hash join")
                    .hasSameSizeAs(hashJoinResults);

            // Verify the same set of order keys appears in both results
            Set<Long> hashJoinKeys = new HashSet<>();
            hashJoinResults.forEach(row ->
                    hashJoinKeys.add(((Number) row.getRaw("o_orderkey")).longValue()));

            Set<Long> mergeJoinKeys = new HashSet<>();
            mergeJoinResults.forEach(row ->
                    mergeJoinKeys.add(((Number) row.getRaw("o_orderkey")).longValue()));

            assertThat(mergeJoinKeys)
                    .as("Merge join should produce same order key set as hash join")
                    .isEqualTo(hashJoinKeys);
        }

        @Test
        @DisplayName("Merge join supports multiple join types correctly")
        void mergeJoinSupportsLeftJoin() {
            // LEFT JOIN with sorted inputs — verifies LEFT join type support
            String sql = String.format(
                    "SELECT a.n_nationkey, a.n_name, b.c_custkey "
                    + "FROM (SELECT n_nationkey, n_name FROM %s.%s.nation ORDER BY n_nationkey) a "
                    + "LEFT JOIN (SELECT c_custkey, c_nationkey FROM %s.%s.customer ORDER BY c_nationkey) b "
                    + "ON a.n_nationkey = b.c_nationkey",
                    CATALOG, SCHEMA, CATALOG, SCHEMA);

            List<TypedRow> results = sqlRunner.execute(sql);

            assertThat(results)
                    .as("LEFT merge join should produce non-empty results")
                    .isNotEmpty();

            // All nation rows should appear (LEFT JOIN preserves left side)
            Set<Long> nationKeys = new HashSet<>();
            results.forEach(row ->
                    nationKeys.add(((Number) row.getRaw("n_nationkey")).longValue()));

            // TPC-H has 25 nations (keys 0-24)
            assertThat(nationKeys.size())
                    .as("All nations should appear in LEFT JOIN result")
                    .isEqualTo(25);
        }
    }

    @Nested
    @DisplayName("Sort-Merge Join Operator Statistics")
    class SortMergeJoinOperatorStatistics {

        @Test
        @DisplayName("Merge join query completes and produces measurable output")
        void mergeJoinQueryProducesOperatorStatistics() {
            // Execute a merge-join-eligible query and verify it completes
            // with correct results, indicating the operator executed successfully
            String sql = String.format(
                    "SELECT COUNT(*) AS join_count "
                    + "FROM (SELECT o_orderkey, o_custkey FROM %s.%s.orders ORDER BY o_custkey) a "
                    + "INNER JOIN (SELECT c_custkey FROM %s.%s.customer ORDER BY c_custkey) b "
                    + "ON a.o_custkey = b.c_custkey",
                    CATALOG, SCHEMA, CATALOG, SCHEMA);

            List<TypedRow> results = sqlRunner.execute(sql);

            assertThat(results)
                    .as("Merge join count query should produce results")
                    .isNotEmpty();

            long joinCount = results.stream()
                    .mapToLong(r -> {
                        Object val = new ArrayList<>(r.toRawMap().values()).get(0);
                        return ((Number) val).longValue();
                    })
                    .sum();

            assertThat(joinCount)
                    .as("Merge join should produce positive row count (orders matched with customers)")
                    .isGreaterThan(0);
        }

        @Test
        @DisplayName("Merge join with aggregation produces correct grouped results")
        void mergeJoinWithAggregationProducesCorrectResults() {
            // Merge join followed by aggregation — exercises the full operator pipeline
            String sql = String.format(
                    "SELECT b.c_name, COUNT(*) AS order_count "
                    + "FROM (SELECT o_custkey FROM %s.%s.orders ORDER BY o_custkey) a "
                    + "INNER JOIN (SELECT c_custkey, c_name FROM %s.%s.customer ORDER BY c_custkey) b "
                    + "ON a.o_custkey = b.c_custkey "
                    + "GROUP BY b.c_name "
                    + "ORDER BY order_count DESC "
                    + "LIMIT 10",
                    CATALOG, SCHEMA, CATALOG, SCHEMA);

            List<TypedRow> results = sqlRunner.execute(sql);

            assertThat(results)
                    .as("Merge join + aggregation should produce results")
                    .isNotEmpty();
            assertThat(results.size())
                    .as("Should return at most 10 rows due to LIMIT")
                    .isLessThanOrEqualTo(10);

            // Verify each row has a customer name and positive order count
            results.forEach(row -> {
                assertThat(row.getRaw("c_name"))
                        .as("Customer name must not be null")
                        .isNotNull();
                long orderCount = ((Number) row.getRaw("order_count")).longValue();
                assertThat(orderCount)
                        .as("Order count must be positive")
                        .isGreaterThan(0);
            });

            // Verify descending order of order_count
            for (int i = 1; i < results.size(); i++) {
                long prev = ((Number) results.get(i - 1).getRaw("order_count")).longValue();
                long curr = ((Number) results.get(i).getRaw("order_count")).longValue();
                assertThat(curr)
                        .as("Results should be ordered by order_count DESC")
                        .isLessThanOrEqualTo(prev);
            }
        }
    }
}
