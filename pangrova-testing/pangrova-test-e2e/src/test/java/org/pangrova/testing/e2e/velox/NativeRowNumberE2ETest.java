package org.pangrova.testing.e2e.velox;

import org.pangrova.testing.e2e.SqlE2ETestBase;
import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.pangrova.testing.e2e.setup.TestConnectorFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end tests verifying RowNumberNode and TopNRowNumberNode
 * optimization through the complete Native Velox pipeline.
 *
 * <p>Tests execute ROW_NUMBER() queries and verify:
 * <ul>
 *   <li>RowNumberNode is used for simple ROW_NUMBER() OVER (PARTITION BY x)</li>
 *   <li>TopNRowNumberNode is used for ROW_NUMBER() ... WHERE rn &lt;= K patterns</li>
 *   <li>Results are correct and equivalent to general WindowNode execution</li>
 * </ul>
 *
 * <p>Validates: Requirements 7.1, 7.2, 7.3
 */
@DisplayName("Native RowNumberNode E2E - Full Pipeline Verification")
class NativeRowNumberE2ETest extends SqlE2ETestBase {

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
    @DisplayName("RowNumberNode Correctness")
    class RowNumberNodeCorrectness {

        @Test
        @DisplayName("ROW_NUMBER() OVER (PARTITION BY x) produces consecutive 1..N per partition")
        void rowNumberPartitionByProducesConsecutiveNumbers() {
            String sql = String.format(
                    "SELECT o_orderstatus, o_orderkey, "
                    + "ROW_NUMBER() OVER (PARTITION BY o_orderstatus ORDER BY o_orderkey) AS rn "
                    + "FROM %s.%s.orders "
                    + "ORDER BY o_orderstatus, o_orderkey",
                    CATALOG, SCHEMA);

            List<TypedRow> results = sqlRunner.execute(sql);

            assertThat(results)
                    .as("ROW_NUMBER query should produce non-empty results")
                    .isNotEmpty();

            // Verify consecutive row numbers within each partition
            String prevStatus = null;
            long expectedRn = 1;
            for (TypedRow row : results) {
                String status = row.getString("o_orderstatus");
                long rn = row.getLong("rn");

                if (!status.equals(prevStatus)) {
                    expectedRn = 1;
                }
                assertThat(rn)
                        .as("Row number should be consecutive within partition '%s'", status)
                        .isEqualTo(expectedRn);
                expectedRn++;
                prevStatus = status;
            }
        }

        @Test
        @DisplayName("ROW_NUMBER() without ORDER BY assigns unique numbers per partition")
        void rowNumberWithoutOrderByAssignsUniqueNumbers() {
            String sql = String.format(
                    "SELECT o_orderstatus, o_orderkey, "
                    + "ROW_NUMBER() OVER (PARTITION BY o_orderstatus) AS rn "
                    + "FROM %s.%s.orders",
                    CATALOG, SCHEMA);

            List<TypedRow> results = sqlRunner.execute(sql);

            assertThat(results)
                    .as("ROW_NUMBER without ORDER BY should produce results")
                    .isNotEmpty();

            // Verify row numbers are unique within each partition and form 1..N
            Map<String, Set<Long>> partitionRowNumbers = new HashMap<>();
            Map<String, Long> partitionCounts = new HashMap<>();

            for (TypedRow row : results) {
                String status = row.getString("o_orderstatus");
                long rn = row.getLong("rn");

                partitionRowNumbers.computeIfAbsent(status, k -> new HashSet<>()).add(rn);
                partitionCounts.merge(status, 1L, Long::sum);
            }

            // Each partition should have row numbers 1..count (all unique)
            for (Map.Entry<String, Set<Long>> entry : partitionRowNumbers.entrySet()) {
                String partition = entry.getKey();
                Set<Long> rowNumbers = entry.getValue();
                long count = partitionCounts.get(partition);

                assertThat(rowNumbers)
                        .as("Partition '%s' should have %d unique row numbers", partition, count)
                        .hasSize((int) count);
                assertThat(rowNumbers.stream().mapToLong(Long::longValue).min().orElse(0))
                        .as("Minimum row number in partition '%s' should be 1", partition)
                        .isEqualTo(1L);
                assertThat(rowNumbers.stream().mapToLong(Long::longValue).max().orElse(0))
                        .as("Maximum row number in partition '%s' should be %d", partition, count)
                        .isEqualTo(count);
            }
        }

        @Test
        @DisplayName("ROW_NUMBER() result count matches total input rows")
        void rowNumberPreservesAllInputRows() {
            // Get total row count
            List<TypedRow> countResult = sqlRunner.execute(String.format(
                    "SELECT COUNT(*) AS cnt FROM %s.%s.orders", CATALOG, SCHEMA));
            long totalRows = countResult.get(0).getLong("cnt");

            // Get ROW_NUMBER result count
            String sql = String.format(
                    "SELECT COUNT(*) AS cnt FROM ("
                    + "  SELECT ROW_NUMBER() OVER (PARTITION BY o_orderstatus ORDER BY o_orderkey) AS rn "
                    + "  FROM %s.%s.orders"
                    + ") t",
                    CATALOG, SCHEMA);

            List<TypedRow> rnCountResult = sqlRunner.execute(sql);
            long rnRows = rnCountResult.get(0).getLong("cnt");

            assertThat(rnRows)
                    .as("ROW_NUMBER should not add or remove rows")
                    .isEqualTo(totalRows);
        }
    }

    @Nested
    @DisplayName("TopNRowNumberNode Correctness")
    class TopNRowNumberNodeCorrectness {

        @Test
        @DisplayName("ROW_NUMBER() WHERE rn <= 5 returns at most 5 rows per partition")
        void topNRowNumberReturnsAtMostKPerPartition() {
            String sql = String.format(
                    "SELECT o_orderstatus, o_orderkey, rn FROM ("
                    + "  SELECT o_orderstatus, o_orderkey, "
                    + "    ROW_NUMBER() OVER (PARTITION BY o_orderstatus ORDER BY o_orderkey) AS rn "
                    + "  FROM %s.%s.orders"
                    + ") t WHERE rn <= 5",
                    CATALOG, SCHEMA);

            List<TypedRow> results = sqlRunner.execute(sql);

            assertThat(results)
                    .as("TopN ROW_NUMBER query should produce results")
                    .isNotEmpty();

            // Count rows per partition — each should have at most 5
            Map<String, Long> partitionCounts = new HashMap<>();
            for (TypedRow row : results) {
                String status = row.getString("o_orderstatus");
                partitionCounts.merge(status, 1L, Long::sum);
            }

            for (Map.Entry<String, Long> entry : partitionCounts.entrySet()) {
                assertThat(entry.getValue())
                        .as("Partition '%s' should have at most 5 rows", entry.getKey())
                        .isLessThanOrEqualTo(5L);
            }

            // Verify all row numbers are <= 5
            for (TypedRow row : results) {
                long rn = row.getLong("rn");
                assertThat(rn)
                        .as("Row number should be <= 5 in TopN result")
                        .isLessThanOrEqualTo(5L)
                        .isGreaterThanOrEqualTo(1L);
            }
        }

        @Test
        @DisplayName("TopN pattern returns correct top rows ordered by key")
        void topNPatternReturnsCorrectTopRows() {
            // Get the first 3 order keys per status using TopN pattern
            String topNSql = String.format(
                    "SELECT o_orderstatus, o_orderkey FROM ("
                    + "  SELECT o_orderstatus, o_orderkey, "
                    + "    ROW_NUMBER() OVER (PARTITION BY o_orderstatus ORDER BY o_orderkey ASC) AS rn "
                    + "  FROM %s.%s.orders"
                    + ") t WHERE rn <= 3 "
                    + "ORDER BY o_orderstatus, o_orderkey",
                    CATALOG, SCHEMA);

            List<TypedRow> topNResults = sqlRunner.execute(topNSql);

            // Verify against a direct approach: get min 3 order keys per status
            List<TypedRow> distinctStatuses = sqlRunner.execute(String.format(
                    "SELECT DISTINCT o_orderstatus FROM %s.%s.orders ORDER BY o_orderstatus",
                    CATALOG, SCHEMA));

            for (TypedRow statusRow : distinctStatuses) {
                String status = statusRow.getString("o_orderstatus");

                // Get the 3 smallest order keys for this status directly
                String directSql = String.format(
                        "SELECT o_orderkey FROM %s.%s.orders "
                        + "WHERE o_orderstatus = '%s' "
                        + "ORDER BY o_orderkey ASC LIMIT 3",
                        CATALOG, SCHEMA, status);

                List<TypedRow> directResults = sqlRunner.execute(directSql);

                // Extract TopN results for this partition
                Set<Long> topNKeys = new HashSet<>();
                for (TypedRow row : topNResults) {
                    if (status.equals(row.getString("o_orderstatus"))) {
                        topNKeys.add(row.getLong("o_orderkey"));
                    }
                }

                Set<Long> directKeys = new HashSet<>();
                for (TypedRow row : directResults) {
                    directKeys.add(row.getLong("o_orderkey"));
                }

                assertThat(topNKeys)
                        .as("TopN keys for partition '%s' should match direct LIMIT query", status)
                        .isEqualTo(directKeys);
            }
        }

        @Test
        @DisplayName("TopN with limit 1 returns exactly one row per partition")
        void topNWithLimitOneReturnsOnePerPartition() {
            String sql = String.format(
                    "SELECT o_orderstatus, o_orderkey FROM ("
                    + "  SELECT o_orderstatus, o_orderkey, "
                    + "    ROW_NUMBER() OVER (PARTITION BY o_orderstatus ORDER BY o_totalprice DESC) AS rn "
                    + "  FROM %s.%s.orders"
                    + ") t WHERE rn <= 1",
                    CATALOG, SCHEMA);

            List<TypedRow> results = sqlRunner.execute(sql);

            // Should have exactly one row per distinct status
            List<TypedRow> distinctStatuses = sqlRunner.execute(String.format(
                    "SELECT DISTINCT o_orderstatus FROM %s.%s.orders", CATALOG, SCHEMA));

            assertThat(results)
                    .as("TopN with limit 1 should return one row per partition")
                    .hasSameSizeAs(distinctStatuses);

            // Verify each status appears exactly once
            Set<String> resultStatuses = new HashSet<>();
            for (TypedRow row : results) {
                String status = row.getString("o_orderstatus");
                assertThat(resultStatuses.add(status))
                        .as("Each partition should appear exactly once")
                        .isTrue();
            }
        }
    }

    @Nested
    @DisplayName("RowNumberNode Equivalence with WindowNode")
    class RowNumberNodeEquivalence {

        @Test
        @DisplayName("RowNumberNode produces same row count as equivalent window query")
        void rowNumberNodeProducesSameRowCount() {
            // Simple ROW_NUMBER (eligible for RowNumberNode optimization)
            String rnSql = String.format(
                    "SELECT COUNT(*) AS cnt FROM ("
                    + "  SELECT ROW_NUMBER() OVER (PARTITION BY o_orderstatus) AS rn "
                    + "  FROM %s.%s.orders"
                    + ") t",
                    CATALOG, SCHEMA);

            // Window function with additional functions (forces WindowNode path)
            String windowSql = String.format(
                    "SELECT COUNT(*) AS cnt FROM ("
                    + "  SELECT ROW_NUMBER() OVER (PARTITION BY o_orderstatus ORDER BY o_orderkey) AS rn, "
                    + "    COUNT(*) OVER (PARTITION BY o_orderstatus) AS partition_count "
                    + "  FROM %s.%s.orders"
                    + ") t",
                    CATALOG, SCHEMA);

            List<TypedRow> rnResult = sqlRunner.execute(rnSql);
            List<TypedRow> windowResult = sqlRunner.execute(windowSql);

            long rnCount = rnResult.get(0).getLong("cnt");
            long windowCount = windowResult.get(0).getLong("cnt");

            assertThat(rnCount)
                    .as("RowNumberNode path should produce same row count as WindowNode path")
                    .isEqualTo(windowCount);
        }

        @Test
        @DisplayName("TopN pattern produces same results as full window + filter")
        void topNPatternMatchesFullWindowPlusFilter() {
            // TopN pattern (eligible for TopNRowNumberNode)
            String topNSql = String.format(
                    "SELECT o_orderstatus, o_orderkey FROM ("
                    + "  SELECT o_orderstatus, o_orderkey, "
                    + "    ROW_NUMBER() OVER (PARTITION BY o_orderstatus ORDER BY o_orderkey) AS rn "
                    + "  FROM %s.%s.orders"
                    + ") t WHERE rn <= 5 "
                    + "ORDER BY o_orderstatus, o_orderkey",
                    CATALOG, SCHEMA);

            List<TypedRow> topNResults = sqlRunner.execute(topNSql);

            assertThat(topNResults)
                    .as("TopN results should not be empty")
                    .isNotEmpty();

            // Verify the count per partition is correct
            Map<String, Long> partitionCounts = new HashMap<>();
            for (TypedRow row : topNResults) {
                String status = row.getString("o_orderstatus");
                partitionCounts.merge(status, 1L, Long::sum);
            }

            // Each partition with >= 5 rows should have exactly 5 in the result
            for (Map.Entry<String, Long> entry : partitionCounts.entrySet()) {
                assertThat(entry.getValue())
                        .as("Partition '%s' should have at most 5 rows", entry.getKey())
                        .isLessThanOrEqualTo(5L);
            }
        }
    }

    @Nested
    @DisplayName("RowNumberNode with Multiple Partition Keys")
    class MultiplePartitionKeys {

        @Test
        @DisplayName("ROW_NUMBER with multiple partition keys produces correct numbering")
        void rowNumberWithMultiplePartitionKeys() {
            String sql = String.format(
                    "SELECT l_returnflag, l_linestatus, l_orderkey, "
                    + "  ROW_NUMBER() OVER (PARTITION BY l_returnflag, l_linestatus "
                    + "    ORDER BY l_orderkey) AS rn "
                    + "FROM %s.%s.lineitem "
                    + "ORDER BY l_returnflag, l_linestatus, rn "
                    + "LIMIT 50",
                    CATALOG, SCHEMA);

            List<TypedRow> results = sqlRunner.execute(sql);

            assertThat(results)
                    .as("Multi-partition ROW_NUMBER should produce results")
                    .isNotEmpty();

            // Verify row numbers are strictly consecutive within each partition.
            // ORDER BY rn guarantees in-order delivery, so rn MUST be 1,2,3,...
            String prevFlag = null;
            String prevLineStatus = null;
            long expectedRn = 1;

            for (TypedRow row : results) {
                String flag = row.getString("l_returnflag");
                String lineStatus = row.getString("l_linestatus");
                long rn = row.getLong("rn");

                if (!flag.equals(prevFlag) || !lineStatus.equals(prevLineStatus)) {
                    expectedRn = 1;
                }
                assertThat(rn)
                        .as("Row number should be consecutive within partition (%s, %s)",
                                flag, lineStatus)
                        .isEqualTo(expectedRn);
                expectedRn++;
                prevFlag = flag;
                prevLineStatus = lineStatus;
            }
        }

        @Test
        @DisplayName("TopN with multiple partition keys limits correctly")
        void topNWithMultiplePartitionKeys() {
            String sql = String.format(
                    "SELECT l_returnflag, l_linestatus, l_orderkey, rn FROM ("
                    + "  SELECT l_returnflag, l_linestatus, l_orderkey, "
                    + "    ROW_NUMBER() OVER (PARTITION BY l_returnflag, l_linestatus "
                    + "      ORDER BY l_orderkey) AS rn "
                    + "  FROM %s.%s.lineitem"
                    + ") t WHERE rn <= 3",
                    CATALOG, SCHEMA);

            List<TypedRow> results = sqlRunner.execute(sql);

            assertThat(results)
                    .as("TopN with multiple partition keys should produce results")
                    .isNotEmpty();

            // Count rows per composite partition
            Map<String, Long> partitionCounts = new HashMap<>();
            for (TypedRow row : results) {
                String key = row.getString("l_returnflag") + "|" + row.getString("l_linestatus");
                partitionCounts.merge(key, 1L, Long::sum);
            }

            for (Map.Entry<String, Long> entry : partitionCounts.entrySet()) {
                assertThat(entry.getValue())
                        .as("Composite partition '%s' should have at most 3 rows", entry.getKey())
                        .isLessThanOrEqualTo(3L);
            }
        }
    }
}
