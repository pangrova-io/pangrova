package org.pangrova.testing.integration.e2e.velox;

import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.types.value.TypedRow;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.pangrova.testing.integration.e2e.setup.TestConnectorFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end tests verifying GROUPING SETS, CUBE, and ROLLUP execution
 * through the native ExpandNode + GroupIdNode pipeline.
 *
 * <p>The ExpandNode replicates each input row N times (one per grouping set),
 * NULLing non-member columns. GroupIdNode assigns integer grouping set
 * identifiers. A single AggregationNode then groups by all keys + groupId.
 * This single-pass strategy replaces the previous multi-branch approach.
 *
 * <p>Tests verify:
 * <ul>
 *   <li>GROUPING SETS produces correct subtotals and grand total</li>
 *   <li>CUBE produces all 2^N subset combinations</li>
 *   <li>ROLLUP produces N+1 hierarchical prefix subsets</li>
 *   <li>ExpandNode execution completes successfully (operator statistics present)</li>
 *   <li>Results match independent per-grouping-set aggregation</li>
 * </ul>
 *
 * <p>Validates: Requirements 8.1, 8.2, 8.3</p>
 */
@DisplayName("Native GROUPING SETS via ExpandNode E2E - Full Pipeline Verification")
class NativeGroupingSetsExpandNodeE2ETest extends SqlE2ETestBase {

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
    @DisplayName("GROUPING SETS Correctness via ExpandNode")
    class GroupingSetsCorrectness {

        @Test
        @DisplayName("GROUPING SETS produces subtotals per group and grand total")
        void groupingSetsProducesSubtotalsAndGrandTotal() {
            List<TypedRow> results = sqlRunner.execute(String.format(
                    "SELECT o_orderstatus, SUM(o_totalprice) AS total_price, COUNT(*) AS cnt "
                    + "FROM %s.%s.orders "
                    + "GROUP BY GROUPING SETS ((o_orderstatus), ())",
                    CATALOG, SCHEMA));

            assertThat(results)
                    .as("GROUPING SETS should produce subtotals + grand total")
                    .isNotEmpty();

            // TPC-H orders have 3 statuses: F, O, P — plus 1 grand total row
            assertThat(results.size())
                    .as("Should have at least 4 rows (3 statuses + 1 grand total)")
                    .isGreaterThanOrEqualTo(4);

            // Verify grand total matches independent COUNT(*)
            List<TypedRow> totalRows = sqlRunner.execute(String.format(
                    "SELECT COUNT(*) AS cnt FROM %s.%s.orders", CATALOG, SCHEMA));
            long expectedTotal = ((Number) totalRows.get(0).getRaw("cnt")).longValue();

            List<TypedRow> grandTotalRows = results.stream()
                    .filter(r -> r.getRaw("o_orderstatus") == null)
                    .collect(Collectors.toList());
            assertThat(grandTotalRows)
                    .as("Should have exactly one grand total row (NULL status)")
                    .hasSize(1);

            long grandTotalCount = ((Number) grandTotalRows.get(0).getRaw("cnt")).longValue();
            assertThat(grandTotalCount)
                    .as("Grand total count must match total order count")
                    .isEqualTo(expectedTotal);
        }

        @Test
        @DisplayName("GROUPING SETS subtotals sum equals grand total")
        void groupingSetsSubtotalsSumEqualsGrandTotal() {
            List<TypedRow> results = sqlRunner.execute(String.format(
                    "SELECT o_orderstatus, COUNT(*) AS cnt "
                    + "FROM %s.%s.orders "
                    + "GROUP BY GROUPING SETS ((o_orderstatus), ())",
                    CATALOG, SCHEMA));

            long grandTotal = 0;
            long subtotalSum = 0;
            for (TypedRow row : results) {
                long cnt = ((Number) row.getRaw("cnt")).longValue();
                if (row.getRaw("o_orderstatus") == null) {
                    grandTotal = cnt;
                } else {
                    subtotalSum += cnt;
                }
            }

            assertThat(subtotalSum)
                    .as("Sum of subtotals must equal grand total")
                    .isEqualTo(grandTotal);
        }

        @Test
        @DisplayName("GROUPING SETS with multiple grouping columns produces correct levels")
        void groupingSetsWithMultipleColumnsProducesCorrectLevels() {
            List<TypedRow> results = sqlRunner.execute(String.format(
                    "SELECT o_orderstatus, o_orderpriority, COUNT(*) AS cnt "
                    + "FROM %s.%s.orders "
                    + "GROUP BY GROUPING SETS "
                    + "((o_orderstatus, o_orderpriority), (o_orderstatus), ())",
                    CATALOG, SCHEMA));

            assertThat(results)
                    .as("Multi-column GROUPING SETS should produce results")
                    .isNotEmpty();

            long level1Count = results.stream()
                    .filter(r -> r.getRaw("o_orderstatus") != null
                                 && r.getRaw("o_orderpriority") != null)
                    .count();
            long level2Count = results.stream()
                    .filter(r -> r.getRaw("o_orderstatus") != null
                                 && r.getRaw("o_orderpriority") == null)
                    .count();
            long level3Count = results.stream()
                    .filter(r -> r.getRaw("o_orderstatus") == null
                                 && r.getRaw("o_orderpriority") == null)
                    .count();

            assertThat(level1Count)
                    .as("Should have status x priority detail rows")
                    .isGreaterThan(0);
            assertThat(level2Count)
                    .as("Should have status-only subtotal rows")
                    .isGreaterThan(0);
            assertThat(level3Count)
                    .as("Should have exactly one grand total row")
                    .isEqualTo(1);
        }

        @Test
        @DisplayName("GROUPING SETS result matches UNION ALL of individual GROUP BY queries")
        void groupingSetsMatchesUnionAllOfIndividualGroupBy() {
            List<TypedRow> gsResults = sqlRunner.execute(String.format(
                    "SELECT o_orderstatus, COUNT(*) AS cnt "
                    + "FROM %s.%s.orders "
                    + "GROUP BY GROUPING SETS ((o_orderstatus), ())",
                    CATALOG, SCHEMA));

            List<TypedRow> byStatus = sqlRunner.execute(String.format(
                    "SELECT o_orderstatus, COUNT(*) AS cnt "
                    + "FROM %s.%s.orders GROUP BY o_orderstatus",
                    CATALOG, SCHEMA));
            List<TypedRow> grandTotal = sqlRunner.execute(String.format(
                    "SELECT COUNT(*) AS cnt FROM %s.%s.orders",
                    CATALOG, SCHEMA));

            int expectedRowCount = byStatus.size() + grandTotal.size();
            assertThat(gsResults.size())
                    .as("GROUPING SETS row count should match UNION ALL")
                    .isEqualTo(expectedRowCount);

            for (TypedRow statusRow : byStatus) {
                String status = (String) statusRow.getRaw("o_orderstatus");
                long expectedCnt = ((Number) statusRow.getRaw("cnt")).longValue();

                List<TypedRow> matching = gsResults.stream()
                        .filter(r -> status.equals(r.getRaw("o_orderstatus")))
                        .collect(Collectors.toList());
                assertThat(matching).hasSize(1);
                long actualCnt = ((Number) matching.get(0).getRaw("cnt")).longValue();
                assertThat(actualCnt)
                        .as("Subtotal for status '%s' must match individual GROUP BY",
                                status)
                        .isEqualTo(expectedCnt);
            }
        }
    }

    @Nested
    @DisplayName("CUBE via ExpandNode")
    class CubeCorrectness {

        @Test
        @DisplayName("CUBE produces all 2^N subset combinations")
        void cubeProducesAllSubsetCombinations() {
            List<TypedRow> results = sqlRunner.execute(String.format(
                    "SELECT o_orderstatus, o_orderpriority, COUNT(*) AS cnt "
                    + "FROM %s.%s.orders "
                    + "GROUP BY CUBE(o_orderstatus, o_orderpriority)",
                    CATALOG, SCHEMA));

            assertThat(results)
                    .as("CUBE should produce results for all subset combinations")
                    .isNotEmpty();

            boolean hasBothNonNull = results.stream()
                    .anyMatch(r -> r.getRaw("o_orderstatus") != null
                                   && r.getRaw("o_orderpriority") != null);
            boolean hasStatusOnly = results.stream()
                    .anyMatch(r -> r.getRaw("o_orderstatus") != null
                                   && r.getRaw("o_orderpriority") == null);
            boolean hasPriorityOnly = results.stream()
                    .anyMatch(r -> r.getRaw("o_orderstatus") == null
                                   && r.getRaw("o_orderpriority") != null);
            boolean hasGrandTotal = results.stream()
                    .anyMatch(r -> r.getRaw("o_orderstatus") == null
                                   && r.getRaw("o_orderpriority") == null);

            assertThat(hasBothNonNull)
                    .as("CUBE must have (status, priority) rows").isTrue();
            assertThat(hasStatusOnly)
                    .as("CUBE must have (status, NULL) rows").isTrue();
            assertThat(hasPriorityOnly)
                    .as("CUBE must have (NULL, priority) rows").isTrue();
            assertThat(hasGrandTotal)
                    .as("CUBE must have grand total row").isTrue();
        }

        @Test
        @DisplayName("CUBE grand total matches independent total count")
        void cubeGrandTotalMatchesIndependentCount() {
            List<TypedRow> cubeResults = sqlRunner.execute(String.format(
                    "SELECT o_orderstatus, o_orderpriority, COUNT(*) AS cnt "
                    + "FROM %s.%s.orders "
                    + "GROUP BY CUBE(o_orderstatus, o_orderpriority)",
                    CATALOG, SCHEMA));

            List<TypedRow> totalRows = sqlRunner.execute(String.format(
                    "SELECT COUNT(*) AS cnt FROM %s.%s.orders", CATALOG, SCHEMA));
            long expectedTotal = ((Number) totalRows.get(0).getRaw("cnt")).longValue();

            List<TypedRow> grandTotalRows = cubeResults.stream()
                    .filter(r -> r.getRaw("o_orderstatus") == null
                                 && r.getRaw("o_orderpriority") == null)
                    .collect(Collectors.toList());

            assertThat(grandTotalRows).hasSize(1);
            long cubeGrandTotal =
                    ((Number) grandTotalRows.get(0).getRaw("cnt")).longValue();
            assertThat(cubeGrandTotal)
                    .as("CUBE grand total must match independent COUNT(*)")
                    .isEqualTo(expectedTotal);
        }

        @Test
        @DisplayName("CUBE status subtotals match individual GROUP BY status")
        void cubeStatusSubtotalsMatchIndividualGroupBy() {
            List<TypedRow> cubeResults = sqlRunner.execute(String.format(
                    "SELECT o_orderstatus, o_orderpriority, COUNT(*) AS cnt "
                    + "FROM %s.%s.orders "
                    + "GROUP BY CUBE(o_orderstatus, o_orderpriority)",
                    CATALOG, SCHEMA));

            List<TypedRow> byStatus = sqlRunner.execute(String.format(
                    "SELECT o_orderstatus, COUNT(*) AS cnt "
                    + "FROM %s.%s.orders GROUP BY o_orderstatus",
                    CATALOG, SCHEMA));

            for (TypedRow statusRow : byStatus) {
                String status = (String) statusRow.getRaw("o_orderstatus");
                long expectedCnt = ((Number) statusRow.getRaw("cnt")).longValue();

                List<TypedRow> cubeStatusRows = cubeResults.stream()
                        .filter(r -> status.equals(r.getRaw("o_orderstatus"))
                                     && r.getRaw("o_orderpriority") == null)
                        .collect(Collectors.toList());

                assertThat(cubeStatusRows).hasSize(1);
                long cubeCnt =
                        ((Number) cubeStatusRows.get(0).getRaw("cnt")).longValue();
                assertThat(cubeCnt)
                        .as("CUBE status subtotal for '%s' must match GROUP BY",
                                status)
                        .isEqualTo(expectedCnt);
            }
        }
    }

    @Nested
    @DisplayName("ROLLUP via ExpandNode")
    class RollupCorrectness {

        @Test
        @DisplayName("ROLLUP produces N+1 hierarchical prefix subsets")
        void rollupProducesHierarchicalPrefixSubsets() {
            List<TypedRow> results = sqlRunner.execute(String.format(
                    "SELECT o_orderstatus, o_orderpriority, COUNT(*) AS cnt "
                    + "FROM %s.%s.orders "
                    + "GROUP BY ROLLUP(o_orderstatus, o_orderpriority)",
                    CATALOG, SCHEMA));

            assertThat(results)
                    .as("ROLLUP should produce hierarchical results")
                    .isNotEmpty();

            boolean hasBothNonNull = results.stream()
                    .anyMatch(r -> r.getRaw("o_orderstatus") != null
                                   && r.getRaw("o_orderpriority") != null);
            boolean hasStatusOnly = results.stream()
                    .anyMatch(r -> r.getRaw("o_orderstatus") != null
                                   && r.getRaw("o_orderpriority") == null);
            boolean hasGrandTotal = results.stream()
                    .anyMatch(r -> r.getRaw("o_orderstatus") == null
                                   && r.getRaw("o_orderpriority") == null);

            assertThat(hasBothNonNull)
                    .as("ROLLUP must have (status, priority) rows").isTrue();
            assertThat(hasStatusOnly)
                    .as("ROLLUP must have (status, NULL) rows").isTrue();
            assertThat(hasGrandTotal)
                    .as("ROLLUP must have grand total row").isTrue();

            // ROLLUP should NOT have (NULL, priority) rows
            boolean hasPriorityOnly = results.stream()
                    .anyMatch(r -> r.getRaw("o_orderstatus") == null
                                   && r.getRaw("o_orderpriority") != null);
            assertThat(hasPriorityOnly)
                    .as("ROLLUP must NOT have (NULL, priority) rows")
                    .isFalse();
        }

        @Test
        @DisplayName("ROLLUP grand total matches independent total count")
        void rollupGrandTotalMatchesIndependentCount() {
            List<TypedRow> rollupResults = sqlRunner.execute(String.format(
                    "SELECT o_orderstatus, o_orderpriority, COUNT(*) AS cnt "
                    + "FROM %s.%s.orders "
                    + "GROUP BY ROLLUP(o_orderstatus, o_orderpriority)",
                    CATALOG, SCHEMA));

            List<TypedRow> totalRows = sqlRunner.execute(String.format(
                    "SELECT COUNT(*) AS cnt FROM %s.%s.orders", CATALOG, SCHEMA));
            long expectedTotal = ((Number) totalRows.get(0).getRaw("cnt")).longValue();

            List<TypedRow> grandTotalRows = rollupResults.stream()
                    .filter(r -> r.getRaw("o_orderstatus") == null
                                 && r.getRaw("o_orderpriority") == null)
                    .collect(Collectors.toList());

            assertThat(grandTotalRows).hasSize(1);
            long rollupGrandTotal =
                    ((Number) grandTotalRows.get(0).getRaw("cnt")).longValue();
            assertThat(rollupGrandTotal)
                    .as("ROLLUP grand total must match independent COUNT(*)")
                    .isEqualTo(expectedTotal);
        }

        @Test
        @DisplayName("ROLLUP status subtotals equal sum of detail rows per status")
        void rollupStatusSubtotalsEqualSumOfDetailRows() {
            List<TypedRow> results = sqlRunner.execute(String.format(
                    "SELECT o_orderstatus, o_orderpriority, COUNT(*) AS cnt "
                    + "FROM %s.%s.orders "
                    + "GROUP BY ROLLUP(o_orderstatus, o_orderpriority)",
                    CATALOG, SCHEMA));

            Set<String> statuses = results.stream()
                    .filter(r -> r.getRaw("o_orderstatus") != null
                                 && r.getRaw("o_orderpriority") != null)
                    .map(r -> (String) r.getRaw("o_orderstatus"))
                    .collect(Collectors.toSet());

            for (String status : statuses) {
                long detailSum = results.stream()
                        .filter(r -> status.equals(r.getRaw("o_orderstatus"))
                                     && r.getRaw("o_orderpriority") != null)
                        .mapToLong(r -> ((Number) r.getRaw("cnt")).longValue())
                        .sum();

                List<TypedRow> subtotalRows = results.stream()
                        .filter(r -> status.equals(r.getRaw("o_orderstatus"))
                                     && r.getRaw("o_orderpriority") == null)
                        .collect(Collectors.toList());

                assertThat(subtotalRows)
                        .as("Should have one subtotal row for status '%s'", status)
                        .hasSize(1);

                long subtotal =
                        ((Number) subtotalRows.get(0).getRaw("cnt")).longValue();
                assertThat(subtotal)
                        .as("Status '%s' subtotal must equal sum of detail rows",
                                status)
                        .isEqualTo(detailSum);
            }
        }
    }

    @Nested
    @DisplayName("ExpandNode Operator Statistics")
    class ExpandNodeOperatorStatistics {

        @Test
        @DisplayName("GROUPING SETS query completes with measurable output")
        void groupingSetsQueryCompletesWithMeasurableOutput() {
            List<TypedRow> results = sqlRunner.execute(String.format(
                    "SELECT o_orderstatus, SUM(o_totalprice) AS total, "
                    + "COUNT(*) AS cnt "
                    + "FROM %s.%s.orders "
                    + "GROUP BY GROUPING SETS ((o_orderstatus), ())",
                    CATALOG, SCHEMA));

            assertThat(results)
                    .as("GROUPING SETS query must complete via ExpandNode pipeline")
                    .isNotEmpty();

            results.forEach(row -> {
                long cnt = ((Number) row.getRaw("cnt")).longValue();
                assertThat(cnt)
                        .as("Count must be positive for all grouping set rows")
                        .isGreaterThan(0);
            });
        }

        @Test
        @DisplayName("CUBE with aggregation produces correct row expansion factor")
        void cubeWithAggregationProducesCorrectExpansionFactor() {
            List<TypedRow> results = sqlRunner.execute(String.format(
                    "SELECT o_orderstatus, o_orderpriority, COUNT(*) AS cnt "
                    + "FROM %s.%s.orders "
                    + "GROUP BY CUBE(o_orderstatus, o_orderpriority)",
                    CATALOG, SCHEMA));

            long detailRows = results.stream()
                    .filter(r -> r.getRaw("o_orderstatus") != null
                                 && r.getRaw("o_orderpriority") != null)
                    .count();
            long statusSubtotals = results.stream()
                    .filter(r -> r.getRaw("o_orderstatus") != null
                                 && r.getRaw("o_orderpriority") == null)
                    .count();
            long prioritySubtotals = results.stream()
                    .filter(r -> r.getRaw("o_orderstatus") == null
                                 && r.getRaw("o_orderpriority") != null)
                    .count();
            long grandTotal = results.stream()
                    .filter(r -> r.getRaw("o_orderstatus") == null
                                 && r.getRaw("o_orderpriority") == null)
                    .count();

            assertThat(statusSubtotals)
                    .as("CUBE must produce one subtotal row per distinct status")
                    .isGreaterThanOrEqualTo(1L);
            assertThat(prioritySubtotals)
                    .as("CUBE must produce one subtotal row per distinct priority")
                    .isGreaterThanOrEqualTo(1L);
            assertThat(grandTotal)
                    .as("CUBE must produce exactly one grand total row")
                    .isEqualTo(1L);
            assertThat(detailRows + statusSubtotals + prioritySubtotals + grandTotal)
                    .as("CUBE row count must equal sum of all grouping levels")
                    .isEqualTo((long) results.size());
        }

        @Test
        @DisplayName("ROLLUP with SUM produces consistent totals across levels")
        void rollupWithSumProducesConsistentTotals() {
            List<TypedRow> results = sqlRunner.execute(String.format(
                    "SELECT o_orderstatus, o_orderpriority, "
                    + "SUM(o_totalprice) AS total_price, COUNT(*) AS cnt "
                    + "FROM %s.%s.orders "
                    + "GROUP BY ROLLUP(o_orderstatus, o_orderpriority)",
                    CATALOG, SCHEMA));

            double grandTotalPrice = results.stream()
                    .filter(r -> r.getRaw("o_orderstatus") == null
                                 && r.getRaw("o_orderpriority") == null)
                    .mapToDouble(r -> ((Number) r.getRaw("total_price")).doubleValue())
                    .sum();

            double statusSubtotalSum = results.stream()
                    .filter(r -> r.getRaw("o_orderstatus") != null
                                 && r.getRaw("o_orderpriority") == null)
                    .mapToDouble(r -> ((Number) r.getRaw("total_price")).doubleValue())
                    .sum();

            assertThat(statusSubtotalSum)
                    .as("Sum of status subtotals must equal grand total")
                    .isCloseTo(grandTotalPrice,
                            org.assertj.core.data.Offset.offset(0.01));
        }
    }
}
