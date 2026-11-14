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

/** SQL result guarantees for selective joins, duplicate build keys and empty inputs. */
@DisplayName("Selective joins through the SQL and Native execution path")
class NativeDynamicFilterJoinE2ETest extends SqlE2ETestBase {

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

    @Test
    void literalBuildPruningPreservesJoinMultiplicityAndOuterRows() {
        String customer = CATALOG + "." + SCHEMA + ".customer";
        String keys = "(VALUES (CAST(1 AS BIGINT)), (CAST(3 AS BIGINT)), (CAST(3 AS BIGINT))) AS v(k)";
        // The generated customer fixture cycles 100 rows over 50 keys.
        // Derive the expected multiset from the probe alone so a duplicate build cannot be lost.
        var probeKeys = sqlRunner.execute("SELECT c_custkey AS id FROM " + customer
                + " WHERE c_custkey <= 3 ORDER BY id").stream().map(row -> row.getLong("id")).toList();
        assertThat(probeKeys).contains(1L, 2L, 3L);
        var inner = sqlRunner.execute("SELECT c.c_custkey AS id FROM " + customer
                + " c JOIN " + keys + " ON c.c_custkey = v.k ORDER BY id");
        var expectedInner = probeKeys.stream().filter(id -> id != 2)
                .flatMap(id -> java.util.Collections.nCopies(id == 3 ? 2 : 1, id).stream()).toList();
        assertThat(inner).extracting(row -> row.getLong("id")).containsExactlyElementsOf(expectedInner);
        var outer = sqlRunner.execute("SELECT c.c_custkey AS id FROM " + customer
                + " c LEFT JOIN " + keys + " ON c.c_custkey = v.k WHERE c.c_custkey <= 3 ORDER BY id");
        var expectedOuter = probeKeys.stream()
                .flatMap(id -> java.util.Collections.nCopies(id == 3 ? 2 : 1, id).stream()).toList();
        assertThat(outer).extracting(row -> row.getLong("id")).containsExactlyElementsOf(expectedOuter);
    }

    @Nested
    @DisplayName("Selective hash joins")
    class SelectiveJoins {

        @Test
        @DisplayName("Selective build produces matching order keys")
        void hashJoinWithSelectiveBuildReturnsMatchingOrderKeys() {
            // Full probe-side scan: count all lineitem rows
            List<TypedRow> fullScanRows = sqlRunner.execute(String.format(
                    "SELECT COUNT(*) AS cnt FROM %s.%s.lineitem", CATALOG, SCHEMA));
            long totalLineitemRows = fullScanRows.get(0).getLong("cnt");

            // Find a custkey that actually exists in orders and has matching lineitem rows.
            // This makes the test robust to data generation seed changes.
            List<TypedRow> existingCust = sqlRunner.execute(String.format(
                    "SELECT o.o_custkey FROM %s.%s.orders o "
                    + "INNER JOIN %s.%s.lineitem l ON o.o_orderkey = l.l_orderkey "
                    + "LIMIT 1",
                    CATALOG, SCHEMA, CATALOG, SCHEMA));
            assertThat(existingCust).as("Need at least one order with matching lineitem").isNotEmpty();
            long targetCustKey = existingCust.get(0).getLong("o_custkey");

            // Hash join with highly selective build side:
            // Only orders from a specific customer (very few orders out of total)
            List<TypedRow> joinRows = sqlRunner.execute(String.format(
                    "SELECT l.l_orderkey, l.l_quantity "
                    + "FROM %s.%s.lineitem l "
                    + "INNER JOIN %s.%s.orders o ON l.l_orderkey = o.o_orderkey "
                    + "WHERE o.o_custkey = " + targetCustKey,
                    CATALOG, SCHEMA, CATALOG, SCHEMA));

            // The join result should be much smaller than total lineitem rows
            // because only orders belonging to the target customer match.
            assertThat(joinRows).isNotEmpty();
            assertThat((long) joinRows.size())
                    .as("Join with selective build side should return far fewer rows "
                        + "than total lineitem count (%d)",
                        totalLineitemRows)
                    .isLessThan(totalLineitemRows);

            // Verify correctness: all returned l_orderkey values should belong
            // to orders from the target customer
            List<TypedRow> custOrders = sqlRunner.execute(String.format(
                    "SELECT o_orderkey FROM %s.%s.orders WHERE o_custkey = " + targetCustKey,
                    CATALOG, SCHEMA));
            Set<Long> validOrderKeys = custOrders.stream()
                    .map(r -> r.getLong("o_orderkey"))
                    .collect(Collectors.toSet());

            joinRows.forEach(row -> {
                long orderKey = row.getLong("l_orderkey");
                assertThat(validOrderKeys)
                        .as("All join result order keys must belong to target customer")
                        .contains(orderKey);
            });
        }

        @Test
        @DisplayName("String predicate on the build side restricts joined results")
        void stringBuildPredicateRestrictsJoinedResults() {
            // Join on integer key with varchar filter on build side:
            // nation table filtered to single nation (highly selective build)
            List<TypedRow> joinRows = sqlRunner.execute(String.format(
                    "SELECT s.s_name, n.n_name "
                    + "FROM %s.%s.supplier s "
                    + "INNER JOIN %s.%s.nation n ON s.s_nationkey = n.n_nationkey "
                    + "WHERE n.n_name = 'FRANCE'",
                    CATALOG, SCHEMA, CATALOG, SCHEMA));

            // With deterministic seed=42 data, 'FRANCE' is guaranteed to exist.
            // The generator is reset per test class so data is reproducible.
            assertThat(joinRows)
                    .as("Join with FRANCE filter must return results (seed=42 guarantees FRANCE exists)")
                    .isNotEmpty();

            // All results should have nation = FRANCE
            joinRows.forEach(row ->
                    assertThat(row.getString("n_name"))
                            .as("All joined rows must have nation FRANCE")
                            .isEqualTo("FRANCE"));
        }

        @Test
        @DisplayName("Customer predicate restricts a three-table join")
        void customerPredicateRestrictsThreeTableJoin() {
            // Three-table join: customer → orders → lineitem
            List<TypedRow> joinRows = sqlRunner.execute(String.format(
                    "SELECT l.l_orderkey, l.l_extendedprice, o.o_orderdate "
                    + "FROM %s.%s.lineitem l "
                    + "INNER JOIN %s.%s.orders o ON l.l_orderkey = o.o_orderkey "
                    + "INNER JOIN %s.%s.customer c ON o.o_custkey = c.c_custkey "
                    + "WHERE c.c_mktsegment = 'BUILDING'",
                    CATALOG, SCHEMA, CATALOG, SCHEMA, CATALOG, SCHEMA));

            assertThat(joinRows).isNotEmpty();

            // Verify correctness: all order keys should belong to BUILDING customers
            List<TypedRow> buildingOrders = sqlRunner.execute(String.format(
                    "SELECT DISTINCT o.o_orderkey "
                    + "FROM %s.%s.orders o "
                    + "INNER JOIN %s.%s.customer c ON o.o_custkey = c.c_custkey "
                    + "WHERE c.c_mktsegment = 'BUILDING'",
                    CATALOG, SCHEMA, CATALOG, SCHEMA));
            Set<Long> validKeys = buildingOrders.stream()
                    .map(r -> r.getLong("o_orderkey"))
                    .collect(Collectors.toSet());

            joinRows.forEach(row -> {
                long orderKey = row.getLong("l_orderkey");
                assertThat(validKeys)
                        .as("All lineitem order keys must belong to BUILDING segment customers")
                        .contains(orderKey);
            });
        }
    }

    @Nested
    @DisplayName("Filtered and empty join inputs")
    class FilteredAndEmptyJoinInputs {

        @Test
        @DisplayName("Filtered customer join matches its aggregate")
        void filteredCustomerJoinMatchesAggregate() {
            List<TypedRow> rows = sqlRunner.execute(String.format(
                    "SELECT o.o_orderkey, o.o_totalprice, c.c_name, c.c_nationkey "
                    + "FROM %s.%s.orders o "
                    + "INNER JOIN %s.%s.customer c ON o.o_custkey = c.c_custkey "
                    + "WHERE c.c_nationkey = 5",
                    CATALOG, SCHEMA, CATALOG, SCHEMA));

            assertThat(rows).isNotEmpty();

            // Verify correctness: every joined row must carry nationkey = 5
            for (TypedRow row : rows) {
                assertThat(row.getLong("c_nationkey"))
                        .as("Joined customer nationkey for order %d",
                                row.getLong("o_orderkey"))
                        .isEqualTo(5L);
            }

            // Cross-validate: row count must match a COUNT over the same JOIN
            List<TypedRow> countRows = sqlRunner.execute(String.format(
                    "SELECT COUNT(*) AS cnt FROM %s.%s.orders o "
                    + "INNER JOIN %s.%s.customer c ON o.o_custkey = c.c_custkey "
                    + "WHERE c.c_nationkey = 5",
                    CATALOG, SCHEMA, CATALOG, SCHEMA));
            long joinCount = countRows.get(0).getLong("cnt");
            assertThat((long) rows.size())
                    .as("JOIN row count must match COUNT over same JOIN")
                    .isEqualTo(joinCount);
        }

        @Test
        @DisplayName("Hash join with empty build side returns no rows")
        void hashJoinWithEmptyBuildSideReturnsNoRows() {
            List<TypedRow> rows = sqlRunner.execute(String.format(
                    "SELECT l.l_orderkey "
                    + "FROM %s.%s.lineitem l "
                    + "INNER JOIN (SELECT o_orderkey FROM %s.%s.orders WHERE o_totalprice < 0) o "
                    + "ON l.l_orderkey = o.o_orderkey",
                    CATALOG, SCHEMA, CATALOG, SCHEMA));

            assertThat(rows)
                    .as("Hash join with empty build side (no orders with negative price) "
                        + "should return zero rows")
                    .isEmpty();
        }
    }
}
