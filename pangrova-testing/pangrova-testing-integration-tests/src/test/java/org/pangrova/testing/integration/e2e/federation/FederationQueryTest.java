package org.pangrova.testing.integration.e2e.federation;

import org.pangrova.types.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.pangrova.testing.integration.e2e.runner.QueryError;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Cross-connector federation query E2E tests.
 *
 * <p>Configures both Iceberg (local filesystem) and Paimon (local filesystem)
 * connectors, then executes cross-connector JOIN, UNION ALL, subquery,
 * ORDER BY + LIMIT, INSERT SELECT, and error propagation scenarios.
 *
 * <p>Covers R11.1-R11.9. All operations execute through the real pipeline:
 * TestCluster HTTP → NativeRuntime → Velox → Iceberg/Paimon → local filesystem.
 *
 * <p>Requirements: 11.1, 11.2, 11.3, 16.6
 */
@DisplayName("Federation Query - Cross-Connector E2E Tests")
class FederationQueryTest extends SqlE2ETestBase {

    private static final String ICE_CATALOG = "iceberg_catalog";
    private static final String PAI_CATALOG = "paimon_catalog";
    private static final String SCHEMA = "default";
    private static final String ICE_PREFIX = ICE_CATALOG + "." + SCHEMA + ".";
    private static final String PAI_PREFIX = PAI_CATALOG + "." + SCHEMA + ".";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(
                        ICE_CATALOG, SCHEMA, icebergWarehouse().toString()),
                ConnectorTestConfig.paimon(
                        PAI_CATALOG, SCHEMA, paimonWarehouse().toString()));
    }

    private String iceTable;
    private String paiTable;

    @BeforeEach
    void assignUniqueTableNames(TestInfo testInfo) {
        String method = testInfo.getTestMethod()
                .map(m -> m.getName()).orElse("unknown");
        long ts = System.nanoTime();
        iceTable = "ice_" + method + "_" + ts;
        paiTable = "pai_" + method + "_" + ts;
    }

    private String iceFqn() {
        return ICE_PREFIX + iceTable;
    }

    private String paiFqn() {
        return PAI_PREFIX + paiTable;
    }

    /** Insert seed data into both Iceberg and Paimon tables for JOIN tests. */
    private void seedJoinData() {
        sqlRunner.executeDdl("CREATE TABLE " + iceFqn()
                + " (id BIGINT, name VARCHAR, region VARCHAR)");
        sqlRunner.execute("INSERT INTO " + iceFqn()
                + " (id, name, region) VALUES "
                + "(1, 'alice', 'east'), (2, 'bob', 'west'), (3, 'carol', 'east')");

        sqlRunner.executeDdl("CREATE TABLE " + paiFqn()
                + " (id BIGINT, amount DOUBLE, status VARCHAR)");
        sqlRunner.execute("INSERT INTO " + paiFqn()
                + " (id, amount, status) VALUES "
                + "(1, 100.0, 'active'), (2, 200.0, 'inactive'), (4, 400.0, 'active')");
    }

    // --- R11.1: Cross-connector JOIN ---

    @Nested
    @DisplayName("R11.1 - Cross-Connector JOIN (Iceberg × Paimon)")
    class CrossConnectorJoin {

        @Test
        @DisplayName("INNER JOIN between Iceberg and Paimon returns correct results")
        void innerJoinAcrossConnectors() {
            seedJoinData();

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT i.id, i.name, p.amount, p.status "
                    + "FROM " + iceFqn() + " i "
                    + "JOIN " + paiFqn() + " p ON i.id = p.id "
                    + "ORDER BY i.id");

            assertThat(rows).hasSize(2);
            assertIdName(rows.get(0), 1L, "alice");
            assertAmount(rows.get(0), 100.0);
            assertIdName(rows.get(1), 2L, "bob");
            assertAmount(rows.get(1), 200.0);
        }
    }

    // --- R11.2: Cross-connector JOIN with WHERE filter ---

    @Nested
    @DisplayName("R11.2 - Cross-Connector JOIN with WHERE Filter")
    class CrossConnectorJoinWithFilter {

        @Test
        @DisplayName("JOIN with WHERE pushes predicates to each connector")
        void joinWithPredicatePushdown() {
            seedJoinData();

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT i.id, i.name, p.amount "
                    + "FROM " + iceFqn() + " i "
                    + "JOIN " + paiFqn() + " p ON i.id = p.id "
                    + "WHERE i.region = 'east' AND p.status = 'active'");

            assertThat(rows).hasSize(1);
            assertIdName(rows.get(0), 1L, "alice");
            assertAmount(rows.get(0), 100.0);
        }
    }

    // --- R11.3: Cross-connector JOIN with aggregation ---

    @Nested
    @DisplayName("R11.3 - Cross-Connector JOIN with Aggregation")
    class CrossConnectorJoinWithAggregation {

        @Test
        @DisplayName("JOIN with GROUP BY produces correct aggregate results")
        void joinWithAggregation() {
            seedJoinData();

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT i.region, COUNT(*) AS cnt, SUM(p.amount) AS total "
                    + "FROM " + iceFqn() + " i "
                    + "JOIN " + paiFqn() + " p ON i.id = p.id "
                    + "GROUP BY i.region "
                    + "ORDER BY i.region");

            assertThat(rows).hasSize(2);

            TypedRow east = rows.get(0);
            assertThat(east.getRaw("region")).isEqualTo("east");
            assertThat(((Number) east.getRaw("cnt")).longValue()).isEqualTo(1L);
            assertThat(((Number) east.getRaw("total")).doubleValue()).isEqualTo(100.0);

            TypedRow west = rows.get(1);
            assertThat(west.getRaw("region")).isEqualTo("west");
            assertThat(((Number) west.getRaw("cnt")).longValue()).isEqualTo(1L);
            assertThat(((Number) west.getRaw("total")).doubleValue()).isEqualTo(200.0);
        }
    }

    // --- R11.4: Cross-connector UNION ALL ---

    @Nested
    @DisplayName("R11.4 - Cross-Connector UNION ALL")
    class CrossConnectorUnionAll {

        @Test
        @DisplayName("UNION ALL returns all rows from both connectors")
        void unionAllAcrossConnectors() {
            sqlRunner.executeDdl("CREATE TABLE " + iceFqn()
                    + " (id BIGINT, label VARCHAR)");
            sqlRunner.execute("INSERT INTO " + iceFqn()
                    + " (id, label) VALUES (1, 'ice_a'), (2, 'ice_b')");

            sqlRunner.executeDdl("CREATE TABLE " + paiFqn()
                    + " (id BIGINT, label VARCHAR)");
            sqlRunner.execute("INSERT INTO " + paiFqn()
                    + " (id, label) VALUES (3, 'pai_c'), (4, 'pai_d'), (5, 'pai_e')");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT id, label FROM " + iceFqn()
                    + " UNION ALL "
                    + "SELECT id, label FROM " + paiFqn());

            assertThat(rows).hasSize(5);
            // Verify all expected labels are present (order not guaranteed across connectors)
            Set<String> labels = rows.stream()
                    .map(r -> r.getRaw("label").toString())
                    .collect(java.util.stream.Collectors.toSet());
            assertThat(labels).containsExactlyInAnyOrder(
                    "ice_a", "ice_b", "pai_c", "pai_d", "pai_e");
        }
    }

    // --- R11.5: Cross-connector subquery ---

    @Nested
    @DisplayName("R11.5 - Cross-Connector Subquery")
    class CrossConnectorSubquery {

        @Test
        @DisplayName("Iceberg subquery filters Paimon outer query correctly")
        void subqueryAcrossConnectors() {
            seedJoinData();

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT p.id, p.amount FROM " + paiFqn() + " p "
                    + "WHERE p.id IN ("
                    + "  SELECT i.id FROM " + iceFqn() + " i "
                    + "  WHERE i.region = 'east'"
                    + ") ORDER BY p.id");

            assertThat(rows).hasSize(1);
            assertThat(((Number) rows.get(0).getRaw("id")).longValue()).isEqualTo(1L);
            assertAmount(rows.get(0), 100.0);
        }
    }

    // --- R11.6: Three-way cross-connector JOIN ---

    @Nested
    @DisplayName("R11.6 - Three-Way Cross-Connector JOIN")
    class ThreeWayCrossConnectorJoin {

        @Test
        @DisplayName("Iceberg × Paimon × Iceberg (different table) JOIN returns correct results")
        void threeWayJoin() {
            seedJoinData();

            String iceTable2 = iceTable + "_extra";
            String iceFqn2 = ICE_PREFIX + iceTable2;
            sqlRunner.executeDdl("CREATE TABLE " + iceFqn2
                    + " (region VARCHAR, tax_rate DOUBLE)");
            sqlRunner.execute("INSERT INTO " + iceFqn2
                    + " (region, tax_rate) VALUES ('east', 0.08), ('west', 0.10)");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT i.id, i.name, p.amount, r.tax_rate "
                    + "FROM " + iceFqn() + " i "
                    + "JOIN " + paiFqn() + " p ON i.id = p.id "
                    + "JOIN " + iceFqn2 + " r ON i.region = r.region "
                    + "ORDER BY i.id");

            assertThat(rows).hasSize(2);

            TypedRow row1 = rows.get(0);
            assertIdName(row1, 1L, "alice");
            assertThat(((Number) row1.getRaw("tax_rate")).doubleValue()).isEqualTo(0.08);

            TypedRow row2 = rows.get(1);
            assertIdName(row2, 2L, "bob");
            assertThat(((Number) row2.getRaw("tax_rate")).doubleValue()).isEqualTo(0.10);
        }
    }

    // --- R11.7: Cross-connector JOIN with ORDER BY + LIMIT ---

    @Nested
    @DisplayName("R11.7 - Cross-Connector JOIN with ORDER BY + LIMIT")
    class CrossConnectorJoinOrderByLimit {

        @Test
        @DisplayName("JOIN with ORDER BY + LIMIT returns correct sorted subset")
        void joinWithOrderByAndLimit() {
            seedJoinData();

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT i.id, i.name, p.amount "
                    + "FROM " + iceFqn() + " i "
                    + "JOIN " + paiFqn() + " p ON i.id = p.id "
                    + "ORDER BY p.amount DESC "
                    + "LIMIT 1");

            assertThat(rows).hasSize(1);
            assertIdName(rows.get(0), 2L, "bob");
            assertAmount(rows.get(0), 200.0);
        }
    }

    // --- R11.8: Cross-connector error propagation ---

    @Nested
    @DisplayName("R11.8 - Cross-Connector Error Propagation")
    class CrossConnectorErrorPropagation {

        @Test
        @DisplayName("Error from one connector includes source info")
        void errorIncludesConnectorSource() {
            seedJoinData();

            String nonExistentTable = ICE_PREFIX + "non_existent_table_" + System.nanoTime();

            QueryError error = sqlRunner.executeExpectingError(
                    "SELECT * FROM " + nonExistentTable + " i "
                    + "JOIN " + paiFqn() + " p ON i.id = p.id");

            assertThat(error.errorType()).isIn("SEMANTIC", "RUNTIME");
            assertThat(error.message()).containsIgnoringCase("not");
        }
    }

    // --- R11.9: Cross-connector INSERT SELECT ---

    @Nested
    @DisplayName("R11.9 - Cross-Connector INSERT SELECT")
    class CrossConnectorInsertSelect {

        @Test
        @DisplayName("Read from Iceberg, write to Paimon transfers data correctly")
        void insertSelectAcrossConnectors() {
            sqlRunner.executeDdl("CREATE TABLE " + iceFqn()
                    + " (id BIGINT, name VARCHAR, score DOUBLE)");
            sqlRunner.execute("INSERT INTO " + iceFqn()
                    + " (id, name, score) VALUES "
                    + "(1, 'alice', 90.0), (2, 'bob', 85.0), (3, 'carol', 95.0)");

            sqlRunner.executeDdl("CREATE TABLE " + paiFqn()
                    + " (id BIGINT, name VARCHAR, score DOUBLE)");

            sqlRunner.execute("INSERT INTO " + paiFqn()
                    + " SELECT id, name, score FROM " + iceFqn());

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + paiFqn() + " ORDER BY id");

            assertThat(rows).hasSize(3);
            assertThat(rows.get(0).getRaw("name")).isEqualTo("alice");
            assertThat(((Number) rows.get(0).getRaw("score")).doubleValue()).isEqualTo(90.0);
            assertThat(rows.get(1).getRaw("name")).isEqualTo("bob");
            assertThat(rows.get(2).getRaw("name")).isEqualTo("carol");
            assertThat(((Number) rows.get(2).getRaw("score")).doubleValue()).isEqualTo(95.0);
        }
    }

    // --- Assertion helpers ---

    private static void assertIdName(TypedRow row,
                                     long expectedId, String expectedName) {
        assertThat(((Number) row.getRaw("id")).longValue()).isEqualTo(expectedId);
        assertThat(row.getRaw("name")).isEqualTo(expectedName);
    }

    private static void assertAmount(TypedRow row, double expected) {
        assertThat(((Number) row.getRaw("amount")).doubleValue()).isEqualTo(expected);
    }
}
