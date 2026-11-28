package org.pangrova.testing.integration.e2e.connector;

import org.pangrova.types.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.pangrova.testing.integration.e2e.fixture.SqlTestRowWriter;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Cross-connector equivalence tests: Iceberg vs Paimon.
 *
 * <p>Verifies that the same query against the same data returns identical
 * results regardless of whether the underlying storage is Iceberg or Paimon.
 *
 * <p>Requirements: 41.4
 */
@DisplayName("Cross-Connector Equivalence - Iceberg vs Paimon E2E Tests")
class CrossConnectorEquivalenceTest extends SqlE2ETestBase {

    private static final String ICE_CATALOG = "iceberg_xconn";
    private static final String PAI_CATALOG = "paimon_xconn";
    private static final String SCHEMA = "default";

    private String icebergTable;
    private String paimonTable;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(
                        ICE_CATALOG, SCHEMA, icebergWarehouse().toString()),
                ConnectorTestConfig.paimon(
                        PAI_CATALOG, SCHEMA, paimonWarehouse().toString()));
    }

    @BeforeAll
    static void registerConnectors() {
        registerTestConnectors(List.of(
                ConnectorTestConfig.icebergLocal(
                        ICE_CATALOG, SCHEMA, icebergWarehouse().toString()),
                ConnectorTestConfig.paimon(
                        PAI_CATALOG, SCHEMA, paimonWarehouse().toString())));
    }

    @BeforeEach
    void assignUniqueTableNames(TestInfo testInfo) {
        String method = testInfo.getTestMethod()
                .map(m -> m.getName()).orElse("unknown");
        String suffix = "_" + System.nanoTime();
        icebergTable = "t_ice_" + method + suffix;
        paimonTable = "t_pai_" + method + suffix;
    }

    // --- 41.4: Same data, same query, Iceberg vs Paimon → identical results ---

    @Nested
    @DisplayName("41.4 - SELECT equivalence across Iceberg and Paimon")
    class SelectEquivalence {

        @Test
        @DisplayName("Same rows inserted into both connectors return identical SELECT results")
        void sameDataReturnsSameResults() {
            String columns = "id BIGINT, name VARCHAR, amount DOUBLE";
            createIcebergTable(icebergTable, columns);
            createPaimonTable(paimonTable, columns);

            List<TypedRow> rows = buildRows(1, 20);
            insertRows(icebergFqn(icebergTable), rows);
            insertRows(paimonFqn(paimonTable), rows);

            List<TypedRow> iceResult = sqlRunner.execute(
                    "SELECT * FROM " + icebergFqn(icebergTable) + " ORDER BY id");
            List<TypedRow> paiResult = sqlRunner.execute(
                    "SELECT * FROM " + paimonFqn(paimonTable) + " ORDER BY id");

            assertThat(iceResult).isEqualTo(paiResult);
        }
    }

    @Nested
    @DisplayName("41.4 - Aggregation equivalence across Iceberg and Paimon")
    class AggregationEquivalence {

        @Test
        @DisplayName("SUM and COUNT return identical results from both connectors")
        void aggregationReturnsSameResults() {
            String columns = "id BIGINT, name VARCHAR, amount DOUBLE";
            createIcebergTable(icebergTable, columns);
            createPaimonTable(paimonTable, columns);

            List<TypedRow> rows = buildRows(1, 50);
            insertRows(icebergFqn(icebergTable), rows);
            insertRows(paimonFqn(paimonTable), rows);

            String aggSql = "SELECT COUNT(*) AS cnt, SUM(amount) AS total FROM ";

            List<TypedRow> iceAgg = sqlRunner.execute(
                    aggSql + icebergFqn(icebergTable));
            List<TypedRow> paiAgg = sqlRunner.execute(
                    aggSql + paimonFqn(paimonTable));

            assertThat(iceAgg).isEqualTo(paiAgg);
        }
    }

    // --- 107.1: JOIN equivalence ---

    @Nested
    @DisplayName("107.1 - JOIN equivalence across Iceberg and Paimon")
    class JoinEquivalence {

        private String iceDimTable;
        private String paiDimTable;

        @BeforeEach
        void createDimensionTables() {
            long suffix = System.nanoTime();
            iceDimTable = "t_ice_dim_" + suffix;
            paiDimTable = "t_pai_dim_" + suffix;

            String dimColumns = "id BIGINT, label VARCHAR";
            createIcebergTable(iceDimTable, dimColumns);
            createPaimonTable(paiDimTable, dimColumns);

            String dimValues = "(1, 'cat_a'), (2, 'cat_b'), (3, 'cat_c')";
            sqlRunner.execute("INSERT INTO " + icebergFqn(iceDimTable)
                    + " (id, label) VALUES " + dimValues);
            sqlRunner.execute("INSERT INTO " + paimonFqn(paiDimTable)
                    + " (id, label) VALUES " + dimValues);
        }

        @Test
        @DisplayName("INNER JOIN returns identical results from both connectors")
        void joinReturnsSameResults() {
            String columns = "id BIGINT, name VARCHAR, amount DOUBLE";
            createIcebergTable(icebergTable, columns);
            createPaimonTable(paimonTable, columns);

            List<TypedRow> rows = buildRows(1, 10);
            insertRows(icebergFqn(icebergTable), rows);
            insertRows(paimonFqn(paimonTable), rows);

            String joinSql = "SELECT t.id, t.name, d.label FROM %s t "
                    + "INNER JOIN %s d ON t.id = d.id ORDER BY t.id";

            List<TypedRow> iceResult = sqlRunner.execute(
                    String.format(joinSql, icebergFqn(icebergTable), icebergFqn(iceDimTable)));
            List<TypedRow> paiResult = sqlRunner.execute(
                    String.format(joinSql, paimonFqn(paimonTable), paimonFqn(paiDimTable)));

            assertThat(iceResult).isEqualTo(paiResult);
        }
    }

    // --- 107.2: Window function equivalence ---

    @Nested
    @DisplayName("107.2 - Window function equivalence across Iceberg and Paimon")
    class WindowFunctionEquivalence {

        @Test
        @DisplayName("ROW_NUMBER and RANK return identical results from both connectors")
        void windowFunctionsReturnSameResults() {
            String columns = "id BIGINT, name VARCHAR, amount DOUBLE";
            createIcebergTable(icebergTable, columns);
            createPaimonTable(paimonTable, columns);

            List<TypedRow> rows = buildRows(1, 10);
            insertRows(icebergFqn(icebergTable), rows);
            insertRows(paimonFqn(paimonTable), rows);

            String windowSql = "SELECT id, name, "
                    + "ROW_NUMBER() OVER (ORDER BY id) AS rn, "
                    + "RANK() OVER (ORDER BY amount) AS rnk "
                    + "FROM %s ORDER BY id";

            List<TypedRow> iceResult = sqlRunner.execute(
                    String.format(windowSql, icebergFqn(icebergTable)));
            List<TypedRow> paiResult = sqlRunner.execute(
                    String.format(windowSql, paimonFqn(paimonTable)));

            assertThat(iceResult).isEqualTo(paiResult);
        }
    }

    // --- 107.3: Subquery equivalence ---

    @Nested
    @DisplayName("107.3 - Subquery equivalence across Iceberg and Paimon")
    class SubqueryEquivalence {

        @Test
        @DisplayName("IN-subquery returns identical results from both connectors")
        void subqueryReturnsSameResults() {
            String columns = "id BIGINT, name VARCHAR, amount DOUBLE";
            createIcebergTable(icebergTable, columns);
            createPaimonTable(paimonTable, columns);

            List<TypedRow> rows = buildRows(1, 20);
            insertRows(icebergFqn(icebergTable), rows);
            insertRows(paimonFqn(paimonTable), rows);

            String subSql = "SELECT * FROM %s WHERE id IN "
                    + "(SELECT id FROM %s WHERE amount > 1000.0) ORDER BY id";

            List<TypedRow> iceResult = sqlRunner.execute(
                    String.format(subSql, icebergFqn(icebergTable), icebergFqn(icebergTable)));
            List<TypedRow> paiResult = sqlRunner.execute(
                    String.format(subSql, paimonFqn(paimonTable), paimonFqn(paimonTable)));

            assertThat(iceResult).isEqualTo(paiResult);
        }
    }

    // --- 107.4: ORDER BY + LIMIT equivalence ---

    @Nested
    @DisplayName("107.4 - ORDER BY + LIMIT equivalence across Iceberg and Paimon")
    class OrderByLimitEquivalence {

        @Test
        @DisplayName("ORDER BY DESC LIMIT returns identical results from both connectors")
        void orderByLimitReturnsSameResults() {
            String columns = "id BIGINT, name VARCHAR, amount DOUBLE";
            createIcebergTable(icebergTable, columns);
            createPaimonTable(paimonTable, columns);

            List<TypedRow> rows = buildRows(1, 20);
            insertRows(icebergFqn(icebergTable), rows);
            insertRows(paimonFqn(paimonTable), rows);

            String sql = "SELECT * FROM %s ORDER BY amount DESC LIMIT 5";

            List<TypedRow> iceResult = sqlRunner.execute(
                    String.format(sql, icebergFqn(icebergTable)));
            List<TypedRow> paiResult = sqlRunner.execute(
                    String.format(sql, paimonFqn(paimonTable)));

            assertThat(iceResult).isEqualTo(paiResult);
        }
    }

    // --- 107.5: GROUP BY + HAVING equivalence ---

    @Nested
    @DisplayName("107.5 - GROUP BY + HAVING equivalence across Iceberg and Paimon")
    class GroupByHavingEquivalence {

        @Test
        @DisplayName("GROUP BY HAVING returns identical results from both connectors")
        void groupByHavingReturnsSameResults() {
            String columns = "id BIGINT, name VARCHAR, amount DOUBLE, category VARCHAR";
            createIcebergTable(icebergTable, columns);
            createPaimonTable(paimonTable, columns);

            List<TypedRow> rows = buildRowsWithCategory(1, 20);
            insertRows(icebergFqn(icebergTable), rows);
            insertRows(paimonFqn(paimonTable), rows);

            String sql = "SELECT category, COUNT(*) AS cnt, SUM(amount) AS total "
                    + "FROM %s GROUP BY category HAVING COUNT(*) > 1 ORDER BY category";

            List<TypedRow> iceResult = sqlRunner.execute(
                    String.format(sql, icebergFqn(icebergTable)));
            List<TypedRow> paiResult = sqlRunner.execute(
                    String.format(sql, paimonFqn(paimonTable)));

            assertThat(iceResult).isEqualTo(paiResult);
        }
    }

    // --- 107.6: DISTINCT equivalence ---

    @Nested
    @DisplayName("107.6 - DISTINCT equivalence across Iceberg and Paimon")
    class DistinctEquivalence {

        @Test
        @DisplayName("SELECT DISTINCT returns identical results from both connectors")
        void distinctReturnsSameResults() {
            String columns = "id BIGINT, name VARCHAR, amount DOUBLE";
            createIcebergTable(icebergTable, columns);
            createPaimonTable(paimonTable, columns);

            // Insert rows with duplicate names
            List<TypedRow> rows = buildRows(1, 10);
            List<TypedRow> dupes = buildRows(1, 5); // duplicate names
            insertRows(icebergFqn(icebergTable), rows);
            insertRows(icebergFqn(icebergTable), dupes);
            insertRows(paimonFqn(paimonTable), rows);
            insertRows(paimonFqn(paimonTable), dupes);

            String sql = "SELECT DISTINCT name FROM %s ORDER BY name";

            List<TypedRow> iceResult = sqlRunner.execute(
                    String.format(sql, icebergFqn(icebergTable)));
            List<TypedRow> paiResult = sqlRunner.execute(
                    String.format(sql, paimonFqn(paimonTable)));

            assertThat(iceResult).isEqualTo(paiResult);
        }
    }

    // --- 107.7: Set operations (UNION ALL) equivalence ---

    @Nested
    @DisplayName("107.7 - UNION ALL equivalence across Iceberg and Paimon")
    class SetOperationsEquivalence {

        @Test
        @DisplayName("UNION ALL returns identical results from both connectors")
        void unionAllReturnsSameResults() {
            String columns = "id BIGINT, name VARCHAR, amount DOUBLE";
            String iceTable2 = "t_ice_u2_" + System.nanoTime();
            String paiTable2 = "t_pai_u2_" + System.nanoTime();

            createIcebergTable(icebergTable, columns);
            createIcebergTable(iceTable2, columns);
            createPaimonTable(paimonTable, columns);
            createPaimonTable(paiTable2, columns);

            List<TypedRow> rows1 = buildRows(1, 5);
            List<TypedRow> rows2 = buildRows(6, 10);

            insertRows(icebergFqn(icebergTable), rows1);
            insertRows(icebergFqn(iceTable2), rows2);
            insertRows(paimonFqn(paimonTable), rows1);
            insertRows(paimonFqn(paiTable2), rows2);

            String sql = "SELECT * FROM %s UNION ALL SELECT * FROM %s ORDER BY id";

            List<TypedRow> iceResult = sqlRunner.execute(
                    String.format(sql, icebergFqn(icebergTable), icebergFqn(iceTable2)));
            List<TypedRow> paiResult = sqlRunner.execute(
                    String.format(sql, paimonFqn(paimonTable), paimonFqn(paiTable2)));

            assertThat(iceResult).extracting(row -> row.getLong("id"))
                    .containsExactly(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L);
            assertThat(paiResult).containsExactlyElementsOf(iceResult);
        }
    }

    // -----------------------------------------------------------------------
    // Table creation helpers
    // -----------------------------------------------------------------------

    private void createIcebergTable(String table, String columns) {
        sqlRunner.executeDdl(String.format(
                "CREATE TABLE %s (%s)", icebergFqn(table), columns));
    }

    private void createPaimonTable(String table, String columns) {
        sqlRunner.executeDdl(String.format(
                "CREATE TABLE %s (%s)", paimonFqn(table), columns));
    }

    // -----------------------------------------------------------------------
    // Row building and insertion helpers
    // -----------------------------------------------------------------------

    private void insertRows(String fqn, List<TypedRow> rows) {
        SqlTestRowWriter.insertRows(sqlRunner, fqn, rows);
    }

    private List<TypedRow> buildRows(int startId, int endId) {
        return SqlTestRowWriter.buildRows(startId, endId);
    }

    private List<TypedRow> buildRowsWithCategory(int startId, int endId) {
        String[] categories = {"electronics", "clothing", "food"};
        return LongStream.rangeClosed(startId, endId)
                .mapToObj(id -> TypedRow.builder().putRaw("id", id).putRaw("name", "item_" + id).putRaw("amount", (double) (id * 100)).putRaw("category", categories[(int) (id % categories.length)]).build())
                .collect(Collectors.toList());
    }

    // -----------------------------------------------------------------------
    // FQN and SQL literal helpers
    // -----------------------------------------------------------------------

    private static String icebergFqn(String table) {
        return ICE_CATALOG + "." + SCHEMA + "." + table;
    }

    private static String paimonFqn(String table) {
        return PAI_CATALOG + "." + SCHEMA + "." + table;
    }


}
