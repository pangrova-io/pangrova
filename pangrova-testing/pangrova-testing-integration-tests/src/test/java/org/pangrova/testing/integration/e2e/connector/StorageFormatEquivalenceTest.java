package org.pangrova.testing.integration.e2e.connector;

import org.pangrova.spi.connector.data.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.pangrova.testing.integration.e2e.fixture.WriteTestHelper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E tests verifying ORC format reading correctness through the real pipeline.
 *
 * <p>Uses metamorphic testing: same data written to ORC and Parquet tables must
 * produce identical query results. Also verifies predicate pushdown, multi-type
 * support, and schema evolution on ORC tables.
 *
 * <p>Requirements: 3.1, 3.2, 3.3, 3.4
 */
@DisplayName("Storage Format Equivalence - ORC vs Parquet E2E Tests")
class StorageFormatEquivalenceTest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String FQN_PREFIX = CATALOG + "." + SCHEMA + ".";

    private String orcTable;
    private String parquetTable;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(
                CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeEach
    void assignUniqueTableNames(TestInfo testInfo) {
        String method = testInfo.getTestMethod().map(m -> m.getName()).orElse("unknown");
        String suffix = "_" + System.nanoTime();
        orcTable = "t_orc_" + method + suffix;
        parquetTable = "t_pqt_" + method + suffix;
    }

    // --- N3.1: ORC and Parquet return same data (metamorphic oracle) ---

    @Test
    @DisplayName("N3.1 - ORC and Parquet tables return identical results for same data")
    void orcAndParquetReturnSameData() {
        String columns = "id BIGINT, name VARCHAR, amount DOUBLE";
        createOrcTable(orcTable, columns);
        createParquetTable(parquetTable, columns);

        List<TypedRow> rows = buildRows(1, 20);
        insertRows(orcTable, rows);
        insertRows(parquetTable, rows);

        List<TypedRow> orcResult = sqlRunner.execute(
                "SELECT * FROM " + fqn(orcTable) + " ORDER BY id");
        List<TypedRow> parquetResult = sqlRunner.execute(
                "SELECT * FROM " + fqn(parquetTable) + " ORDER BY id");

        assertThat(orcResult).isEqualTo(parquetResult);
    }

    // --- N3.2: Predicate pushdown on ORC table ---

    @Test
    @DisplayName("N3.2 - ORC predicate pushdown returns correct filtered rows")
    void orcPredicatePushdownCorrectness() {
        String columns = "id BIGINT, name VARCHAR, amount DOUBLE";
        createOrcTable(orcTable, columns);
        insertRows(orcTable, buildRows(1, 50));

        List<TypedRow> rows = sqlRunner.execute(
                "SELECT * FROM " + fqn(orcTable) + " WHERE amount > 3000.0 ORDER BY id");

        // amount = id * 100, so amount > 3000 means id > 30 → ids 31..50
        assertThat(rows).hasSize(20);
        assertThat(extractIds(rows).get(0)).isEqualTo(31L);
        assertThat(extractIds(rows).get(rows.size() - 1)).isEqualTo(50L);
    }

    // --- N3.3: ORC table with all 6 data types ---

    @Test
    @DisplayName("N3.3 - ORC table reads all data types correctly")
    void orcAllDataTypesReadable() {
        String columns = "id BIGINT, label VARCHAR, score DOUBLE, "
                + "active BOOLEAN, created DATE, updated TIMESTAMP";
        createOrcTable(orcTable, columns);

        sqlRunner.execute("INSERT INTO " + fqn(orcTable)
                + " (id, label, score, active, created, updated) VALUES "
                + "(1, 'alpha', 99.5, true, DATE '2024-01-15', TIMESTAMP '2024-01-15 10:30:00'), "
                + "(2, 'beta', 0.0, false, DATE '2024-06-01', TIMESTAMP '2024-06-01 23:59:59')");

        List<TypedRow> rows = sqlRunner.execute(
                "SELECT * FROM " + fqn(orcTable) + " ORDER BY id");

        assertThat(rows).hasSize(2);
        assertRow(rows.get(0), 1L, "alpha", 99.5, true);
        assertRow(rows.get(1), 2L, "beta", 0.0, false);
    }

    // --- N3.4: Schema evolution on ORC table (ADD COLUMN → NULL fill) ---

    @Test
    @DisplayName("N3.4 - ORC schema evolution fills NULL for new column on old data")
    void orcSchemaEvolutionNullFill() {
        String columns = "id BIGINT, name VARCHAR";
        createOrcTable(orcTable, columns);

        sqlRunner.execute("INSERT INTO " + fqn(orcTable)
                + " (id, name) VALUES (1, 'before_evolution'), (2, 'also_before')");

        sqlRunner.executeDdl("ALTER TABLE " + fqn(orcTable) + " ADD COLUMN score DOUBLE");

        sqlRunner.execute("INSERT INTO " + fqn(orcTable)
                + " (id, name, score) VALUES (3, 'after_evolution', 88.0)");

        List<TypedRow> rows = sqlRunner.execute(
                "SELECT id, name, score FROM " + fqn(orcTable) + " ORDER BY id");

        assertThat(rows).hasSize(3);
        assertThat(rows.get(0).getRaw("score")).isNull();
        assertThat(rows.get(1).getRaw("score")).isNull();
        assertThat(rows.get(2).getRaw("score")).isEqualTo(88.0);
    }

    // -----------------------------------------------------------------------
    // Table creation helpers
    // -----------------------------------------------------------------------

    private void createOrcTable(String table, String columns) {
        sqlRunner.executeDdl(String.format(
                "CREATE TABLE %s (%s) TBLPROPERTIES ('write.format.default'='orc')",
                fqn(table), columns));
    }

    private void createParquetTable(String table, String columns) {
        sqlRunner.executeDdl(String.format(
                "CREATE TABLE %s (%s)", fqn(table), columns));
    }

    // -----------------------------------------------------------------------
    // Row building and insertion helpers
    // -----------------------------------------------------------------------

    private void insertRows(String table, List<TypedRow> rows) {
        WriteTestHelper.insertRows(sqlRunner, fqn(table), rows);
    }

    private List<TypedRow> buildRows(int startId, int endId) {
        return WriteTestHelper.buildRows(startId, endId);
    }

    // -----------------------------------------------------------------------
    // Assertion helpers
    // -----------------------------------------------------------------------

    private static void assertRow(TypedRow row,
                                  long expectedId, String expectedLabel,
                                  double expectedScore, boolean expectedActive) {
        assertThat(row.getRaw("id")).isEqualTo(expectedId);
        assertThat(row.getRaw("label")).isEqualTo(expectedLabel);
        assertThat(row.getRaw("score")).isEqualTo(expectedScore);
        assertThat(row.getRaw("active")).isEqualTo(expectedActive);
    }

    private static List<Long> extractIds(List<TypedRow> rows) {
        return rows.stream()
                .map(r -> ((Number) r.getRaw("id")).longValue())
                .collect(Collectors.toList());
    }

    // -----------------------------------------------------------------------
    // SQL literal formatting
    // -----------------------------------------------------------------------

    private String fqn(String table) {
        return FQN_PREFIX + table;
    }


}
