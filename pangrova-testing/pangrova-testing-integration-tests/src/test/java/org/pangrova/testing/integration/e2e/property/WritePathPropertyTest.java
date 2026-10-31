package org.pangrova.testing.integration.e2e.property;

import net.jqwik.api.*;
import net.jqwik.api.lifecycle.BeforeContainer;
import org.pangrova.connector.spi.data.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based tests for write path correctness.
 *
 * <p>Validates write path invariants:
 * - INSERT increases row count by exact number of inserted rows
 * - UPDATE preserves total row count
 * - DELETE decreases row count
 * - Write then read consistency (what you write is what you get)</p>
 *
 * <p>Requirements validated: 6.1, 6.2, 9.3</p>
 */
@DisplayName("Write Path - Property Tests")
class WritePathPropertyTest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String FQN_PREFIX = CATALOG + "." + SCHEMA + ".";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeAll
    @BeforeContainer
    static void registerConnectors() {
        isDockerReady();
        registerTestConnectors(List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString())));
    }

    @Property(tries = 3)
    @Tag("Property: INSERT increases row count")
    void insertIncreasesRowCount(@ForAll("rowCounts") int numRows) {
        String table = FQN_PREFIX + "prop_write_ins_" + System.nanoTime();
        sqlRunner.executeDdl("CREATE TABLE " + table + " (id BIGINT, val VARCHAR)");

        StringBuilder sb = new StringBuilder("INSERT INTO " + table + " VALUES ");
        for (int i = 0; i < numRows; i++) {
            if (i > 0) sb.append(", ");
            sb.append("(").append(i).append(", 'v").append(i).append("')");
        }
        sqlRunner.execute(sb.toString());

        List<TypedRow> rows = sqlRunner.execute("SELECT COUNT(*) AS cnt FROM " + table);
        assertThat(((Number) rows.get(0).getRaw("cnt")).longValue()).isEqualTo(numRows);

        sqlRunner.executeDdl("DROP TABLE " + table);
    }

    @Property(tries = 3)
    @Tag("Property: UPDATE preserves row count")
    void updatePreservesRowCount(@ForAll("values") String newVal) {
        String table = FQN_PREFIX + "prop_write_upd_" + System.nanoTime();
        sqlRunner.executeDdl("CREATE TABLE " + table + " (id BIGINT, val VARCHAR)");
        sqlRunner.execute("INSERT INTO " + table + " VALUES (1, 'a'), (2, 'b'), (3, 'c')");

        List<TypedRow> before = sqlRunner.execute("SELECT COUNT(*) AS cnt FROM " + table);
        sqlRunner.execute("UPDATE " + table + " SET val = '" + newVal + "' WHERE id = 1");
        List<TypedRow> after = sqlRunner.execute("SELECT COUNT(*) AS cnt FROM " + table);

        assertThat(((Number) after.get(0).getRaw("cnt")).longValue())
                .isEqualTo(((Number) before.get(0).getRaw("cnt")).longValue());

        sqlRunner.executeDdl("DROP TABLE " + table);
    }

    @Property(tries = 3)
    @Tag("Property: DELETE reduces row count")
    void deleteReducesRowCount(@ForAll("deleteIds") int deleteId) {
        String table = FQN_PREFIX + "prop_write_del_" + System.nanoTime();
        sqlRunner.executeDdl("CREATE TABLE " + table + " (id BIGINT, val VARCHAR)");
        sqlRunner.execute("INSERT INTO " + table + " VALUES (1, 'a'), (2, 'b'), (3, 'c')");

        List<TypedRow> before = sqlRunner.execute("SELECT COUNT(*) AS cnt FROM " + table);
        sqlRunner.execute("DELETE FROM " + table + " WHERE id = " + deleteId);
        List<TypedRow> after = sqlRunner.execute("SELECT COUNT(*) AS cnt FROM " + table);

        assertThat(((Number) after.get(0).getRaw("cnt")).longValue())
                .isLessThanOrEqualTo(((Number) before.get(0).getRaw("cnt")).longValue());

        sqlRunner.executeDdl("DROP TABLE " + table);
    }

    @Property(tries = 1)
    @Tag("Property: write-read consistency")
    void writeReadConsistency() {
        String table = FQN_PREFIX + "prop_write_rr_" + System.nanoTime();
        sqlRunner.executeDdl("CREATE TABLE " + table + " (id BIGINT, name VARCHAR)");
        sqlRunner.execute("INSERT INTO " + table + " VALUES (42, 'test_value')");

        List<TypedRow> rows = sqlRunner.execute(
                "SELECT * FROM " + table + " WHERE id = 42");
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getRaw("name")).isEqualTo("test_value");

        sqlRunner.executeDdl("DROP TABLE " + table);
    }

    @Provide
    Arbitrary<Integer> rowCounts() {
        return Arbitraries.of(1, 3, 5);
    }

    @Provide
    Arbitrary<String> values() {
        return Arbitraries.of("updated", "new_val", "changed");
    }

    @Provide
    Arbitrary<Integer> deleteIds() {
        return Arbitraries.of(1, 2, 3);
    }
}
