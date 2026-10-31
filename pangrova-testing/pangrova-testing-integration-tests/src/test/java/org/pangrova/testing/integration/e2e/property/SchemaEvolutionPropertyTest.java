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
 * Property-based tests for schema evolution correctness.
 *
 * <p>Validates that schema changes (ADD COLUMN, DROP COLUMN) do not corrupt
 * existing data and maintain read consistency:
 * - Adding a column does not affect existing row values
 * - New columns default to NULL for existing rows
 * - Row count is preserved across schema changes
 * - Queries on original columns still work after evolution</p>
 *
 * <p>Requirements validated: 8.1, 8.2, 9.3</p>
 */
@DisplayName("Schema Evolution - Property Tests")
class SchemaEvolutionPropertyTest extends SqlE2ETestBase {

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
    @Tag("Property: ADD COLUMN preserves data")
    void addColumnPreservesData(@ForAll("columnNames") String newCol) {
        String table = FQN_PREFIX + "prop_schema_add_" + System.nanoTime();
        sqlRunner.executeDdl("CREATE TABLE " + table + " (id BIGINT, name VARCHAR)");
        sqlRunner.execute("INSERT INTO " + table + " VALUES (1, 'alice'), (2, 'bob')");

        List<TypedRow> before = sqlRunner.execute("SELECT id, name FROM " + table);
        sqlRunner.executeDdl("ALTER TABLE " + table + " ADD COLUMN " + newCol + " VARCHAR");
        List<TypedRow> after = sqlRunner.execute("SELECT id, name FROM " + table);

        assertThat(after).hasSameSizeAs(before);
        for (int i = 0; i < before.size(); i++) {
            assertThat(after.get(i).getRaw("id")).isEqualTo(before.get(i).getRaw("id"));
            assertThat(after.get(i).getRaw("name")).isEqualTo(before.get(i).getRaw("name"));
        }
        sqlRunner.executeDdl("DROP TABLE " + table);
    }

    @Property(tries = 3)
    @Tag("Property: new column defaults to NULL")
    void newColumnDefaultsToNull(@ForAll("columnNames") String newCol) {
        String table = FQN_PREFIX + "prop_schema_null_" + System.nanoTime();
        sqlRunner.executeDdl("CREATE TABLE " + table + " (id BIGINT, name VARCHAR)");
        sqlRunner.execute("INSERT INTO " + table + " VALUES (1, 'alice')");

        sqlRunner.executeDdl("ALTER TABLE " + table + " ADD COLUMN " + newCol + " BIGINT");
        List<TypedRow> rows = sqlRunner.execute("SELECT " + newCol + " FROM " + table);
        assertThat(rows.get(0).getRaw(newCol)).isNull();

        sqlRunner.executeDdl("DROP TABLE " + table);
    }

    @Property(tries = 1)
    @Tag("Property: schema change preserves row count")
    void schemaChangePreservesRowCount() {
        String table = FQN_PREFIX + "prop_schema_cnt_" + System.nanoTime();
        sqlRunner.executeDdl("CREATE TABLE " + table
                + " (id BIGINT, val VARCHAR, extra VARCHAR)");
        sqlRunner.execute("INSERT INTO " + table + " VALUES "
                + "(1, 'a', 'x'), (2, 'b', 'y'), (3, 'c', 'z')");

        List<TypedRow> before = sqlRunner.execute("SELECT COUNT(*) AS cnt FROM " + table);
        sqlRunner.executeDdl("ALTER TABLE " + table + " DROP COLUMN extra");
        List<TypedRow> after = sqlRunner.execute("SELECT COUNT(*) AS cnt FROM " + table);

        assertThat(((Number) after.get(0).getRaw("cnt")).longValue())
                .isEqualTo(((Number) before.get(0).getRaw("cnt")).longValue());

        sqlRunner.executeDdl("DROP TABLE " + table);
    }

    @Provide
    Arbitrary<String> columnNames() {
        return Arbitraries.of("col_new", "extra_field", "metadata");
    }
}
