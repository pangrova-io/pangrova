package org.pangrova.testing.integration.e2e.ddl;

import org.pangrova.spi.connector.data.value.TypedRow;
import org.pangrova.testing.testkit.E2ETest;
import org.pangrova.testing.integration.e2e.PangrovaE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E tests for schema evolution DDL operations.
 *
 * <p>Covers ALTER TABLE ADD COLUMN, DROP COLUMN, RENAME COLUMN,
 * and CHANGE COLUMN TYPE through the full pipeline.
 */
@E2ETest
@DisplayName("Schema Evolution DDL - E2E Tests")
class SchemaEvolutionTest extends PangrovaE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String FQN_PREFIX = CATALOG + "." + SCHEMA + ".";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    private String tableName;

    @BeforeEach
    void assignUniqueTableName(TestInfo testInfo) {
        String method = testInfo.getTestMethod().map(m -> m.getName()).orElse("unknown");
        tableName = "t_schema_" + method.replaceAll("[^a-zA-Z0-9]", "_") + "_" + System.nanoTime();
    }

    private String fqn() {
        return FQN_PREFIX + tableName;
    }

    // ── ADD COLUMN ──

    @Nested
    @DisplayName("ALTER TABLE ADD COLUMN")
    class AddColumn {

        @Test
        @DisplayName("ADD COLUMN adds new column to existing table")
        void addColumn() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, name VARCHAR)");
            executeDml("INSERT INTO " + fqn() + " VALUES (1, 'alice')");

            executeDdl("ALTER TABLE " + fqn() + " ADD COLUMN score BIGINT");

            List<TypedRow> rows = executeQuery("DESCRIBE " + fqn());
            assertThat(rows).hasSizeGreaterThanOrEqualTo(3);
        }

        @Test
        @DisplayName("ADD COLUMN with new data is queryable")
        void addColumnThenInsert() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, name VARCHAR)");
            executeDml("INSERT INTO " + fqn() + " VALUES (1, 'bob')");

            executeDdl("ALTER TABLE " + fqn() + " ADD COLUMN age BIGINT");
            executeDml("INSERT INTO " + fqn() + " VALUES (2, 'carol', 30)");

            List<TypedRow> rows = executeQuery("SELECT * FROM " + fqn() + " ORDER BY id");
            assertThat(rows).hasSize(2);
        }
    }

    // ── DROP COLUMN ──

    @Nested
    @DisplayName("ALTER TABLE DROP COLUMN")
    class DropColumn {

        @Test
        @DisplayName("DROP COLUMN removes column from schema")
        void dropColumn() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, name VARCHAR, temp VARCHAR)");
            executeDml("INSERT INTO " + fqn() + " VALUES (1, 'x', 'unused')");

            executeDdl("ALTER TABLE " + fqn() + " DROP COLUMN temp");

            List<TypedRow> rows = executeQuery("DESCRIBE " + fqn());
            // Should have 2 columns now
            assertThat(rows).hasSize(2);
        }
    }

    // ── RENAME COLUMN ──

    @Nested
    @DisplayName("ALTER TABLE RENAME COLUMN")
    class RenameColumn {

        @Test
        @DisplayName("RENAME COLUMN changes column name")
        void renameColumn() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, old_name VARCHAR)");
            executeDml("INSERT INTO " + fqn() + " VALUES (1, 'test')");

            executeDdl("ALTER TABLE " + fqn() + " RENAME COLUMN old_name TO new_name");

            List<TypedRow> rows = executeQuery("SELECT new_name FROM " + fqn());
            assertThat(rows).hasSize(1);
            assertThat(rows.get(0).getRaw("new_name")).isEqualTo("test");
        }
    }

    // ── CHANGE COLUMN TYPE ──

    @Nested
    @DisplayName("ALTER TABLE CHANGE COLUMN TYPE")
    class ChangeColumnType {

        @Test
        @DisplayName("Widen INT to BIGINT succeeds")
        void widenIntToBigint() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, val INT)");
            executeDml("INSERT INTO " + fqn() + " VALUES (1, 42)");

            executeDdl("ALTER TABLE " + fqn() + " ALTER COLUMN val TYPE BIGINT");

            List<TypedRow> rows = executeQuery("SELECT val FROM " + fqn());
            assertThat(rows).hasSize(1);
            assertThat(((Number) rows.get(0).getRaw("val")).longValue()).isEqualTo(42L);
        }
    }
}
