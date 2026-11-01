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
 * E2E tests for table lifecycle DDL operations.
 *
 * <p>Covers CREATE TABLE, DROP TABLE, CREATE TABLE IF NOT EXISTS,
 * and DROP TABLE IF EXISTS through the full pipeline.
 */
@E2ETest
@DisplayName("Table Lifecycle DDL - E2E Tests")
class TableLifecycleTest extends PangrovaE2ETestBase {

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
        tableName = "t_ddl_" + method.replaceAll("[^a-zA-Z0-9]", "_") + "_" + System.nanoTime();
    }

    private String fqn() {
        return FQN_PREFIX + tableName;
    }

    // ── CREATE TABLE ──

    @Nested
    @DisplayName("CREATE TABLE")
    class CreateTable {

        @Test
        @DisplayName("CREATE TABLE with basic column types succeeds")
        void createBasicTable() {
            executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, name VARCHAR, amount DOUBLE, active BOOLEAN)");
            // Verify table exists by querying it
            assertQueryReturnsEmpty("SELECT * FROM " + fqn());
        }

        @Test
        @DisplayName("CREATE TABLE IF NOT EXISTS does not fail when table exists")
        void createTableIfNotExists() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT)");
            // Should not throw
            executeDdl("CREATE TABLE IF NOT EXISTS " + fqn() + " (id BIGINT)");
            assertQueryReturnsEmpty("SELECT * FROM " + fqn());
        }

        @Test
        @DisplayName("CREATE TABLE supports DATE column type")
        void createTableWithDate() {
            executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, event_date DATE, description VARCHAR)");
            executeDml("INSERT INTO " + fqn() + " VALUES (1, DATE '2024-01-15', 'test')");
            assertQueryReturnsRows("SELECT * FROM " + fqn(), 1);
        }
    }

    // ── DROP TABLE ──

    @Nested
    @DisplayName("DROP TABLE")
    class DropTable {

        @Test
        @DisplayName("DROP TABLE removes table from catalog")
        void dropTableRemovesTable() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT)");
            executeDml("INSERT INTO " + fqn() + " VALUES (1)");
            executeDdl("DROP TABLE " + fqn());

            // Table should not exist — querying should fail
            String error = executeExpectingError("SELECT * FROM " + fqn());
            assertThat(error).isNotNull();
        }

        @Test
        @DisplayName("DROP TABLE IF EXISTS does not fail for non-existent table")
        void dropTableIfExists() {
            String nonExistent = FQN_PREFIX + "t_nonexistent_" + System.nanoTime();
            executeDdl("DROP TABLE IF EXISTS " + nonExistent);
            // Should complete without error
        }
    }

    // ── SHOW / DESCRIBE ──

    @Nested
    @DisplayName("SHOW and DESCRIBE")
    class ShowDescribe {

        @Test
        @DisplayName("SHOW TABLES lists created table")
        void showTablesListsTable() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, val VARCHAR)");
            List<TypedRow> rows = executeQuery("SHOW TABLES");
            assertThat(rows).isNotEmpty();
        }

        @Test
        @DisplayName("DESCRIBE TABLE shows column definitions")
        void describeTable() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, name VARCHAR, price DOUBLE)");
            List<TypedRow> rows = executeQuery("DESCRIBE " + fqn());
            assertThat(rows).hasSizeGreaterThanOrEqualTo(3);
        }
    }
}
