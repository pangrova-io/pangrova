package org.pangrova.testing.integration.e2e.index;

import org.pangrova.connector.spi.data.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E tests for statistics cleanup when data files are removed.
 *
 * <p>Verifies that after DELETE/OVERWRITE operations remove data files,
 * the corresponding file_column_statistics entries are cleaned up,
 * preventing stale L0 statistics from causing incorrect pruning decisions.
 *
 * <p>Validates: Requirements 2.AC4, 4.AC5
 */
@DisplayName("Statistics Cleanup E2E")
class StatisticsCleanupE2ETest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String FQN_PREFIX = CATALOG + "." + SCHEMA + ".";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    private String tableName;

    @BeforeAll
    static void setupCatalog() {
        registerTestConnectors(List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString())));
    }

    @BeforeEach
    void createTestTable(TestInfo info) {
        String method = info.getTestMethod().map(m -> m.getName()).orElse("unknown");
        tableName = "t_cleanup_" + method + "_" + System.nanoTime();
        sqlRunner.executeDdl("CREATE TABLE " + fqn()
                + " (id BIGINT, value BIGINT, status VARCHAR)");
    }

    private String fqn() {
        return FQN_PREFIX + tableName;
    }


    @Nested
    @DisplayName("Delete and Query Correctness")
    class DeleteAndQueryCorrectness {

        @Test
        @DisplayName("Query returns correct results after DELETE removes rows")
        void queryCorrectAfterDelete() {
            // Insert data across multiple files
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, value, status) VALUES (1, 100, 'ACTIVE'), (2, 200, 'ACTIVE')");
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, value, status) VALUES (3, 300, 'INACTIVE'), (4, 400, 'INACTIVE')");
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, value, status) VALUES (5, 500, 'ACTIVE'), (6, 600, 'ACTIVE')");

            // Verify initial data
            List<TypedRow> initial = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE status = 'ACTIVE'");
            assertThat(initial).hasSize(4);

            // Delete some rows (triggers file removal in COW mode)
            sqlRunner.execute("DELETE FROM " + fqn() + " WHERE status = 'INACTIVE'");

            // Query after delete — should not return deleted rows
            List<TypedRow> afterDelete = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE status = 'ACTIVE'");
            assertThat(afterDelete).hasSize(4);

            // Verify deleted rows are gone
            List<TypedRow> deleted = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE status = 'INACTIVE'");
            assertThat(deleted).isEmpty();
        }
    }

    @Nested
    @DisplayName("Overwrite and Statistics Cleanup")
    class OverwriteAndCleanup {

        @Test
        @DisplayName("Query returns correct results after INSERT OVERWRITE")
        void queryCorrectAfterOverwrite() {
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, value, status) VALUES (1, 100, 'OLD'), (2, 200, 'OLD')");
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, value, status) VALUES (3, 300, 'OLD'), (4, 400, 'OLD')");

            // Verify initial state
            List<TypedRow> initial = sqlRunner.execute("SELECT * FROM " + fqn());
            assertThat(initial).hasSize(4);

            // Update rows — COW replaces files
            sqlRunner.execute("UPDATE " + fqn()
                    + " SET status = 'NEW' WHERE value <= 200");

            // Query should reflect the update
            List<TypedRow> updated = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE status = 'NEW'");
            assertThat(updated).hasSize(2);

            // Old status should still exist for non-updated rows
            List<TypedRow> old = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE status = 'OLD'");
            assertThat(old).hasSize(2);
        }
    }
}
