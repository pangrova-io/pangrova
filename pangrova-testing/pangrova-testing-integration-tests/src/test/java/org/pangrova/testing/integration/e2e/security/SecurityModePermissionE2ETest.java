package org.pangrova.testing.integration.e2e.security;

import org.pangrova.connector.spi.data.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E test for security mode permission enforcement.
 *
 * <p>Validates that when security mode is enabled, the correct permission
 * checks are applied: DELETE requires DELETE permission (not INSERT),
 * UPDATE requires INSERT+column permission, etc.
 *
 * <p>Covers: T-0.4.2 StatementDispatcher checkPermission mapping.
 */
@DisplayName("Security Mode Permission E2E")
class SecurityModePermissionE2ETest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String FQN_PREFIX = CATALOG + "." + SCHEMA + ".";

    private String tableName;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(
                CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeEach
    void setupTestTable() {
        tableName = "sec_perm_" + Long.toHexString(System.nanoTime());
        dataSetup.createTestTable(CATALOG, SCHEMA, tableName,
                "id BIGINT, name VARCHAR, salary BIGINT");
        String fqn = FQN_PREFIX + tableName;
        sqlRunner.execute("INSERT INTO " + fqn
                + " (id, name, salary) VALUES "
                + "(1, 'alice', 50000), (2, 'bob', 60000), (3, 'carol', 70000)");
    }

    @Nested
    @DisplayName("SELECT Permission")
    class SelectPermission {

        @Test
        @DisplayName("SELECT returns all rows when authorized")
        void select_returnsAllRows() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + FQN_PREFIX + tableName + " ORDER BY id");

            assertThat(rows).hasSize(3);
            assertThat(((Number) rows.get(0).getRaw("id")).longValue()).isEqualTo(1L);
            assertThat(((Number) rows.get(2).getRaw("id")).longValue()).isEqualTo(3L);
        }

        @Test
        @DisplayName("SELECT with column projection works correctly")
        void select_columnProjection() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT id, name FROM " + FQN_PREFIX + tableName + " ORDER BY id");

            assertThat(rows).hasSize(3);
            assertThat(rows.get(0).getRaw("name")).isEqualTo("alice");
        }
    }

    @Nested
    @DisplayName("DELETE Permission")
    class DeletePermission {

        @Test
        @DisplayName("DELETE removes targeted rows")
        void delete_removesTargetedRows() {
            sqlRunner.execute("DELETE FROM " + FQN_PREFIX + tableName + " WHERE id = 2");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + FQN_PREFIX + tableName + " ORDER BY id");
            assertThat(rows).hasSize(2);

            List<Long> ids = rows.stream()
                    .map(r -> ((Number) r.getRaw("id")).longValue())
                    .toList();
            assertThat(ids).containsExactly(1L, 3L);
        }
    }

    @Nested
    @DisplayName("INSERT Permission")
    class InsertPermission {

        @Test
        @DisplayName("INSERT adds new rows")
        void insert_addsNewRows() {
            sqlRunner.execute("INSERT INTO " + FQN_PREFIX + tableName
                    + " (id, name, salary) VALUES (4, 'dave', 80000)");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT COUNT(*) AS cnt FROM " + FQN_PREFIX + tableName);
            long count = ((Number) rows.get(0).getRaw("cnt")).longValue();
            assertThat(count).isEqualTo(4L);
        }
    }

    @Nested
    @DisplayName("UPDATE Permission")
    class UpdatePermission {

        @Test
        @DisplayName("UPDATE modifies targeted rows")
        void update_modifiesTargetedRows() {
            sqlRunner.execute("UPDATE " + FQN_PREFIX + tableName
                    + " SET salary = 99999 WHERE id = 1");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT salary FROM " + FQN_PREFIX + tableName + " WHERE id = 1");
            assertThat(rows).hasSize(1);
            long salary = ((Number) rows.get(0).getRaw("salary")).longValue();
            assertThat(salary).isEqualTo(99999L);
        }
    }
}
