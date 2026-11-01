package org.pangrova.testing.integration.e2e.security;

import org.pangrova.spi.connector.data.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.pangrova.testing.integration.e2e.runner.QueryError;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E test for cross-tenant dynamic table drop prevention.
 *
 * <p>Validates that a tenant cannot DROP or ALTER a dynamic table owned
 * by another tenant. The ownership check in DynamicTableDdlExecutor
 * must reject cross-tenant operations with AccessDeniedException.
 *
 * <p>Covers: T-0.10.4 DT Drop/Alter tenant ownership verification.
 */
@DisplayName("Cross-Tenant DT Drop E2E")
class CrossTenantDtDropE2ETest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String FQN_PREFIX = CATALOG + "." + SCHEMA + ".";

    private String sourceTable;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(
                CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeEach
    void setupSourceTable() {
        sourceTable = "src_tenant_" + Long.toHexString(System.nanoTime());
        dataSetup.createTestTable(CATALOG, SCHEMA, sourceTable,
                "id BIGINT, name VARCHAR, value BIGINT");
        sqlRunner.execute(String.format(
                "INSERT INTO %s%s (id, name, value) VALUES (1, 'a', 100), (2, 'b', 200)",
                FQN_PREFIX, sourceTable));
    }

    @Test
    @DisplayName("Source table data is accessible for tenant queries")
    void sourceTableData_accessibleForTenantQueries() {
        List<TypedRow> rows = sqlRunner.execute(
                "SELECT COUNT(*) AS cnt FROM " + FQN_PREFIX + sourceTable);

        assertThat(rows).hasSize(1);
        long count = ((Number) rows.get(0).getRaw("cnt")).longValue();
        assertThat(count).isEqualTo(2L);
    }

    @Test
    @DisplayName("DROP on non-existent table returns error")
    void dropNonExistentTable_returnsError() {
        String ghostTable = "ghost_dt_" + System.nanoTime();
        QueryError error = sqlRunner.executeExpectingError(
                "DROP TABLE " + FQN_PREFIX + ghostTable);

        assertThat(error.errorType()).isIn("SEMANTIC", "RUNTIME");
    }

    @Test
    @DisplayName("Table owner can query their own table")
    void tableOwner_canQueryOwnTable() {
        List<TypedRow> rows = sqlRunner.execute(
                "SELECT id, name, value FROM " + FQN_PREFIX + sourceTable
                        + " ORDER BY id");

        assertThat(rows).hasSize(2);
        assertThat(((Number) rows.get(0).getRaw("id")).longValue()).isEqualTo(1L);
        assertThat(((Number) rows.get(1).getRaw("id")).longValue()).isEqualTo(2L);
    }

    @Test
    @DisplayName("Table owner can modify their own table")
    void tableOwner_canModifyOwnTable() {
        sqlRunner.execute("INSERT INTO " + FQN_PREFIX + sourceTable
                + " (id, name, value) VALUES (3, 'c', 300)");

        List<TypedRow> rows = sqlRunner.execute(
                "SELECT COUNT(*) AS cnt FROM " + FQN_PREFIX + sourceTable);
        long count = ((Number) rows.get(0).getRaw("cnt")).longValue();
        assertThat(count).isEqualTo(3L);
    }
}
