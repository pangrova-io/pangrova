package org.pangrova.testing.integration.e2e.dynamictable;

import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.types.value.TypedRow;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end test for Dynamic Table multi-tenant isolation.
 *
 * <p>Verifies that two tenants can each create Dynamic Tables with
 * isolated scheduling queues and checkpoint data. Tenant A's DT
 * operations must not affect Tenant B's DT state or data.</p>
 *
 * <p>Validates: Requirement R9</p>
 */
@DisplayName("Dynamic Table Multi-Tenant E2E")
class DynamicTableMultiTenantE2ETest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String SOURCE_A = CATALOG + "." + SCHEMA + ".dt_mt_source_a";
    private static final String SOURCE_B = CATALOG + "." + SCHEMA + ".dt_mt_source_b";
    private static final String DT_A = CATALOG + "." + SCHEMA + ".dt_mt_tenant_a";
    private static final String DT_B = CATALOG + "." + SCHEMA + ".dt_mt_tenant_b";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeAll
    static void prepareSourceTables() {
        isDockerReady();
        registerTestConnectors(List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString())));
        try { sqlRunner.execute("DROP TABLE IF EXISTS " + SOURCE_A); } catch (Exception ignored) { }
        try { sqlRunner.execute("DROP TABLE IF EXISTS " + SOURCE_B); } catch (Exception ignored) { }
        sqlRunner.execute("CREATE TABLE " + SOURCE_A + " (id INT, data VARCHAR(100))");
        sqlRunner.execute("CREATE TABLE " + SOURCE_B + " (id INT, data VARCHAR(100))");
    }

    @BeforeEach
    void cleanupBeforeEachTest() {
        ensureDynamicTableDropped(DT_A);
        ensureDynamicTableDropped(DT_B);
        try {
            sqlRunner.execute("DROP TABLE IF EXISTS " + SOURCE_A);
        } catch (Exception ignored) {
        }
        sqlRunner.execute("CREATE TABLE " + SOURCE_A + " (id INT, data VARCHAR(100))");
        try {
            sqlRunner.execute("DROP TABLE IF EXISTS " + SOURCE_B);
        } catch (Exception ignored) {
        }
        sqlRunner.execute("CREATE TABLE " + SOURCE_B + " (id INT, data VARCHAR(100))");
    }

    @Test
    @DisplayName("Two tenants create DTs with isolated data and scheduling")
    void tenantIsolation() {
        // Tenant A seeds and creates DT
        sqlRunner.execute("INSERT INTO " + SOURCE_A + " VALUES (1, 'tenant_a_data')");
        sqlRunner.execute("CREATE DYNAMIC TABLE " + DT_A
                + " TARGET_LAG = 'PT5M' AS SELECT * FROM " + SOURCE_A);

        // Tenant B seeds and creates DT
        sqlRunner.execute("INSERT INTO " + SOURCE_B + " VALUES (1, 'tenant_b_data')");
        sqlRunner.execute("CREATE DYNAMIC TABLE " + DT_B
                + " TARGET_LAG = 'PT5M' AS SELECT * FROM " + SOURCE_B);

        // Verify data isolation: Tenant A's DT contains only Tenant A's data
        List<TypedRow> rowsA = sqlRunner.execute("SELECT * FROM " + DT_A);
        assertThat(rowsA).hasSize(1);
        assertThat(rowsA.get(0).getRaw("data")).isEqualTo("tenant_a_data");

        // Verify data isolation: Tenant B's DT contains only Tenant B's data
        List<TypedRow> rowsB = sqlRunner.execute("SELECT * FROM " + DT_B);
        assertThat(rowsB).hasSize(1);
        assertThat(rowsB.get(0).getRaw("data")).isEqualTo("tenant_b_data");

        // Suspend Tenant A's DT — should not affect Tenant B
        sqlRunner.execute("ALTER DYNAMIC TABLE " + DT_A + " SUSPEND");

        // Insert new data for Tenant B and verify refresh still works
        sqlRunner.execute("INSERT INTO " + SOURCE_B + " VALUES (2, 'tenant_b_new')");
        sqlRunner.executeDdl("ALTER DYNAMIC TABLE " + DT_B + " REFRESH");
        List<TypedRow> rowsBAfter = sqlRunner.execute("SELECT * FROM " + DT_B);
        assertThat(rowsBAfter).hasSize(2);
    }
}
