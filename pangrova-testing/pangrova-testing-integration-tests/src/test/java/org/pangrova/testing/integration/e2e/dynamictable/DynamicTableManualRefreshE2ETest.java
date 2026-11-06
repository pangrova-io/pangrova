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
 * End-to-end test for Dynamic Table manual refresh trigger.
 *
 * <p>Verifies that ALTER DYNAMIC TABLE ... REFRESH triggers an immediate
 * refresh cycle outside the normal scheduled interval.</p>
 *
 * <p>Validates: Requirement R22</p>
 */
@DisplayName("Dynamic Table Manual Refresh E2E")
class DynamicTableManualRefreshE2ETest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String SOURCE_TABLE = CATALOG + "." + SCHEMA + ".dt_manual_source";
    private static final String DT_FULL = CATALOG + "." + SCHEMA + ".dt_manual_target";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeAll
    static void prepareSourceTable() {
        isDockerReady();
        registerTestConnectors(List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString())));
        try { sqlRunner.execute("DROP TABLE IF EXISTS " + SOURCE_TABLE); } catch (Exception ignored) { }
        sqlRunner.execute("CREATE TABLE " + SOURCE_TABLE
                + " (id INT, name VARCHAR(100))");
    }

    @BeforeEach
    void cleanupBeforeEachTest() {
        ensureDynamicTableDropped(DT_FULL);
        try {
            sqlRunner.execute("DROP TABLE IF EXISTS " + SOURCE_TABLE);
        } catch (Exception ignored) {
        }
        sqlRunner.execute("CREATE TABLE " + SOURCE_TABLE
                + " (id INT, name VARCHAR(100))");
    }

    @Test
    @DisplayName("ALTER DYNAMIC TABLE REFRESH triggers immediate refresh with pending data")
    void manualRefreshTrigger() {
        // Create DT with a long target lag so scheduled refresh won't fire
        sqlRunner.execute("INSERT INTO " + SOURCE_TABLE + " VALUES (1, 'Alice')");
        sqlRunner.execute("CREATE DYNAMIC TABLE " + DT_FULL
                + " TARGET_LAG = 'PT1H' AS SELECT * FROM " + SOURCE_TABLE);

        // Insert new data
        sqlRunner.execute("INSERT INTO " + SOURCE_TABLE + " VALUES (2, 'Bob')");

        // Trigger manual refresh
        sqlRunner.execute("ALTER DYNAMIC TABLE " + DT_FULL + " REFRESH");

        // Verify manual refresh picked up the new data
        List<TypedRow> rows = sqlRunner.execute("SELECT * FROM " + DT_FULL);
        assertThat(rows).hasSize(2);
        assertThat(rows).extracting(r -> r.getRaw("name"))
                .containsExactlyInAnyOrder("Alice", "Bob");
    }
}
