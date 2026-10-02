package org.pangrova.testing.e2e.smoke;

import org.pangrova.testing.E2ETest;
import org.pangrova.testing.e2e.PangrovaE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * T14: Activation Smoke — 2 E2E scenarios.
 *
 * <p>Validates basic startup self-check and minimal query execution.
 */
@E2ETest
@DisplayName("T14: Activation Smoke")
class ActivationSmokeE2ETest extends PangrovaE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @Test
    @DisplayName("Scenario 1: Startup self-check passes")
    void startupSelfCheckPasses() {
        // Cluster started successfully — verify all components are alive
        assertThat(coordinatorHttpServer).isNotNull();
        assertThat(coordinatorHttpServer.getPort()).isPositive();
        assertThat(workerManager.getActiveWorkers()).isNotEmpty();
        assertThat(queryManager).isNotNull();
    }

    @Test
    @DisplayName("Scenario 2: Smoke query assertion")
    void smokeQueryAssertion() {
        // Basic SELECT 1 must work
        assertScalarEquals("SELECT CAST(1 AS BIGINT)", 1L);

        // DDL + DML + SELECT cycle
        String tbl = CATALOG + "." + SCHEMA + ".smoke_" + System.nanoTime();
        executeDdl("CREATE TABLE " + tbl + " (id BIGINT)");
        try {
            executeDml("INSERT INTO " + tbl + " VALUES (42)");
            assertScalarEquals("SELECT id FROM " + tbl, 42L);
        } finally {
            executeDdl("DROP TABLE " + tbl);
        }
    }
}
