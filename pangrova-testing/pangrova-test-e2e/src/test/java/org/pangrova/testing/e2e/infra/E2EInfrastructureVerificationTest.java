package org.pangrova.testing.e2e.infra;

import org.pangrova.testing.E2ETest;
import org.pangrova.testing.e2e.PangrovaE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verification test for E2E infrastructure: proves the test cluster
 * can start, execute a trivial query, and stop without errors.
 *
 * <p>This is the "empty test can start/stop cluster" gate for Task 5.
 */
@E2ETest
class E2EInfrastructureVerificationTest extends PangrovaE2ETestBase {

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of();  // No connectors needed for infra verification
    }

    @Test
    void clusterStartsAndStops() {
        // If we reach here, the cluster is already running (SqlE2ETestBase @BeforeAll)
        assertThat(coordinatorHttpServer).isNotNull();
        assertThat(coordinatorHttpServer.getPort()).isPositive();
    }

    @Test
    void trivialSelectOneSucceeds() {
        assertQueryReturnsRows("SELECT 1", 1);
    }

    @Test
    void frameworkIsAccessible() {
        assertThat(framework).isNotNull();
        assertThat(sqlRunner).isNotNull();
    }
}
