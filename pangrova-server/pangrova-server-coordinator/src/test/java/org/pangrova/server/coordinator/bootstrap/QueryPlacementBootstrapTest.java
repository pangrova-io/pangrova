package org.pangrova.server.coordinator.bootstrap;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.pangrova.server.coordinator.enums.CoordinatorDeploymentRole;
import org.pangrova.server.coordinator.testinfra.SignedOidcCoordinatorTestBase;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Tag("integration")
class QueryPlacementBootstrapTest extends SignedOidcCoordinatorTestBase {
    @Test
    void securedQueryBootstrapStartsWithoutTheRetiredDispatcher() {
        assertThat(CoordinatorBootstrap.class.getClassLoader().getResource(
                "org/pangrova/scheduler/resource/WeightedFairScheduler.class")).isNull();
        var running = new CoordinatorBootstrap(tenantConfig(CoordinatorBootstrapConfig.QueryConfig.defaults()));
        try {
            running.start();
            assertThat(running.isStarted()).isTrue();
        } finally {
            running.shutdown();
        }
        assertThat(running.isStarted()).isFalse();
    }

    @Test
    void invalidCleanupConfigurationLeavesTheBootstrapStopped() {
        var invalidCleanup = new CoordinatorBootstrapConfig.QueryConfig(300, 10, 0, 600, 0);
        var failed = new CoordinatorBootstrap(tenantConfig(invalidCleanup));
        try {
            assertThatThrownBy(failed::start).isInstanceOf(IllegalArgumentException.class);
            assertThat(failed.isStarted()).isFalse();
        } finally {
            failed.shutdown();
        }
    }

    private CoordinatorBootstrapConfig tenantConfig(CoordinatorBootstrapConfig.QueryConfig query) {
        return withControlAuthentication(new CoordinatorBootstrapConfig(
                new CoordinatorBootstrapConfig.CoreConfig(
                        "tenant-placement-lifecycle", 0, 30_000, CoordinatorDeploymentRole.COMBINED, "0.0.0.0"),
                new CoordinatorBootstrapConfig.FeatureFlags(false, true, false, false, false, false, true, false),
                query, CoordinatorBootstrapConfig.TransportConfig.defaults(),
                CoordinatorBootstrapConfig.WorkerHealthConfig.defaults(), null, null, null));
    }
}
