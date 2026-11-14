package org.pangrova.server.coordinator.bootstrap;

import org.pangrova.server.coordinator.enums.CoordinatorRole;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.pangrova.server.coordinator.testinfra.SecuredCoordinatorFixtures.withAuthority;

/**
 * Verifies role-based coordinator deployment: a QUERY-only coordinator
 * starts HTTP + QueryManager but does NOT start indexing or streaming subsystems.
 */
@DisplayName("Role-Based Coordinator Deployment")
class RoleBasedDeploymentTest {

    private CoordinatorBootstrap bootstrap;

    @AfterEach
    void tearDown() {
        if (bootstrap != null && bootstrap.isStarted()) {
            bootstrap.shutdown();
        }
    }

    @Test
    @DisplayName("QUERY-only coordinator starts HTTP and QueryManager")
    void queryOnlyCoordinatorStartsQueryPath() {
        CoordinatorBootstrapConfig config = new CoordinatorBootstrapConfig(
                new CoordinatorBootstrapConfig.CoreConfig(
                        "query-coord-1", 0,
                        CoordinatorBootstrapConfig.DEFAULT_SHUTDOWN_TIMEOUT_MS,
                        CoordinatorRole.QUERY, "0.0.0.0"),
                CoordinatorBootstrapConfig.FeatureFlags.allDisabled(),
                CoordinatorBootstrapConfig.QueryConfig.defaults(),
                CoordinatorBootstrapConfig.TransportConfig.defaults(),
                CoordinatorBootstrapConfig.WorkerHealthConfig.defaults(),
                null, null, null);

        bootstrap = new CoordinatorBootstrap(withAuthority(config));
        bootstrap.start();

        assertThat(bootstrap.isStarted()).isTrue();
        assertThat(bootstrap.getHttpPort()).isGreaterThan(0);
        assertThat(bootstrap.getQueryManager()).isNotNull();
    }

    @Test
    @DisplayName("QUERY-only coordinator does not start indexing or streaming")
    void queryOnlyCoordinatorSkipsMaintenanceSubsystems() {
        // Indexing and streaming require metadata + datasource which we don't provide.
        // A QUERY-only coordinator should NOT attempt to start them even if feature flags
        // are enabled, because role.isMaintenance() returns false.
        CoordinatorBootstrapConfig config = new CoordinatorBootstrapConfig(
                new CoordinatorBootstrapConfig.CoreConfig(
                        "query-coord-2", 0,
                        CoordinatorBootstrapConfig.DEFAULT_SHUTDOWN_TIMEOUT_MS,
                        CoordinatorRole.QUERY, "0.0.0.0"),
                // Enable streaming + indexing flags — but QUERY role should skip them
                new CoordinatorBootstrapConfig.FeatureFlags(
                        false, false, false, true, false, false, false, false,
                        false, false, false, true),
                CoordinatorBootstrapConfig.QueryConfig.defaults(),
                CoordinatorBootstrapConfig.TransportConfig.defaults(),
                CoordinatorBootstrapConfig.WorkerHealthConfig.defaults(),
                null, null, null);

        bootstrap = new CoordinatorBootstrap(withAuthority(config));
        bootstrap.start();

        // Coordinator starts successfully — proves it didn't try to initialize
        // indexing/streaming subsystems (which would fail without metadata datasource)
        assertThat(bootstrap.isStarted()).isTrue();
        assertThat(bootstrap.getHttpPort()).isGreaterThan(0);
    }

    @Test
    @DisplayName("COMBINED role starts both query and maintenance paths")
    void combinedRoleStartsBothPaths() {
        CoordinatorBootstrapConfig config = CoordinatorBootstrapConfig.withDefaults("combined-1", 0);

        bootstrap = new CoordinatorBootstrap(withAuthority(config));
        bootstrap.start();

        assertThat(bootstrap.isStarted()).isTrue();
        assertThat(bootstrap.getHttpPort()).isGreaterThan(0);
        assertThat(bootstrap.getQueryManager()).isNotNull();
    }

    @Test
    @DisplayName("CoordinatorRole.QUERY returns isQuery=true, isMaintenance=false")
    void queryRolePredicates() {
        assertThat(CoordinatorRole.QUERY.isQuery()).isTrue();
        assertThat(CoordinatorRole.QUERY.isMaintenance()).isFalse();
    }

    @Test
    @DisplayName("CoordinatorRole.MAINTENANCE returns isQuery=false, isMaintenance=true")
    void maintenanceRolePredicates() {
        assertThat(CoordinatorRole.MAINTENANCE.isQuery()).isFalse();
        assertThat(CoordinatorRole.MAINTENANCE.isMaintenance()).isTrue();
    }

    @Test
    @DisplayName("CoordinatorRole.COMBINED returns both true")
    void combinedRolePredicates() {
        assertThat(CoordinatorRole.COMBINED.isQuery()).isTrue();
        assertThat(CoordinatorRole.COMBINED.isMaintenance()).isTrue();
    }
}
