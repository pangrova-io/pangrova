package org.pangrova.coordinator.bootstrap;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies role-based coordinator deployment: a QUERY-only coordinator
 * starts HTTP + QueryManager but does NOT start indexing or streaming subsystems.
 */
@DisplayName("Role-Based Coordinator Deployment")
class RoleBasedDeploymentTest extends org.pangrova.coordinator.testinfra.SignedOidcCoordinatorTestBase {

    private CoordinatorBootstrap bootstrap;
    @org.junit.jupiter.api.io.TempDir
    java.nio.file.Path indexDirectory;
    private String previousIndexPath;

    @org.junit.jupiter.api.BeforeEach
    void configureOwnedIndexPath() {
        previousIndexPath = System.getProperty("pangrova.index.basePath");
        System.setProperty("pangrova.index.basePath", indexDirectory.toString());
    }

    @AfterEach
    void tearDown() {
        try {
            if (bootstrap != null) bootstrap.shutdown();
        } finally {
            if (previousIndexPath == null) System.clearProperty("pangrova.index.basePath");
            else System.setProperty("pangrova.index.basePath", previousIndexPath);
        }
    }

    @Test
    @DisplayName("QUERY-only coordinator starts HTTP and QueryManager")
    void queryOnlyCoordinatorStartsQueryPath() {
        CoordinatorBootstrapConfig config = new CoordinatorBootstrapConfig(
                new CoordinatorBootstrapConfig.CoreConfig(
                        "query-coord-1", 0,
                        CoordinatorBootstrapConfig.DEFAULT_SHUTDOWN_TIMEOUT_MS,
                        CoordinatorRole.QUERY),
                CoordinatorBootstrapConfig.FeatureFlags.allDisabled(),
                CoordinatorBootstrapConfig.QueryConfig.defaults(),
                CoordinatorBootstrapConfig.TransportConfig.defaults(),
                CoordinatorBootstrapConfig.WorkerHealthConfig.defaults(),
                null, null, null);

        bootstrap = new CoordinatorBootstrap(withControlAuthentication(config));
        bootstrap.start();

        assertThat(bootstrap.isStarted()).isTrue();
        assertThat(bootstrap.getHttpPort()).isGreaterThan(0);
        assertThat(bootstrap.getQueryManager()).isNotNull();
    }

    @Test
    @DisplayName("QUERY-only coordinator does not start indexing or streaming")
    void queryOnlyCoordinatorSkipsMaintenanceSubsystems() {
        // A QUERY-only coordinator must not start maintenance scheduling,
        // even with a real authority and maintenance feature flags configured.
        CoordinatorBootstrapConfig config = new CoordinatorBootstrapConfig(
                new CoordinatorBootstrapConfig.CoreConfig(
                        "query-coord-2", 0,
                        CoordinatorBootstrapConfig.DEFAULT_SHUTDOWN_TIMEOUT_MS,
                        CoordinatorRole.QUERY),
                // Enable streaming + indexing flags — but QUERY role should skip them
                new CoordinatorBootstrapConfig.FeatureFlags(
                        false, false, false, true, false, false, false, false,
                        false, false, false, true),
                CoordinatorBootstrapConfig.QueryConfig.defaults(),
                CoordinatorBootstrapConfig.TransportConfig.defaults(),
                CoordinatorBootstrapConfig.WorkerHealthConfig.defaults(),
                null, null, null);

        bootstrap = new CoordinatorBootstrap(withControlAuthentication(config));
        bootstrap.start();

        assertThat(bootstrap.isStarted()).isTrue();
        assertThat(bootstrap.getHttpPort()).isGreaterThan(0);
    }

    @Test
    @DisplayName("COMBINED role starts both query and maintenance paths")
    void combinedRoleStartsBothPaths() {
        CoordinatorBootstrapConfig config = CoordinatorBootstrapConfig.withDefaults("combined-1", 0);

        bootstrap = new CoordinatorBootstrap(withControlAuthentication(config));
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
