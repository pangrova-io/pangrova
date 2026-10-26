package org.pangrova.coordinator.bootstrap;

// IllegalStateException is used for the fail-fast check
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for Task #77: InMemory lease fallback detection.
 *
 * <p>When HA bootstrap falls back to InMemoryDistributedLease (no metadata datasource)
 * in a multi-node deployment, the coordinator should fail fast to prevent silent
 * brain-split where each node holds its own local lease.</p>
 *
 * <p>Multi-node is detected by:
 * <ul>
 *   <li>Non-loopback advertised host (e.g., 10.0.0.1, prod-host.example.com)</li>
 *   <li>Multiple shuffle endpoints configured</li>
 * </ul>
 */
@DisplayName("HA Lease Backend InMemory Fallback (Task #77)")
class HaLeaseBackendFallbackTest {

    @Test
    @DisplayName("Multi-node deployment with non-loopback host fails fast when no datasource")
    void multiNodeNonLoopbackHostFailsFast() {
        // Configure with a non-loopback advertised host but no metadata JDBC (no datasource)
        CoordinatorBootstrapConfig config = new CoordinatorBootstrapConfig(
                new CoordinatorBootstrapConfig.CoreConfig(
                        "coord-1", 0, CoordinatorBootstrapConfig.DEFAULT_SHUTDOWN_TIMEOUT_MS, org.pangrova.coordinator.bootstrap.CoordinatorRole.COMBINED, "0.0.0.0"),
                new CoordinatorBootstrapConfig.FeatureFlags(
                        false, false, false, false, false, false, false, false,
                        false, false, false, false),
                CoordinatorBootstrapConfig.QueryConfig.defaults(),
                new CoordinatorBootstrapConfig.TransportConfig(0, 256L * 1024 * 1024,
                        "10.0.0.5", null),  // non-loopback → multi-node
                CoordinatorBootstrapConfig.WorkerHealthConfig.defaults(),
                null, null, null);  // no metadata JDBC config → no datasource

        CoordinatorBootstrap bootstrap = new CoordinatorBootstrap(config);

        // Starting must fail fast before accepting a process-local lease in a multi-node deployment.
        assertThatThrownBy(bootstrap::start)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("distributed lease backend")
                .hasMessageContaining("advertisedHost=10.0.0.5");
    }

    @Test
    @DisplayName("Multi-node deployment with non-loopback advertised host and different coordinator ID fails fast")
    void multiNodeNonLoopbackAlternativeHostFailsFast() {
        CoordinatorBootstrapConfig config = new CoordinatorBootstrapConfig(
                new CoordinatorBootstrapConfig.CoreConfig(
                        "coord-2", 0, CoordinatorBootstrapConfig.DEFAULT_SHUTDOWN_TIMEOUT_MS, org.pangrova.coordinator.bootstrap.CoordinatorRole.COMBINED, "0.0.0.0"),
                new CoordinatorBootstrapConfig.FeatureFlags(
                        false, false, false, false, false, false, false, false,
                        false, false, false, false),
                CoordinatorBootstrapConfig.QueryConfig.defaults(),
                new CoordinatorBootstrapConfig.TransportConfig(0, 256L * 1024 * 1024,
                        "192.168.1.100", null),  // non-loopback → multi-node
                CoordinatorBootstrapConfig.WorkerHealthConfig.defaults(),
                null, null, null);  // no metadata JDBC config → no datasource

        CoordinatorBootstrap bootstrap = new CoordinatorBootstrap(config);

        assertThatThrownBy(bootstrap::start)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("distributed lease backend")
                .hasMessageContaining("advertisedHost=192.168.1.100");
    }

    @Test
    @DisplayName("Single-node deployment with loopback host selects InMemory lease")
    void singleNodeLoopbackHostUsesInMemorySuccessfully() {
        // Default config with loopback host, no metadata → single-node, should succeed
        CoordinatorBootstrapConfig config = CoordinatorBootstrapConfig.withDefaults("coord-3", 0);

        CoordinatorBootstrap bootstrap = new CoordinatorBootstrap(config);
        var lease = bootstrap.ensureDistributedLease();
        assertThat(lease).isInstanceOf(
                org.pangrova.coordinator.coordination.inmemory.InMemoryDistributedLease.class);
        assertThat(bootstrap.ensureDistributedLease()).isSameAs(lease);
    }

    @Test
    @DisplayName("Single-node localhost deployment with empty shuffle endpoints selects InMemory")
    void singleNodeEmptyShuffleEndpointsUsesInMemory() {
        CoordinatorBootstrapConfig config = new CoordinatorBootstrapConfig(
                new CoordinatorBootstrapConfig.CoreConfig(
                        "coord-4", 0, CoordinatorBootstrapConfig.DEFAULT_SHUTDOWN_TIMEOUT_MS, org.pangrova.coordinator.bootstrap.CoordinatorRole.COMBINED, "0.0.0.0"),
                new CoordinatorBootstrapConfig.FeatureFlags(
                        false, false, false, false, false, false, false, false,
                        false, false, false, false),
                CoordinatorBootstrapConfig.QueryConfig.defaults(),
                new CoordinatorBootstrapConfig.TransportConfig(0, 256L * 1024 * 1024,
                        "localhost", null),  // loopback → single-node
                CoordinatorBootstrapConfig.WorkerHealthConfig.defaults(),
                null, null, null);

        CoordinatorBootstrap bootstrap = new CoordinatorBootstrap(config);
        var lease = bootstrap.ensureDistributedLease();
        assertThat(lease).isInstanceOf(
                org.pangrova.coordinator.coordination.inmemory.InMemoryDistributedLease.class);
        assertThat(bootstrap.ensureDistributedLease()).isSameAs(lease);
    }
}
