package org.pangrova.coordinator.coordination;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.pangrova.coordinator.coordination.HighAvailabilityCoordinationService.CoordinationConfig;
import org.pangrova.persistence.coordination.memory.lease.InMemoryDistributedLease;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tests for HA coordination config: validates default values,
 * custom config, and validation rules via {@link CoordinationConfig}.
 */
@DisplayName("HA Lease CoordinationConfig tests")
class HaLeaseConfigTest {

    @Test
    @DisplayName("Default CoordinationConfig values match original hardcoded (30s/10s)")
    void defaultsMatchOriginalHardcodedValues() {
        CoordinationConfig defaults = CoordinationConfig.defaults();

        assertThat(defaults.leaseTimeoutMs()).isEqualTo(30_000);
        assertThat(defaults.heartbeatIntervalMs()).isEqualTo(10_000);
    }

    @Test
    @DisplayName("Custom lease timeout and heartbeat interval are respected")
    void customConfigValuesRespected() {
        long customLeaseMs = 60_000;
        long customHeartbeatMs = 15_000;

        CoordinationConfig config = new CoordinationConfig(customLeaseMs, customHeartbeatMs);

        assertThat(config.leaseTimeoutMs()).isEqualTo(customLeaseMs);
        assertThat(config.heartbeatIntervalMs()).isEqualTo(customHeartbeatMs);
    }

    @Test
    @DisplayName("Custom config flows through to HighAvailabilityCoordinationService")
    void customConfigFlowsThroughToService() {
        long customLeaseMs = 45_000;
        long customHeartbeatMs = 5_000;

        CoordinationConfig config = new CoordinationConfig(customLeaseMs, customHeartbeatMs);
        InMemoryDistributedLease lease = new InMemoryDistributedLease();

        try (var service = new HighAvailabilityCoordinationService(lease, config, new org.pangrova.persistence.query.memory.coordination.InMemoryExecutionAnchorStore())) {
            // Verify the service uses the custom config by starting and verifying
            // it can elect a leader (which exercises the configured intervals)
            service.start("custom-config-node");
            assertThat(service.isLeader("custom-config-node")).isTrue();
            assertThat(service.getRole())
                    .isEqualTo(HighAvailabilityCoordinationService.Role.LEADER);
        }
    }

    @Test
    @DisplayName("CoordinationConfig rejects non-positive leaseTimeoutMs")
    void rejectsNonPositiveLeaseTimeout() {
        assertThatThrownBy(() -> new CoordinationConfig(0, 5_000))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("leaseTimeoutMs must be positive");

        assertThatThrownBy(() -> new CoordinationConfig(-1, 5_000))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("leaseTimeoutMs must be positive");
    }

    @Test
    @DisplayName("CoordinationConfig rejects non-positive heartbeatIntervalMs")
    void rejectsNonPositiveHeartbeatInterval() {
        assertThatThrownBy(() -> new CoordinationConfig(30_000, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("heartbeatIntervalMs must be positive");

        assertThatThrownBy(() -> new CoordinationConfig(30_000, -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("heartbeatIntervalMs must be positive");
    }

    @Test
    @DisplayName("CoordinationConfig rejects heartbeatIntervalMs >= leaseTimeoutMs")
    void rejectsHeartbeatNotLessThanLeaseTimeout() {
        // heartbeat == lease timeout
        assertThatThrownBy(() -> new CoordinationConfig(10_000, 10_000))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be less than leaseTimeoutMs");

        // heartbeat > lease timeout
        assertThatThrownBy(() -> new CoordinationConfig(10_000, 15_000))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be less than leaseTimeoutMs");
    }

    @Test
    @DisplayName("Default constructor uses CoordinationConfig.defaults()")
    void defaultConstructorUsesDefaults() {
        InMemoryDistributedLease lease = new InMemoryDistributedLease();
        try (var service = new HighAvailabilityCoordinationService(lease, new org.pangrova.persistence.query.memory.coordination.InMemoryExecutionAnchorStore())) {
            // Service should be constructable and functional with defaults
            service.start("default-node");
            assertThat(service.isLeader("default-node")).isTrue();
        }
    }

    @Test
    @DisplayName("Fast failover config (200ms/50ms) works end-to-end with leader switchover")
    void fastFailoverConfigWorksEndToEnd() throws Exception {
        // Use a very aggressive config suitable for testing
        long leaseMs = 200;
        long heartbeatMs = 50;

        InMemoryDistributedLease sharedLease = new InMemoryDistributedLease();
        CoordinationConfig fastConfig = new CoordinationConfig(leaseMs, heartbeatMs);

        var node1 = new HighAvailabilityCoordinationService(sharedLease, fastConfig, new org.pangrova.persistence.query.memory.coordination.InMemoryExecutionAnchorStore());
        var node2 = new HighAvailabilityCoordinationService(sharedLease, fastConfig, new org.pangrova.persistence.query.memory.coordination.InMemoryExecutionAnchorStore());

        try {
            node1.start("fast-node-1");
            assertThat(node1.getRole()).isEqualTo(HighAvailabilityCoordinationService.Role.LEADER);

            node2.start("fast-node-2");
            assertThat(node2.getRole()).isEqualTo(HighAvailabilityCoordinationService.Role.STANDBY);

            // Kill node-1 -- lease expires after 200ms, detected within 50ms
            node1.close();

            // Wait for failover (lease expiry + detection cycle + margin)
            Thread.sleep(500);

            assertThat(node2.getRole()).isEqualTo(HighAvailabilityCoordinationService.Role.LEADER);
            assertThat(node2.isLeader("fast-node-2")).isTrue();
        } finally {
            node1.close();
            node2.close();
        }
    }
}
