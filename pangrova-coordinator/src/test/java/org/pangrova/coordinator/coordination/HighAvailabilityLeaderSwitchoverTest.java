package org.pangrova.coordinator.coordination;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Integration test verifying HA leader switchover:
 * node-1 becomes leader → node-1 fails → node-2 detects failure and takes over.
 *
 * Uses InMemoryDistributedLease with short timeouts for fast failover.
 * No mocks — real HighAvailabilityCoordinationService instances sharing a lease.
 */
class HighAvailabilityLeaderSwitchoverTest {

    @Test
    void standbyTakesOverAfterLeaderFailure() throws Exception {
        org.pangrova.coordinator.coordination.inmemory.InMemoryDistributedLease sharedLease =
                new org.pangrova.coordinator.coordination.inmemory.InMemoryDistributedLease();
        HighAvailabilityCoordinationService.CoordinationConfig fastConfig =
                new HighAvailabilityCoordinationService.CoordinationConfig(200, 50);

        HighAvailabilityCoordinationService node1 = new HighAvailabilityCoordinationService(sharedLease, fastConfig);
        HighAvailabilityCoordinationService node2 = new HighAvailabilityCoordinationService(sharedLease, fastConfig);

        try {
            // node-1 starts first and becomes leader
            node1.start("node-1");
            assertThat(node1.getRole())
                    .isEqualTo(HighAvailabilityCoordinationService.Role.LEADER);
            assertThat(node1.getLeaderId()).isEqualTo("node-1");

            // node-2 starts as standby
            node2.start("node-2");
            assertThat(node2.getRole())
                    .isEqualTo(HighAvailabilityCoordinationService.Role.STANDBY);

            // Simulate node-1 failure: close stops lease renewal
            node1.close();

            // Wait for lease expiry (200ms) + detection (50ms) + margin
            Thread.sleep(500);

            // node-2 detected failure and took over
            assertThat(node2.getLeaderId()).isEqualTo("node-2");
            assertThat(node2.getRole())
                    .isEqualTo(HighAvailabilityCoordinationService.Role.LEADER);
            assertThat(node2.isLeader("node-2")).isTrue();
        } finally {
            node1.close();
            node2.close();
        }
    }
}
