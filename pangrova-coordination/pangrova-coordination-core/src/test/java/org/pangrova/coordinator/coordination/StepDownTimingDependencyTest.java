package org.pangrova.coordinator.coordination;

import org.pangrova.persistence.memory.coordination.InMemoryDistributedLease;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies active-active control demotion: callbacks fire and the singleton
 * role transitions while local query ownership remains intact.
 */
class StepDownTimingDependencyTest {

    private HighAvailabilityCoordinationService haService;

    @AfterEach
    void tearDown() {
        if (haService != null) {
            haService.close();
        }
    }

    @Test
    void stepDown_demotionCallbackFiresWithoutClearingQueryOwnership() {
        haService = new HighAvailabilityCoordinationService(
                new InMemoryDistributedLease(),
                new HighAvailabilityCoordinationService.CoordinationConfig(5000, 1000));

        haService.tryBecomeLeader("node-1");
        haService.acquireQueryOwnership("q-1", "node-1");
        haService.acquireQueryOwnership("q-2", "node-1");

        AtomicBoolean demotionFired = new AtomicBoolean(false);
        haService.onDemotion(() -> demotionFired.set(true));

        boolean released = haService.stepDown("node-1");

        assertThat(released).isTrue();
        assertThat(demotionFired.get()).isTrue();
        assertThat(haService.getRole())
                .isEqualTo(HighAvailabilityCoordinationService.Role.STANDBY);
        assertThat(haService.releaseQueryOwnership("q-1", "node-1")).isTrue();
        assertThat(haService.releaseQueryOwnership("q-2", "node-1")).isTrue();
    }

    @Test
    void stepDown_roleTransitionsToStandby() {
        haService = new HighAvailabilityCoordinationService(
                new InMemoryDistributedLease(),
                new HighAvailabilityCoordinationService.CoordinationConfig(5000, 1000));

        haService.tryBecomeLeader("node-1");
        assertThat(haService.getRole())
                .isEqualTo(HighAvailabilityCoordinationService.Role.LEADER);

        haService.stepDown("node-1");

        assertThat(haService.getRole())
                .isEqualTo(HighAvailabilityCoordinationService.Role.STANDBY);
    }

    @Test
    void stepDown_newLeaderCanAcquireAfterStepDown() {
        InMemoryDistributedLease sharedLease = new InMemoryDistributedLease();
        var config = new HighAvailabilityCoordinationService.CoordinationConfig(5000, 1000);

        var node1 = new HighAvailabilityCoordinationService(sharedLease, config);
        var node2 = new HighAvailabilityCoordinationService(sharedLease, config);
        try {
            node1.tryBecomeLeader("node-1");
            assertThat(node1.isLeader("node-1")).isTrue();

            node1.stepDown("node-1");

            // After step-down, node-2 can become leader
            assertThat(node2.tryBecomeLeader("node-2")).isTrue();
            assertThat(node2.isLeader("node-2")).isTrue();
        } finally {
            node1.close();
            node2.close();
        }
    }

    @Test
    void stepDown_nonLeaderCannotStepDown() {
        haService = new HighAvailabilityCoordinationService(
                new InMemoryDistributedLease(),
                new HighAvailabilityCoordinationService.CoordinationConfig(5000, 1000));

        haService.tryBecomeLeader("node-1");
        assertThat(haService.stepDown("node-2")).isFalse();
        // Role should remain LEADER since node-2 was not the leader
    }
}
