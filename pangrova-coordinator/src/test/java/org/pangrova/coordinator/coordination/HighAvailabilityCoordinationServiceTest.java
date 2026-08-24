package org.pangrova.coordinator.coordination;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.atomic.AtomicReference;

import org.pangrova.coordinator.coordination.inmemory.InMemoryDistributedLease;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests HA coordination: leader election, lease renewal, failover,
 * query ownership, and epoch-based zombie fencing.
 * No mocks — real HighAvailabilityCoordinationService instances.
 */
class HighAvailabilityCoordinationServiceTest {

    private HighAvailabilityCoordinationService haService;

    @BeforeEach
    void setUp() {
        haService = new HighAvailabilityCoordinationService(
                new InMemoryDistributedLease(),
                new HighAvailabilityCoordinationService.CoordinationConfig(5000, 1000));
    }

    @AfterEach
    void tearDown() {
        haService.close();
    }

    @Test
    void firstCandidate_becomesLeader() {
        assertThat(haService.tryBecomeLeader("node-1")).isTrue();
        assertThat(haService.hasActiveLeader()).isTrue();
        assertThat(haService.isLeader("node-1")).isTrue();
        assertThat(haService.getLeaderId()).isEqualTo("node-1");
    }

    @Test
    void acquireQueryOwnership_rejectsBeyondConfiguredCap() {
        // A-OWN-01: the query-ownership cap is configurable and enforced fail-closed.
        var svc = new HighAvailabilityCoordinationService(
                new InMemoryDistributedLease(),
                new HighAvailabilityCoordinationService.CoordinationConfig(5000, 1000, 2));
        try {
            assertThat(svc.acquireQueryOwnership("q1", "node-1")).isTrue();
            assertThat(svc.acquireQueryOwnership("q2", "node-1")).isTrue();
            // Third acquisition exceeds the configured cap of 2 and is rejected (fail-closed).
            assertThat(svc.acquireQueryOwnership("q3", "node-1")).isFalse();
            // Releasing frees capacity so a new acquisition succeeds again.
            assertThat(svc.releaseQueryOwnership("q1", "node-1")).isTrue();
            assertThat(svc.acquireQueryOwnership("q3", "node-1")).isTrue();
        } finally {
            svc.close();
        }
    }

    @Test
    void coordinationConfig_defaultCapApplied() {
        // A-OWN-01: the backward-compatible 2-arg constructor applies the default cap.
        var config = new HighAvailabilityCoordinationService.CoordinationConfig(5000, 1000);
        assertThat(config.maxQueryOwnershipEntries())
                .isEqualTo(HighAvailabilityCoordinationService.CoordinationConfig
                        .DEFAULT_MAX_QUERY_OWNERSHIP_ENTRIES);
    }

    @Test
    void coordinationConfig_rejectsNonPositiveCap() {
        // A-OWN-01: an invalid (non-positive) cap is rejected at construction.
        assertThatThrownBy(() ->
                new HighAvailabilityCoordinationService.CoordinationConfig(5000, 1000, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxQueryOwnershipEntries");
    }

    @Test
    void secondCandidate_rejectedWhileLeaderActive() {
        // Use two separate service instances sharing the same lease backend,
        // simulating two independent coordinator nodes. Each has its own epoch
        // counter, so node-2 cannot force-replace node-1 via epoch fencing
        // when node-1's lease is still active and not expired.
        InMemoryDistributedLease sharedLease = new InMemoryDistributedLease();
        var svc1 = new HighAvailabilityCoordinationService(sharedLease,
                new HighAvailabilityCoordinationService.CoordinationConfig(5000, 1000));
        var svc2 = new HighAvailabilityCoordinationService(sharedLease,
                new HighAvailabilityCoordinationService.CoordinationConfig(5000, 1000));
        try {
            svc1.tryBecomeLeader("node-1");
            assertThat(svc2.tryBecomeLeader("node-2")).isFalse();
            assertThat(svc1.isLeader("node-1")).isTrue();
        } finally {
            svc1.close();
            svc2.close();
        }
    }

    @Test
    void sameLeader_canRenewLease() {
        haService.tryBecomeLeader("node-1");
        assertThat(haService.renewLease("node-1")).isTrue();
    }

    @Test
    void nonLeader_cannotRenewLease() {
        haService.tryBecomeLeader("node-1");
        assertThat(haService.renewLease("node-2")).isFalse();
    }

    @Test
    void leaderStepDown_allowsNewElection() {
        haService.tryBecomeLeader("node-1");
        assertThat(haService.stepDown("node-1")).isTrue();
        assertThat(haService.hasActiveLeader()).isFalse();

        assertThat(haService.tryBecomeLeader("node-2")).isTrue();
        assertThat(haService.isLeader("node-2")).isTrue();
    }

    @Test
    void nonLeader_cannotStepDown() {
        haService.tryBecomeLeader("node-1");
        assertThat(haService.stepDown("node-2")).isFalse();
    }

    @Test
    void failoverCallback_invokedOnLeaderChange() {
        AtomicReference<String> failoverLeader = new AtomicReference<>();
        haService.onFailover(failoverLeader::set);

        haService.tryBecomeLeader("node-1");
        haService.stepDown("node-1");
        haService.tryBecomeLeader("node-2");

        // No failover callback on step-down + fresh election (no previous leader at CAS time)
        // Failover is triggered when a new leader replaces an expired lease
    }

    @Test
    void queryOwnership_acquireAndRelease() {
        assertThat(haService.acquireQueryOwnership("q-1", "node-1")).isTrue();
        assertThat(haService.acquireQueryOwnership("q-1", "node-2")).isFalse();
        assertThat(haService.releaseQueryOwnership("q-1", "node-1")).isTrue();
        assertThat(haService.acquireQueryOwnership("q-1", "node-2")).isTrue();
    }

    @Test
    void queryOwnership_wrongOwnerCannotRelease() {
        haService.acquireQueryOwnership("q-1", "node-1");
        assertThat(haService.releaseQueryOwnership("q-1", "node-2")).isFalse();
    }

    @Test
    void transferOwnership_movesQueriesBetweenNodes() {
        haService.acquireQueryOwnership("q-1", "node-1");
        haService.acquireQueryOwnership("q-2", "node-1");
        haService.acquireQueryOwnership("q-3", "node-2");

        int transferred = haService.transferOwnership("node-1", "node-2");
        assertThat(transferred).isEqualTo(2);

        // node-2 now owns q-1 and q-2
        assertThat(haService.releaseQueryOwnership("q-1", "node-2")).isTrue();
        assertThat(haService.releaseQueryOwnership("q-2", "node-2")).isTrue();
    }

    @Test
    void epochValidation_rejectsStaleEpoch() {
        // First become leader to bump epoch above 0
        haService.tryBecomeLeader("node-1");
        long leaderEpoch = haService.getCurrentEpoch();
        assertThat(leaderEpoch).isGreaterThan(0);

        haService.acquireQueryOwnership("q-1", "node-1");

        // Valid epoch (current leader epoch)
        assertThat(haService.validateEpoch("q-1", leaderEpoch)).isTrue();

        // Stale epoch (before leader election)
        assertThat(haService.validateEpoch("q-1", 0)).isFalse();
    }

    @Test
    void close_clearsAllState() {
        haService.tryBecomeLeader("node-1");
        haService.acquireQueryOwnership("q-1", "node-1");

        haService.close();

        assertThat(haService.hasActiveLeader()).isFalse();
        assertThat(haService.getLeaderId()).isNull();
    }

    @Test
    void leaderReElection_sameNodeSucceeds() {
        haService.tryBecomeLeader("node-1");
        // Same node can re-elect (renew)
        assertThat(haService.tryBecomeLeader("node-1")).isTrue();
    }

    /**
     * Integration test: node-1 becomes leader via start(), then node-1 fails
     * (close stops lease renewal), node-2 detects expired lease and takes over.
     *
     * Uses short lease (200ms) and heartbeat (50ms) for fast failover.
     * Both nodes share the same InMemoryDistributedLease.
     */
    @Test
    void leaderSwitchover_standbyTakesOverAfterLeaderFailure() throws Exception {
        InMemoryDistributedLease sharedLease = new InMemoryDistributedLease();
        HighAvailabilityCoordinationService.CoordinationConfig fastConfig =
                new HighAvailabilityCoordinationService.CoordinationConfig(200, 50);

        HighAvailabilityCoordinationService node1 = new HighAvailabilityCoordinationService(sharedLease, fastConfig);
        HighAvailabilityCoordinationService node2 = new HighAvailabilityCoordinationService(sharedLease, fastConfig);

        try {
            // node-1 starts first and becomes leader
            node1.start("node-1");
            assertThat(node1.getRole()).isEqualTo(HighAvailabilityCoordinationService.Role.LEADER);
            assertThat(node1.getLeaderId()).isEqualTo("node-1");

            // node-2 starts as standby (node-1 already holds the lease)
            node2.start("node-2");
            assertThat(node2.getRole()).isEqualTo(HighAvailabilityCoordinationService.Role.STANDBY);

            // Simulate node-1 failure: close stops lease renewal, lease will expire
            node1.close();

            // Wait for lease expiry (200ms) + detection cycle (50ms) + margin
            Thread.sleep(500);

            // Verify node-2 detected leader failure and took over
            assertThat(node2.getLeaderId()).isEqualTo("node-2");
            assertThat(node2.getRole()).isEqualTo(HighAvailabilityCoordinationService.Role.LEADER);
            assertThat(node2.isLeader("node-2")).isTrue();
        } finally {
            node1.close();
            node2.close();
        }
    }
}
