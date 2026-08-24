package org.pangrova.coordinator.bootstrap;

import static org.assertj.core.api.Assertions.assertThat;

import org.pangrova.coordinator.coordination.HighAvailabilityCoordinationService;
import org.pangrova.coordinator.coordination.inmemory.InMemoryDistributedLease;
import org.junit.jupiter.api.Test;

/**
 * Regression for F-CHAIN-HA-01 / F-COORD-HA-03: journal recovery must be gated on actually-held
 * leadership. {@code HighAvailabilityBootstrap.startHA()} only calls {@code recoverJournalState()}
 * when {@code haService.isLeader(coordinatorId)} is true. This test pins the predicate that gate
 * relies on: a second coordinator starting while the first holds leadership is NOT leader, so it
 * skips recovery and cannot replay+purge the live leader's in-flight queries from the shared journal.
 */
class JournalRecoveryLeadershipGateTest {

    @Test
    void startingStandbyIsNotLeaderSoRecoveryIsSkipped() {
        var sharedLease = new InMemoryDistributedLease();
        var leader = new HighAvailabilityCoordinationService(sharedLease);
        var standby = new HighAvailabilityCoordinationService(sharedLease);
        try {
            leader.onFailover(id -> { });
            standby.onFailover(id -> { });

            // First node wins leadership → its bootstrap WOULD recover.
            leader.start("leader-1");
            assertThat(leader.isLeader("leader-1")).isTrue();

            // Second node starts while leader-1 holds the lease → STANDBY.
            standby.start("standby-2");
            assertThat(standby.isLeader("standby-2"))
                    .as("a starting standby must not be leader, so journal recovery is gated off")
                    .isFalse();
        } finally {
            leader.close();
            standby.close();
        }
    }
}
