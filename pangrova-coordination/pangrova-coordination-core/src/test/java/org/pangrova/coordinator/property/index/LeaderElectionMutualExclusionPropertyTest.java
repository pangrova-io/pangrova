package org.pangrova.coordinator.property.index;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.pangrova.persistence.memory.coordination.InMemoryDistributedLease;
import org.pangrova.coordinator.indexing.lifecycle.MaintenanceLeaderElection;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property P3: Leader Election Mutual Exclusion.
 *
 * <p>∀ concurrent {@code tryAcquireOrRenew()} calls with random timing,
 * at most one returns true at any point in time.</p>
 *
 * <p>Uses the production single-node lease implementation and verifies that
 * {@link MaintenanceLeaderElection} preserves the at-most-one-leader invariant
 * under concurrency.</p>
 *
 * <p><b>Validates: Requirements 4.AC1, 4.AC2, 4.AC7</b></p>
 */
class LeaderElectionMutualExclusionPropertyTest {

    /**
     * Given N concurrent coordinators calling tryAcquireOrRenew(),
     * at most one succeeds per round.
     */
    @Property(tries = 10)
    void atMostOneLeaderPerRound(
            @ForAll("coordinatorCounts") int coordinatorCount) throws Exception {

        InMemoryDistributedLease testLease = new InMemoryDistributedLease();
        List<MaintenanceLeaderElection> elections = new ArrayList<>();
        for (int i = 0; i < coordinatorCount; i++) {
            elections.add(new MaintenanceLeaderElection(testLease, "coord-" + i));
        }

        ExecutorService executor = Executors.newFixedThreadPool(coordinatorCount);
        try {
            // Run multiple rounds to stress the invariant
            for (int round = 0; round < 5; round++) {
                CountDownLatch ready = new CountDownLatch(coordinatorCount);
                CountDownLatch go = new CountDownLatch(1);

                List<Future<Boolean>> futures = new ArrayList<>();
                for (MaintenanceLeaderElection election : elections) {
                    futures.add(executor.submit(() -> {
                        ready.countDown();
                        go.await();
                        return election.tryAcquireOrRenew();
                    }));
                }

                ready.await();
                go.countDown();

                int leaderCount = 0;
                for (Future<Boolean> f : futures) {
                    if (f.get()) {
                        leaderCount++;
                    }
                }

                assertThat(leaderCount)
                        .as("Round %d: at most one leader among %d coordinators",
                                round, coordinatorCount)
                        .isLessThanOrEqualTo(1);
            }
        } finally {
            executor.shutdownNow();
        }
    }

    /**
     * A single coordinator calling tryAcquireOrRenew() repeatedly
     * always succeeds (lease renewal works).
     */
    @Property(tries = 10)
    void singleCoordinatorAlwaysRenews(
            @ForAll("renewalCounts") int renewals) {

        InMemoryDistributedLease testLease = new InMemoryDistributedLease();
        MaintenanceLeaderElection election = new MaintenanceLeaderElection(
                testLease, "sole-coord");

        for (int i = 0; i < renewals; i++) {
            boolean result = election.tryAcquireOrRenew();
            assertThat(result)
                    .as("Single coordinator must always hold lease (call %d)", i)
                    .isTrue();
        }
    }

    /**
     * After a leader is established, a different coordinator cannot
     * acquire the lease while the original holder keeps renewing.
     */
    @Property(tries = 10)
    void activeLeaderBlocksOtherAcquisitions(
            @ForAll("coordinatorCounts") int challengerCount) {

        InMemoryDistributedLease testLease = new InMemoryDistributedLease();
        MaintenanceLeaderElection leader = new MaintenanceLeaderElection(
                testLease, "leader");

        // Leader acquires first
        assertThat(leader.tryAcquireOrRenew()).isTrue();

        // Challengers try to acquire — all must fail
        for (int i = 0; i < challengerCount; i++) {
            MaintenanceLeaderElection challenger = new MaintenanceLeaderElection(
                    testLease, "challenger-" + i);
            assertThat(challenger.tryAcquireOrRenew())
                    .as("Challenger %d must not acquire while leader is active", i)
                    .isFalse();
        }

        // Leader can still renew
        assertThat(leader.tryAcquireOrRenew()).isTrue();
    }

    // ── Arbitraries ─────────────────────────────────────────────────────

    @Provide
    Arbitrary<Integer> coordinatorCounts() {
        return Arbitraries.integers().between(2, 8);
    }

    @Provide
    Arbitrary<Integer> renewalCounts() {
        return Arbitraries.integers().between(1, 20);
    }

}
