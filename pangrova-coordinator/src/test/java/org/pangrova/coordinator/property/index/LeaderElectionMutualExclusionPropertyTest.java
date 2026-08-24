package org.pangrova.coordinator.property.index;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.pangrova.coordinator.index.lifecycle.MaintenanceLeaderElection;
import org.pangrova.spi.coordination.DistributedLease;
import org.pangrova.spi.coordination.LeaseEntry;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property P3: Leader Election Mutual Exclusion.
 *
 * <p>∀ concurrent {@code tryAcquireOrRenew()} calls with random timing,
 * at most one returns true at any point in time.</p>
 *
 * <p>Uses a test {@link DistributedLease} implementation backed by
 * {@link AtomicReference} to enforce mutual exclusion at the SPI level,
 * then verifies that {@link MaintenanceLeaderElection} correctly delegates
 * and preserves the at-most-one-leader invariant under concurrency.</p>
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

        AtomicReferenceLease testLease = new AtomicReferenceLease();
        List<MaintenanceLeaderElection> elections = new ArrayList<>();
        for (int i = 0; i < coordinatorCount; i++) {
            elections.add(new MaintenanceLeaderElection(
                    testLease, "coord-" + i, new AtomicLong(0)));
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

        AtomicReferenceLease testLease = new AtomicReferenceLease();
        MaintenanceLeaderElection election = new MaintenanceLeaderElection(
                testLease, "sole-coord", new AtomicLong(0));

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

        AtomicReferenceLease testLease = new AtomicReferenceLease();
        MaintenanceLeaderElection leader = new MaintenanceLeaderElection(
                testLease, "leader", new AtomicLong(0));

        // Leader acquires first
        assertThat(leader.tryAcquireOrRenew()).isTrue();

        // Challengers try to acquire — all must fail
        for (int i = 0; i < challengerCount; i++) {
            MaintenanceLeaderElection challenger = new MaintenanceLeaderElection(
                    testLease, "challenger-" + i, new AtomicLong(0));
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

    // ── Test DistributedLease implementation ─────────────────────────────

    /**
     * Thread-safe in-memory lease using {@link AtomicReference} for
     * mutual exclusion. Simulates the DistributedLease SPI contract
     * without requiring a database.
     */
    static final class AtomicReferenceLease implements DistributedLease {

        private final ConcurrentHashMap<String, LeaseEntry> leases =
                new ConcurrentHashMap<>();

        @Override
        public synchronized boolean tryAcquire(String leaseKey, String holderId,
                                  long epoch, Duration duration) {
            Instant now = Instant.now();
            Instant expiresAt = now.plus(duration);
            LeaseEntry newEntry = new LeaseEntry(
                    leaseKey, holderId, epoch, now, expiresAt);

            // CAS: insert if absent
            LeaseEntry existing = leases.putIfAbsent(leaseKey, newEntry);
            if (existing == null) {
                return true;
            }

            // Epoch fencing: higher epoch can force-replace only if expired
            // (non-expired lease = active holder, must not be preempted)
            if (existing.isExpired() && epoch > existing.epoch()) {
                return leases.replace(leaseKey, existing, newEntry);
            }

            return false;
        }

        @Override
        public synchronized boolean tryRenew(String leaseKey, String holderId,
                                Duration duration) {
            LeaseEntry existing = leases.get(leaseKey);
            if (existing == null) {
                return false;
            }
            if (!existing.holderId().equals(holderId)) {
                return false;
            }
            Instant now = Instant.now();
            LeaseEntry renewed = new LeaseEntry(
                    leaseKey, holderId, existing.epoch(), existing.acquiredAt(),
                    now.plus(duration));
            return leases.replace(leaseKey, existing, renewed);
        }

        @Override
        public boolean release(String leaseKey, String holderId) {
            LeaseEntry existing = leases.get(leaseKey);
            if (existing == null) {
                return false;
            }
            if (!existing.holderId().equals(holderId)) {
                return false;
            }
            return leases.remove(leaseKey, existing);
        }

        @Override
        public Optional<LeaseEntry> currentHolder(String leaseKey) {
            LeaseEntry entry = leases.get(leaseKey);
            if (entry == null || entry.isExpired()) {
                return Optional.empty();
            }
            return Optional.of(entry);
        }

        @Override
        public int expireStale() {
            int removed = 0;
            for (var it = leases.entrySet().iterator(); it.hasNext(); ) {
                var e = it.next();
                if (e.getValue().isExpired()) {
                    it.remove();
                    removed++;
                }
            }
            return removed;
        }
    }
}
