package org.pangrova.persistence.coordination.postgresql.lease;


import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import org.pangrova.persistence.coordination.api.lease.DistributedLease;
import org.pangrova.coordination.api.lease.LeaseEntry;
import org.pangrova.testing.testkit.ContainerTestBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Integration test for {@link JdbcDistributedLease} using TestContainers PostgreSQL.
 *
 * <p>Validates distributed mutual exclusion: two coordinators competing for the
 * same lease key, only one holds the lease at any time. Also validates heartbeat
 * renewal, graceful release, and automatic expiry on crash.</p>
 *
 * <p>Validates: Requirements B.1</p>
 */
@DisplayName("JdbcDistributedLease PostgreSQL Integration")
class JdbcDistributedLeaseIntegrationTest extends ContainerTestBase {

    private DataSource dataSource;

    @BeforeEach
    void setUp() throws Exception {
        dataSource = org.pangrova.testing.testkit.postgres.MigratedPostgres.create();
    }

    @Test
    @DisplayName("Single coordinator acquires lease successfully")
    void singleCoordinatorAcquiresLease() {
        DistributedLease lease = new JdbcDistributedLease(dataSource);

        Optional<LeaseEntry> acquired = lease.tryAcquire(
                "leader", "coord-1", Duration.ofSeconds(30));

        assertThat(acquired).isPresent();
        Optional<LeaseEntry> holder = lease.currentHolder("leader");
        assertThat(holder).isPresent();
        assertThat(holder.get().holderId()).isEqualTo("coord-1");
        assertThat(holder.get().epoch()).isEqualTo(1);
    }

    @Test
    @DisplayName("Two coordinators: only one holds lease at a time")
    void twoCoordinatorsOnlyOneHoldsLease() {
        DistributedLease lease1 = new JdbcDistributedLease(dataSource);
        DistributedLease lease2 = new JdbcDistributedLease(dataSource);

        Optional<LeaseEntry> acquired1 = lease1.tryAcquire(
                "leader", "coord-1", Duration.ofSeconds(30));
        Optional<LeaseEntry> acquired2 = lease2.tryAcquire(
                "leader", "coord-2", Duration.ofSeconds(30));

        assertThat(acquired1).isPresent();
        assertThat(acquired2).isEmpty();

        Optional<LeaseEntry> holder = lease1.currentHolder("leader");
        assertThat(holder).isPresent();
        assertThat(holder.get().holderId()).isEqualTo("coord-1");
    }

    @Test
    @DisplayName("Heartbeat renewal extends lease expiry")
    void heartbeatRenewalExtendsExpiry() {
        DistributedLease lease = new JdbcDistributedLease(dataSource);
        LeaseEntry held = lease.tryAcquire(
                "leader", "coord-1", Duration.ofSeconds(10)).orElseThrow();

        boolean renewed = lease.tryRenew(
                "leader", "coord-1", held.epoch(), Duration.ofSeconds(30));

        assertThat(renewed).isTrue();
        Optional<LeaseEntry> holder = lease.currentHolder("leader");
        assertThat(holder).isPresent();
        assertThat(holder.get().holderId()).isEqualTo("coord-1");
    }

    @Test
    @DisplayName("Non-holder cannot renew lease")
    void nonHolderCannotRenew() {
        DistributedLease lease = new JdbcDistributedLease(dataSource);
        LeaseEntry held = lease.tryAcquire(
                "leader", "coord-1", Duration.ofSeconds(30)).orElseThrow();

        boolean renewed = lease.tryRenew(
                "leader", "coord-2", held.epoch(), Duration.ofSeconds(30));

        assertThat(renewed).isFalse();
    }

    @Test
    @DisplayName("Graceful release allows another coordinator to acquire")
    void gracefulReleaseAllowsAcquisition() {
        DistributedLease lease1 = new JdbcDistributedLease(dataSource);
        DistributedLease lease2 = new JdbcDistributedLease(dataSource);

        LeaseEntry first = lease1.tryAcquire(
                "leader", "coord-1", Duration.ofSeconds(30)).orElseThrow();
        boolean released = lease1.release("leader", "coord-1", first.epoch());
        assertThat(released).isTrue();

        LeaseEntry acquired2 = lease2.tryAcquire(
                "leader", "coord-2", Duration.ofSeconds(30)).orElseThrow();
        assertThat(acquired2.epoch()).isEqualTo(first.epoch() + 1);

        Optional<LeaseEntry> holder = lease2.currentHolder("leader");
        assertThat(holder).isPresent();
        assertThat(holder.get().holderId()).isEqualTo("coord-2");
    }

    @Test
    @DisplayName("Expired lease allows another coordinator to acquire (crash simulation)")
    void expiredLeaseAllowsAcquisition() throws Exception {
        DistributedLease lease1 = new JdbcDistributedLease(dataSource);
        DistributedLease lease2 = new JdbcDistributedLease(dataSource);

        // Acquire with very short TTL to simulate crash (no renewal)
        LeaseEntry first = lease1.tryAcquire(
                "leader", "coord-1", Duration.ofSeconds(2)).orElseThrow();

        // Wait for expiry
        await().atMost(5, SECONDS).until(() -> lease1.currentHolder("leader").isEmpty());

        // Second coordinator can now acquire
        LeaseEntry acquired2 = lease2.tryAcquire(
                "leader", "coord-2", Duration.ofSeconds(30)).orElseThrow();
        assertThat(acquired2.epoch()).isEqualTo(first.epoch() + 1);

        Optional<LeaseEntry> holder = lease2.currentHolder("leader");
        assertThat(holder).isPresent();
        assertThat(holder.get().holderId()).isEqualTo("coord-2");
    }

    @Test
    @DisplayName("Active lease cannot be preempted by a contender")
    void activeLeaseCannotBePreempted() {
        DistributedLease lease1 = new JdbcDistributedLease(dataSource);
        DistributedLease lease2 = new JdbcDistributedLease(dataSource);

        LeaseEntry first = lease1.tryAcquire(
                "leader", "coord-1", Duration.ofSeconds(30)).orElseThrow();
        assertThat(lease2.tryAcquire("leader", "coord-2", Duration.ofSeconds(30)))
                .isEmpty();

        Optional<LeaseEntry> holder = lease2.currentHolder("leader");
        assertThat(holder).contains(first);
    }

    @Test
    @DisplayName("Concurrent acquisition: exactly one winner")
    void concurrentAcquisitionExactlyOneWinner() throws Exception {
        int coordinatorCount = 5;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(coordinatorCount);
        AtomicInteger winnerCount = new AtomicInteger(0);

        ExecutorService executor = Executors.newFixedThreadPool(coordinatorCount);
        for (int i = 0; i < coordinatorCount; i++) {
            String coordId = "coord-" + i;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    DistributedLease lease = new JdbcDistributedLease(dataSource);
                    if (lease.tryAcquire(
                            "leader", coordId, Duration.ofSeconds(30)).isPresent()) {
                        winnerCount.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertThat(doneLatch.await(10, TimeUnit.SECONDS)).isTrue();
        executor.shutdown();

        assertThat(winnerCount.get()).isEqualTo(1);
    }

    @Test
    @DisplayName("expired lease retains its epoch high-water mark")
    void expiredLeaseRetainsEpochHighWatermark() {
        DistributedLease lease = new JdbcDistributedLease(dataSource);

        LeaseEntry first = lease.tryAcquire(
                "key-1", "coord-1", Duration.ofSeconds(1)).orElseThrow();
        lease.tryAcquire("key-2", "coord-2", Duration.ofSeconds(30));

        await().atMost(5, SECONDS).until(() -> lease.currentHolder("key-1").isEmpty());
        LeaseEntry next = lease.tryAcquire(
                "key-1", "coord-3", Duration.ofSeconds(30)).orElseThrow();

        assertThat(next.epoch()).isEqualTo(first.epoch() + 1);
        assertThat(lease.currentHolder("key-2")).isPresent();
    }

    @Test
    @DisplayName("Re-entrant acquisition succeeds for same holder")
    void reentrantAcquisitionSucceeds() {
        DistributedLease lease = new JdbcDistributedLease(dataSource);

        LeaseEntry first = lease.tryAcquire(
                "leader", "coord-1", Duration.ofSeconds(30)).orElseThrow();
        LeaseEntry second = lease.tryAcquire(
                "leader", "coord-1", Duration.ofSeconds(30)).orElseThrow();

        assertThat(second).isEqualTo(first);
    }

}
