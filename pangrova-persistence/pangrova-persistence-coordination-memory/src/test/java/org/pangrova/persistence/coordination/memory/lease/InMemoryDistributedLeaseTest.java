package org.pangrova.persistence.coordination.memory.lease;

import org.junit.jupiter.api.Test;
import org.pangrova.coordination.api.lease.LeaseEntry;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;

import static org.assertj.core.api.Assertions.assertThat;

class InMemoryDistributedLeaseTest {

    private static final String LEASE_KEY = "test_leader";
    private static final String HOLDER_A = "coordinator-A";
    private static final String HOLDER_B = "coordinator-B";
    private static final Duration LEASE_TTL = Duration.ofSeconds(30);

    @Test
    void backendIssuesFirstEpoch() {
        InMemoryDistributedLease lease = new InMemoryDistributedLease();

        LeaseEntry entry = lease.tryAcquire(
                LEASE_KEY, HOLDER_A, LEASE_TTL).orElseThrow();

        assertThat(entry.epoch()).isEqualTo(1);
    }

    @Test
    void renewalPreservesEpoch() {
        InMemoryDistributedLease lease = new InMemoryDistributedLease();
        LeaseEntry entry = lease.tryAcquire(
                LEASE_KEY, HOLDER_A, LEASE_TTL).orElseThrow();

        assertThat(lease.tryRenew(
                LEASE_KEY, HOLDER_A, entry.epoch(), LEASE_TTL)).isTrue();
        assertThat(lease.currentHolder(LEASE_KEY).orElseThrow().epoch())
                .isEqualTo(entry.epoch());
    }

    @Test
    void wrongOwnerOrEpochCannotRenew() {
        InMemoryDistributedLease lease = new InMemoryDistributedLease();
        LeaseEntry entry = lease.tryAcquire(
                LEASE_KEY, HOLDER_A, LEASE_TTL).orElseThrow();

        assertThat(lease.tryRenew(
                LEASE_KEY, HOLDER_B, entry.epoch(), LEASE_TTL)).isFalse();
        assertThat(lease.tryRenew(
                LEASE_KEY, HOLDER_A, entry.epoch() + 1, LEASE_TTL)).isFalse();
    }

    @Test
    void activeLeaseCannotBePreempted() {
        InMemoryDistributedLease lease = new InMemoryDistributedLease();
        LeaseEntry first = lease.tryAcquire(
                LEASE_KEY, HOLDER_A, LEASE_TTL).orElseThrow();

        assertThat(lease.tryAcquire(LEASE_KEY, HOLDER_B, LEASE_TTL)).isEmpty();
        assertThat(lease.currentHolder(LEASE_KEY)).contains(first);
    }

    @Test
    void reentrantAcquisitionReturnsSameTokenWithoutExtendingTenure() {
        InMemoryDistributedLease lease = new InMemoryDistributedLease();
        LeaseEntry first = lease.tryAcquire(
                LEASE_KEY, HOLDER_A, LEASE_TTL).orElseThrow();

        assertThat(lease.tryAcquire(LEASE_KEY, HOLDER_A, LEASE_TTL)).contains(first);
    }

    @Test
    void releaseRetainsHighWatermarkAndFencesOldToken() {
        InMemoryDistributedLease lease = new InMemoryDistributedLease();
        LeaseEntry first = lease.tryAcquire(
                LEASE_KEY, HOLDER_A, LEASE_TTL).orElseThrow();
        assertThat(lease.release(
                LEASE_KEY, HOLDER_A, first.epoch())).isTrue();

        LeaseEntry second = lease.tryAcquire(
                LEASE_KEY, HOLDER_B, LEASE_TTL).orElseThrow();
        assertThat(second.epoch()).isEqualTo(first.epoch() + 1);
        assertThat(lease.tryRenew(
                LEASE_KEY, HOLDER_A, first.epoch(), LEASE_TTL)).isFalse();
        assertThat(lease.release(
                LEASE_KEY, HOLDER_A, first.epoch())).isFalse();
    }

    @Test
    void expiryRetainsHighWatermark() {
        MutableClock clock = new MutableClock();
        InMemoryDistributedLease lease = new InMemoryDistributedLease(clock);
        LeaseEntry first = lease.tryAcquire(
                LEASE_KEY, HOLDER_A, Duration.ofMillis(10)).orElseThrow();
        clock.advance(Duration.ofMillis(30));

        LeaseEntry second = lease.tryAcquire(
                LEASE_KEY, HOLDER_B, LEASE_TTL).orElseThrow();
        assertThat(second.epoch()).isEqualTo(first.epoch() + 1);
    }

    private static final class MutableClock extends Clock {
        private Instant instant = Instant.parse("2026-09-19T00:00:00Z");

        private void advance(Duration duration) {
            instant = instant.plus(duration);
        }

        @Override
        public ZoneId getZone() {
            return ZoneId.of("UTC");
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return instant;
        }
    }

    @Test
    void activeAndRetainedLeaseCountsAreDistinct() {
        InMemoryDistributedLease lease = new InMemoryDistributedLease();
        LeaseEntry first = lease.tryAcquire(
                "key1", HOLDER_A, LEASE_TTL).orElseThrow();
        lease.tryAcquire("key2", HOLDER_B, LEASE_TTL);

        assertThat(lease.leaseCount()).isEqualTo(2);
        assertThat(lease.activeLeaseCount()).isEqualTo(2);
        lease.release("key1", HOLDER_A, first.epoch());
        assertThat(lease.leaseCount()).isEqualTo(2);
        assertThat(lease.activeLeaseCount()).isEqualTo(1);
    }

}
