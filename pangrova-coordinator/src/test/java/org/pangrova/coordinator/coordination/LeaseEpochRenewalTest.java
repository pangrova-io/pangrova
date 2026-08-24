package org.pangrova.coordinator.coordination;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import org.pangrova.coordinator.coordination.inmemory.InMemoryDistributedLease;
import org.pangrova.spi.coordination.LeaseEntry;
import org.junit.jupiter.api.Test;

/**
 * Regression tests for lease epoch semantics in {@link InMemoryDistributedLease}.
 *
 * <p>Validates that:
 * <ul>
 *   <li>Epoch is set on acquisition and incremented on each renewal (fencing)</li>
 *   <li>Renewals only succeed for the current holder</li>
 *   <li>Higher-epoch acquisitions supersede lower-epoch leases</li>
 *   <li>Released/expired leases can be re-acquired</li>
 *   <li>Epoch monotonicity is maintained across holder cycles</li>
 * </ul>
 */
class LeaseEpochRenewalTest {

    private static final String LEASE_KEY = "test_leader";
    private static final String HOLDER_A = "coordinator-A";
    private static final String HOLDER_B = "coordinator-B";
    private static final Duration LEASE_TTL = Duration.ofSeconds(30);

    @Test
    void epochIsSetOnAcquisition() {
        InMemoryDistributedLease lease = new InMemoryDistributedLease();
        assertThat(lease.tryAcquire(LEASE_KEY, HOLDER_A, 0, LEASE_TTL)).isTrue();
        LeaseEntry entry = lease.getLease(LEASE_KEY);
        assertThat(entry.epoch()).isEqualTo(0);
    }

    @Test
    void epochIncrementsOnRenewal() {
        InMemoryDistributedLease lease = new InMemoryDistributedLease();
        assertThat(lease.tryAcquire(LEASE_KEY, HOLDER_A, 5, LEASE_TTL)).isTrue();

        // Renewal increments epoch for fencing
        assertThat(lease.tryRenew(LEASE_KEY, HOLDER_A, LEASE_TTL)).isTrue();
        LeaseEntry entry = lease.getLease(LEASE_KEY);
        assertThat(entry.epoch()).isEqualTo(6);
        assertThat(entry.holderId()).isEqualTo(HOLDER_A);
    }

    @Test
    void renewalRejectedForWrongHolder() {
        InMemoryDistributedLease lease = new InMemoryDistributedLease();
        assertThat(lease.tryAcquire(LEASE_KEY, HOLDER_A, 3, LEASE_TTL)).isTrue();

        // Different holder cannot renew
        assertThat(lease.tryRenew(LEASE_KEY, HOLDER_B, LEASE_TTL)).isFalse();
        LeaseEntry entry = lease.getLease(LEASE_KEY);
        assertThat(entry.epoch()).isEqualTo(3);
        assertThat(entry.holderId()).isEqualTo(HOLDER_A);
    }

    @Test
    void higherEpochAcquisitionSupersedesLowerEpoch() {
        InMemoryDistributedLease lease = new InMemoryDistributedLease();
        assertThat(lease.tryAcquire(LEASE_KEY, HOLDER_A, 3, LEASE_TTL)).isTrue();

        // Holder B can acquire with higher epoch
        assertThat(lease.tryAcquire(LEASE_KEY, HOLDER_B, 10, LEASE_TTL)).isTrue();
        LeaseEntry entry = lease.getLease(LEASE_KEY);
        assertThat(entry.holderId()).isEqualTo(HOLDER_B);
        assertThat(entry.epoch()).isEqualTo(10);
    }

    @Test
    void lowerEpochAcquisitionRejected() {
        InMemoryDistributedLease lease = new InMemoryDistributedLease();
        assertThat(lease.tryAcquire(LEASE_KEY, HOLDER_A, 10, LEASE_TTL)).isTrue();

        // Holder B cannot acquire with lower epoch
        assertThat(lease.tryAcquire(LEASE_KEY, HOLDER_B, 5, LEASE_TTL)).isFalse();
        LeaseEntry entry = lease.getLease(LEASE_KEY);
        assertThat(entry.holderId()).isEqualTo(HOLDER_A);
        assertThat(entry.epoch()).isEqualTo(10);
    }

    @Test
    void releaseAndReacquire() {
        InMemoryDistributedLease lease = new InMemoryDistributedLease();
        assertThat(lease.tryAcquire(LEASE_KEY, HOLDER_A, 3, LEASE_TTL)).isTrue();

        // Release the lease
        assertThat(lease.release(LEASE_KEY, HOLDER_A)).isTrue();
        assertThat(lease.currentHolder(LEASE_KEY)).isEmpty();

        // Holder B acquires — since entry is removed, any epoch works
        assertThat(lease.tryAcquire(LEASE_KEY, HOLDER_B, 0, LEASE_TTL)).isTrue();
        LeaseEntry entry = lease.getLease(LEASE_KEY);
        assertThat(entry.holderId()).isEqualTo(HOLDER_B);
    }

    @Test
    void sameHolderReacquisitionWithHigherEpoch() {
        InMemoryDistributedLease lease = new InMemoryDistributedLease();
        assertThat(lease.tryAcquire(LEASE_KEY, HOLDER_A, 0, LEASE_TTL)).isTrue();

        // Same holder re-acquires with higher epoch
        assertThat(lease.tryAcquire(LEASE_KEY, HOLDER_A, 5, LEASE_TTL)).isTrue();
        LeaseEntry entry = lease.getLease(LEASE_KEY);
        assertThat(entry.epoch()).isEqualTo(5);
    }

    @Test
    void multipleRenewalsExtendLease() {
        InMemoryDistributedLease lease = new InMemoryDistributedLease();
        assertThat(lease.tryAcquire(LEASE_KEY, HOLDER_A, 0, LEASE_TTL)).isTrue();

        // Multiple renewals should all succeed, each incrementing epoch
        for (int i = 0; i < 10; i++) {
            assertThat(lease.tryRenew(LEASE_KEY, HOLDER_A, LEASE_TTL)).isTrue();
        }

        LeaseEntry entry = lease.getLease(LEASE_KEY);
        assertThat(entry.holderId()).isEqualTo(HOLDER_A);
        assertThat(entry.epoch()).isEqualTo(10);
    }

    @Test
    void expireStaleRemovesExpiredEntries() {
        InMemoryDistributedLease lease = new InMemoryDistributedLease();
        // Acquire with 0-duration lease (expires immediately)
        assertThat(lease.tryAcquire(LEASE_KEY, HOLDER_A, 0, Duration.ZERO)).isTrue();

        // Entry should be expired
        assertThat(lease.currentHolder(LEASE_KEY)).isEmpty();

        // expireStale removes it
        int expired = lease.expireStale();
        assertThat(expired).isEqualTo(1);
        assertThat(lease.getLease(LEASE_KEY)).isNull();
    }

    @Test
    void haServiceEpochSyncsAfterRenewal() {
        InMemoryDistributedLease lease = new InMemoryDistributedLease();
        var haService = new HighAvailabilityCoordinationService(
                lease,
                new HighAvailabilityCoordinationService.CoordinationConfig(30_000, 10_000));
        try {
            // Acquire leadership
            assertThat(haService.tryBecomeLeader(HOLDER_A)).isTrue();
            long epochAfterAcquire = haService.getCurrentEpoch();
            assertThat(epochAfterAcquire).isGreaterThanOrEqualTo(1);

            // Renewal should succeed
            assertThat(haService.renewLease(HOLDER_A)).isTrue();

            // Epoch should be at least as high as after acquire
            assertThat(haService.getCurrentEpoch()).isGreaterThanOrEqualTo(epochAfterAcquire);
        } finally {
            haService.close();
        }
    }

    @Test
    void leaseCountTracksActiveLeases() {
        InMemoryDistributedLease lease = new InMemoryDistributedLease();
        assertThat(lease.leaseCount()).isEqualTo(0);

        lease.tryAcquire("key1", HOLDER_A, 0, LEASE_TTL);
        assertThat(lease.leaseCount()).isEqualTo(1);

        lease.tryAcquire("key2", HOLDER_B, 0, LEASE_TTL);
        assertThat(lease.leaseCount()).isEqualTo(2);

        lease.release("key1", HOLDER_A);
        assertThat(lease.leaseCount()).isEqualTo(1);
    }

    @Test
    void renewalFailsForNonexistentLease() {
        InMemoryDistributedLease lease = new InMemoryDistributedLease();
        assertThat(lease.tryRenew(LEASE_KEY, HOLDER_A, LEASE_TTL)).isFalse();
    }

    @Test
    void releaseFailsForWrongHolder() {
        InMemoryDistributedLease lease = new InMemoryDistributedLease();
        lease.tryAcquire(LEASE_KEY, HOLDER_A, 0, LEASE_TTL);
        assertThat(lease.release(LEASE_KEY, HOLDER_B)).isFalse();
        assertThat(lease.getLease(LEASE_KEY).holderId()).isEqualTo(HOLDER_A);
    }
}
