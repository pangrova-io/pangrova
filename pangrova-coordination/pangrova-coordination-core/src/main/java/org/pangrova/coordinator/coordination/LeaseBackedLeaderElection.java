package org.pangrova.coordinator.coordination;

import org.pangrova.persistence.api.coordination.DistributedLease;
import org.pangrova.spi.coordination.LeaseEntry;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/**
 * Lease-based leader election using the unified {@link DistributedLease} SPI.
 *
 * <p>Uses a fixed lease key {@code "coordinator_leader"}. The lease backend
 * is the sole issuer of fencing epochs.</p>
 */
public final class LeaseBackedLeaderElection {

    private static final String LEASE_KEY = "coordinator_leader";
    private final DistributedLease lease;

    public LeaseBackedLeaderElection(DistributedLease lease) {
        this.lease = Objects.requireNonNull(lease, "lease must not be null");
    }

    public Optional<LeaseEntry> tryAcquire(String candidateId, long leaseDurationMs) {
        Objects.requireNonNull(candidateId, "candidateId must not be null");
        return lease.tryAcquire(LEASE_KEY, candidateId,
                Duration.ofMillis(leaseDurationMs));
    }

    public boolean tryRenew(String leaderId, long epoch, long leaseDurationMs) {
        Objects.requireNonNull(leaderId, "leaderId must not be null");
        return lease.tryRenew(
                LEASE_KEY, leaderId, epoch, Duration.ofMillis(leaseDurationMs));
    }

    public boolean release(String leaderId, long epoch) {
        Objects.requireNonNull(leaderId, "leaderId must not be null");
        return lease.release(LEASE_KEY, leaderId, epoch);
    }

    public Optional<String> currentLeader() {
        return lease.currentHolder(LEASE_KEY).map(LeaseEntry::holderId);
    }

    /**
     * Returns the full lease entry for the current leader, including epoch.
     * Used by HA service to synchronize distributed epoch on leader promotion.
     */
    public Optional<LeaseEntry> currentLeaderEntry() {
        return lease.currentHolder(LEASE_KEY);
    }
}
