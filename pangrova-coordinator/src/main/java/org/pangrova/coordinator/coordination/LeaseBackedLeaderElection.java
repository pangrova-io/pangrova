package org.pangrova.coordinator.coordination;

import org.pangrova.spi.coordination.DistributedLease;
import org.pangrova.spi.coordination.LeaseEntry;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/**
 * Lease-based leader election using the unified {@link DistributedLease} SPI.
 *
 * <p>Uses a fixed lease key {@code "coordinator_leader"}. Epoch can be
 * supplied via {@link #tryAcquireWithEpoch} for proper fencing, or
 * defaults to 0 via the legacy {@link #tryAcquire} interface.</p>
 */
public final class LeaseBackedLeaderElection {

    private static final String LEASE_KEY = "coordinator_leader";
    private static final long DEFAULT_EPOCH = 0;

    private final DistributedLease lease;

    public LeaseBackedLeaderElection(DistributedLease lease) {
        this.lease = Objects.requireNonNull(lease, "lease must not be null");
    }

    public boolean tryAcquire(String candidateId, long leaseDurationMs) {
        Objects.requireNonNull(candidateId, "candidateId must not be null");
        return lease.tryAcquire(LEASE_KEY, candidateId, DEFAULT_EPOCH,
                Duration.ofMillis(leaseDurationMs));
    }

    /**
     * Acquires leadership with explicit epoch for fencing.
     *
     * @param candidateId candidate node identifier
     * @param epoch       monotonic epoch for stale-leader fencing
     * @param leaseDurationMs lease TTL in milliseconds
     * @return true if the candidate is now the leader
     */
    public boolean tryAcquireWithEpoch(String candidateId, long epoch, long leaseDurationMs) {
        Objects.requireNonNull(candidateId, "candidateId must not be null");
        return lease.tryAcquire(LEASE_KEY, candidateId, epoch,
                Duration.ofMillis(leaseDurationMs));
    }

    public boolean tryRenew(String leaderId, long leaseDurationMs) {
        Objects.requireNonNull(leaderId, "leaderId must not be null");
        return lease.tryRenew(LEASE_KEY, leaderId, Duration.ofMillis(leaseDurationMs));
    }

    public boolean release(String leaderId) {
        Objects.requireNonNull(leaderId, "leaderId must not be null");
        return lease.release(LEASE_KEY, leaderId);
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
