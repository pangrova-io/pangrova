package org.pangrova.coordinator.index.lifecycle;

import org.pangrova.spi.coordination.DistributedLease;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Leader election for index maintenance tasks, delegating to
 * the unified {@link DistributedLease} SPI.
 *
 * <p>Uses lease key {@code "index-maintenance"} to ensure only one
 * coordinator runs maintenance operations (GC, consistency checks,
 * stuck build recovery) at a time.</p>
 */
public final class MaintenanceLeaderElection {

    private static final Logger LOG = LoggerFactory.getLogger(MaintenanceLeaderElection.class);
    private static final Duration LEASE_DURATION = Duration.ofSeconds(30);
    private static final String LEASE_KEY = "index-maintenance";

    private final DistributedLease lease;
    private final String coordinatorId;
    private final AtomicLong epoch;

    /**
     * Creates a new election instance, reading the current epoch from the
     * lease backend to guarantee monotonicity across restarts.
     */
    public MaintenanceLeaderElection(DistributedLease lease, String coordinatorId) {
        this.lease = Objects.requireNonNull(lease, "lease must not be null");
        this.coordinatorId = Objects.requireNonNull(coordinatorId, "coordinatorId must not be null");
        long initialEpoch = lease.currentHolder(LEASE_KEY)
                .map(entry -> entry.epoch())
                .orElse(0L);
        this.epoch = new AtomicLong(initialEpoch);
        LOG.info("event=epoch_initialized coordinator={} epoch={}", coordinatorId, initialEpoch);
    }

    /**
     * Constructor with explicit epoch source for testing or manual fencing.
     * The epoch is incremented on each successful acquisition to fence stale holders.
     */
    public MaintenanceLeaderElection(DistributedLease lease, String coordinatorId, AtomicLong epoch) {
        this.lease = Objects.requireNonNull(lease, "lease must not be null");
        this.coordinatorId = Objects.requireNonNull(coordinatorId, "coordinatorId must not be null");
        this.epoch = Objects.requireNonNull(epoch, "epoch must not be null");
    }

    /**
     * Attempts to acquire or renew the maintenance lease.
     *
     * @return true if this coordinator holds the lease after this call
     */
    public boolean tryAcquireOrRenew() {
        if (lease.tryRenew(LEASE_KEY, coordinatorId, LEASE_DURATION)) {
            LOG.debug("event=lease_renewed coordinator={}", coordinatorId);
            return true;
        }
        long currentEpoch = epoch.incrementAndGet();
        if (lease.tryAcquire(LEASE_KEY, coordinatorId, currentEpoch, LEASE_DURATION)) {
            LOG.info("event=lease_acquired coordinator={} epoch={}", coordinatorId, currentEpoch);
            return true;
        }
        LOG.debug("event=lease_not_acquired coordinator={}", coordinatorId);
        return false;
    }

    /** Returns the configured lease duration. */
    public Duration leaseDuration() {
        return LEASE_DURATION;
    }
}
