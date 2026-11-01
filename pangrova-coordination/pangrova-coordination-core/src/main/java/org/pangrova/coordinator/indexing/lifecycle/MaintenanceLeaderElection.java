package org.pangrova.coordinator.indexing.lifecycle;

import org.pangrova.persistence.api.coordination.DistributedLease;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicReference;

import org.pangrova.spi.coordination.LeaseEntry;

/**
 * Leader election for index maintenance tasks, delegating to
 * the unified {@link DistributedLease} SPI.
 *
 * <p>Uses one generic maintenance-control lease so only one coordinator runs
 * singleton catalog/background state machines at a time. Query scheduling has
 * an independent lifecycle and never inherits this lease.</p>
 */
public final class MaintenanceLeaderElection {

    private static final Logger LOG = LoggerFactory.getLogger(MaintenanceLeaderElection.class);
    private static final Duration LEASE_DURATION = Duration.ofSeconds(30);
    private static final String LEASE_KEY = "maintenance-control";

    private final DistributedLease lease;
    private final String coordinatorId;
    private final AtomicReference<LeaseEntry> heldLease = new AtomicReference<>();

    /**
     * Creates a new election instance. The backend assigns the fencing epoch.
     */
    public MaintenanceLeaderElection(DistributedLease lease, String coordinatorId) {
        this.lease = Objects.requireNonNull(lease, "lease must not be null");
        this.coordinatorId = Objects.requireNonNull(coordinatorId, "coordinatorId must not be null");
    }

    /**
     * Attempts to acquire or renew the maintenance lease.
     *
     * @return true if this coordinator holds the lease after this call
     */
    public boolean tryAcquireOrRenew() {
        return acquireOrRenewEpoch().isPresent();
    }

    /**
     * Acquires or renews the maintenance lease and returns the exact durable epoch.
     *
     * <p>Renewal and release carry the exact backend-issued fencing epoch.
     * Contenders cannot invent a token or preempt an unexpired holder.</p>
     */
    public OptionalLong acquireOrRenewEpoch() {
        LeaseEntry held = heldLease.get();
        if (held != null && lease.tryRenew(
                LEASE_KEY, coordinatorId, held.epoch(), LEASE_DURATION)) {
            LOG.debug("event=lease_renewed coordinator={}", coordinatorId);
            return OptionalLong.of(held.epoch());
        }
        if (held != null) {
            heldLease.compareAndSet(held, null);
        }
        var acquired = lease.tryAcquire(LEASE_KEY, coordinatorId, LEASE_DURATION);
        if (acquired.isPresent()) {
            LeaseEntry entry = acquired.orElseThrow();
            heldLease.set(entry);
            LOG.info("event=lease_acquired coordinator={} epoch={}",
                    coordinatorId, entry.epoch());
            return OptionalLong.of(entry.epoch());
        }
        LOG.debug("event=lease_not_acquired coordinator={}", coordinatorId);
        return OptionalLong.empty();
    }

    /** Returns the configured lease duration. */
    public Duration leaseDuration() {
        return LEASE_DURATION;
    }

    public String holderId() {
        return coordinatorId;
    }

    /** Releases leadership during graceful shutdown. */
    public boolean release() {
        LeaseEntry held = heldLease.getAndSet(null);
        return held != null && lease.release(
                LEASE_KEY, coordinatorId, held.epoch());
    }
}
