package org.pangrova.coordinator.dynamictable.refresh.lock;

import org.pangrova.coordinator.coordination.inmemory.InMemoryDistributedLease;
import org.pangrova.spi.coordination.DistributedLease;
import org.pangrova.spi.coordination.LeaseEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;

/**
 * Manages mutual-exclusion locks for Dynamic Table refresh operations,
 * delegating to the unified {@link DistributedLease} SPI.
 *
 * <p>Lease keys are prefixed with {@code "dt-refresh:"} to namespace
 * DT refresh locks within the shared lease store.</p>
 */
public final class RefreshLockManager {

    private static final Logger LOG = LoggerFactory.getLogger(RefreshLockManager.class);
    private static final String KEY_PREFIX = "dt-refresh:";

    private final DistributedLease lease;

    public RefreshLockManager(DistributedLease lease) {
        this.lease = Objects.requireNonNull(lease, "lease must not be null");
    }

    /** Attempts to acquire the refresh lock for the given Dynamic Table. */
    public Optional<LeaseEntry> tryAcquire(
            String dtName, String coordinatorId, Duration timeout) {
        Optional<LeaseEntry> acquired = lease.tryAcquire(
                leaseKey(dtName), coordinatorId, timeout);
        if (acquired.isPresent()) {
            LOG.debug("event=dt_lock_acquired dtName={} coordinatorId={} epoch={}",
                    dtName, coordinatorId, acquired.orElseThrow().epoch());
        }
        return acquired;
    }

    /**
     * Renews an existing refresh lock held by the specified coordinator.
     *
     * <p>Succeeds only if the given coordinator currently holds an active
     * (non-expired) lease for the DT. Extends the expiration by the given
     * duration from now.</p>
     *
     * <p>This is the prerequisite for BUG-D03: lease renewal during
     * long-running DT refreshes to prevent lock expiry and concurrent writes.</p>
     *
     * @param dtName        Dynamic Table name
     * @param coordinatorId coordinator that currently holds the lock
     * @param duration      new TTL from now
     * @return true if the lease was renewed
     */
    public boolean tryRenew(LeaseEntry held, Duration duration) {
        Objects.requireNonNull(held, "held must not be null");
        boolean renewed = lease.tryRenew(
                held.leaseKey(), held.holderId(), held.epoch(), duration);
        if (renewed) {
            LOG.debug("event=dt_lock_renewed dtName={} coordinatorId={} duration={}",
                    held.leaseKey(), held.holderId(), duration);
        } else {
            LOG.debug("event=dt_lock_renew_failed dtName={} coordinatorId={}",
                    held.leaseKey(), held.holderId());
        }
        return renewed;
    }

    public void release(LeaseEntry held) {
        Objects.requireNonNull(held, "held must not be null");
        lease.release(held.leaseKey(), held.holderId(), held.epoch());
    }

    /**
     * Returns the number of active locks, if the underlying lease supports counting.
     * Production JDBC leases return empty since counting requires a query
     * and is not a lock-manager concern.
     */
    public OptionalInt lockCount() {
        if (lease instanceof InMemoryDistributedLease mem) {
            return OptionalInt.of(mem.activeLeaseCount());
        }
        return OptionalInt.empty();
    }

    /** Returns the current lock entry for the given Dynamic Table, or null if unlocked. */
    public LockEntry getLock(String dtName) {
        return lease.currentHolder(leaseKey(dtName))
                .map(e -> new LockEntry(dtName, e.holderId(), e.epoch(),
                        e.acquiredAt(),
                        Duration.between(e.acquiredAt(), e.expiresAt())))
                .orElse(null);
    }

    private static String leaseKey(String dtName) {
        return KEY_PREFIX + dtName;
    }
}
