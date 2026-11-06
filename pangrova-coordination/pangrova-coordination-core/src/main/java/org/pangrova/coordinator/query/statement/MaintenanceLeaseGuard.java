package org.pangrova.coordinator.query.statement;

import org.pangrova.persistence.coordination.api.lease.DistributedLease;
import org.pangrova.coordination.api.lease.LeaseEntry;
import org.pangrova.security.api.error.AccessDeniedException;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/** Keeps one maintenance attempt's lease alive and never restores a lost attempt. */
final class MaintenanceLeaseGuard implements AutoCloseable {

    private final DistributedLease leases;
    private final LeaseEntry acquired;
    private final Duration ttl;
    private final ScheduledExecutorService renewal;
    private final AtomicReference<String> failure = new AtomicReference<>();

    MaintenanceLeaseGuard(DistributedLease leases, LeaseEntry acquired, Duration ttl) {
        this.leases = leases;
        this.acquired = acquired;
        this.ttl = ttl;
        renewal = Executors.newSingleThreadScheduledExecutor(task -> {
            Thread thread = new Thread(task, "table-maintenance-lease");
            thread.setDaemon(true);
            return thread;
        });
        long period = Math.max(1, ttl.toMillis() / 3);
        renewal.scheduleWithFixedDelay(this::renew, period, period, TimeUnit.MILLISECONDS);
    }

    /** Checks backend ownership immediately before execution and before reporting success. */
    void requireHeld() {
        renew();
        String reason = failure.get();
        if (reason != null) {
            throw new AccessDeniedException("Maintenance lease lost: " + reason
                    + "; execution cannot be reported as successful");
        }
    }

    private void renew() {
        if (failure.get() != null) {
            return;
        }
        try {
            if (!leases.tryRenew(acquired.leaseKey(), acquired.holderId(), acquired.epoch(), ttl)) {
                failure.compareAndSet(null, "the attempt no longer owns its fencing epoch");
            }
        } catch (RuntimeException unavailable) {
            failure.compareAndSet(null, "ownership could not be verified: " + unavailable.getMessage());
        }
    }

    @Override
    public void close() {
        renewal.shutdownNow();
    }
}
