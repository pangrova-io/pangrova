package org.pangrova.coordinator.dynamictable.refresh.pipeline;

import org.pangrova.coordinator.dynamictable.refresh.LockRenewalFailedException;
import org.pangrova.coordinator.dynamictable.refresh.lock.RefreshLockManager;
import org.pangrova.metadata.dynamictable.DynamicTableState;
import org.pangrova.spi.coordination.LeaseEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages distributed lock acquisition, periodic renewal, and release
 * for Dynamic Table refresh cycles.
 *
 * <p>Concurrency strategy: the {@code renewalAborted} flag is set by the
 * renewal scheduler thread and read by the refresh pipeline thread.
 * AtomicBoolean provides the necessary visibility guarantee.</p>
 */
public final class RefreshLockCoordinator {

    private static final Logger LOG = LoggerFactory.getLogger(RefreshLockCoordinator.class);

    private final RefreshLockManager lockManager;
    private final ScheduledExecutorService renewalScheduler;
    private final AtomicBoolean renewalAborted = new AtomicBoolean(false);
    private final ConcurrentHashMap<String, LeaseEntry> heldLeases = new ConcurrentHashMap<>();

    public RefreshLockCoordinator(RefreshLockManager lockManager) {
        this(lockManager, createDefaultRenewalScheduler());
    }

    public RefreshLockCoordinator(RefreshLockManager lockManager,
                                  ScheduledExecutorService renewalScheduler) {
        this.lockManager = Objects.requireNonNull(lockManager, "lockManager must not be null");
        this.renewalScheduler = Objects.requireNonNull(renewalScheduler, "renewalScheduler must not be null");
    }

    private static ScheduledExecutorService createDefaultRenewalScheduler() {
        var scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "dt-refresh-lock-renewal");
            t.setDaemon(true);
            return t;
        });
        if (scheduler instanceof java.util.concurrent.ScheduledThreadPoolExecutor stpe) {
            stpe.setRemoveOnCancelPolicy(true);
        }
        return scheduler;
    }

    /**
     * Attempts to acquire the distributed refresh lock for the given DT.
     *
     * @param state   current Dynamic Table state (provides dtName, coordinatorId, epoch)
     * @param timeout lock acquisition timeout
     * @return true if the lock was acquired
     */
    public boolean acquireLock(DynamicTableState state, Duration timeout) {
        if (heldLeases.containsKey(state.dtName())) {
            return false;
        }
        LeaseEntry acquired = lockManager.tryAcquire(
                state.dtName(), state.coordinatorId(), timeout).orElse(null);
        return acquired != null
                && heldLeases.putIfAbsent(state.dtName(), acquired) == null;
    }

    /**
     * Releases the distributed refresh lock for the given DT.
     */
    public void releaseLock(String dtName, String coordinatorId) {
        LeaseEntry held = heldLeases.remove(dtName);
        if (held != null && held.holderId().equals(coordinatorId)) {
            lockManager.release(held);
        }
    }

    /**
     * Resets the renewal-aborted flag and starts periodic lock renewal.
     * Returns a future that can be cancelled to stop renewal.
     *
     * @param dtName        Dynamic Table name
     * @param coordinatorId owning coordinator
     * @param lockTimeout   lock lease duration (renewal interval = lockTimeout / 3)
     * @return scheduled future for the renewal task
     */
    public ScheduledFuture<?> startRenewal(String dtName, String coordinatorId,
                                           Duration lockTimeout) {
        renewalAborted.set(false);
        long renewalIntervalMs = lockTimeout.toMillis() / 3;
        return renewalScheduler.scheduleAtFixedRate(
                () -> renewLock(dtName, coordinatorId, lockTimeout),
                renewalIntervalMs, renewalIntervalMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Checks whether lock renewal has been aborted. If so, throws
     * {@link LockRenewalFailedException} to prevent writes after lock expiry.
     */
    public void checkRenewalAborted(String dtName, String refreshId) {
        if (renewalAborted.get()) {
            throw new LockRenewalFailedException(dtName, refreshId);
        }
    }

    /**
     * Periodic lock renewal callback. Invoked by the renewal scheduler
     * at {@code lockTimeout / 3} intervals to extend the lease before
     * it expires during long-running refreshes.
     *
     * <p>If renewal fails (lease expired or stolen by another coordinator),
     * sets {@code renewalAborted} so the pipeline aborts before writing.</p>
     */
    private void renewLock(String dtName, String coordinatorId, Duration lockTimeout) {
        try {
            LeaseEntry held = heldLeases.get(dtName);
            boolean renewed = held != null && held.holderId().equals(coordinatorId)
                    && lockManager.tryRenew(held, lockTimeout);
            if (!renewed) {
                renewalAborted.set(true);
                LOG.warn("event=dt_lock_renewal_failed dtName={} coordinatorId={} action=abort_before_write",
                        dtName, coordinatorId);
            } else {
                LOG.debug("event=dt_lock_renewed dtName={} coordinatorId={}", dtName, coordinatorId);
            }
        } catch (Exception e) {
            renewalAborted.set(true);
            LOG.error("event=dt_lock_renewal_error dtName={} coordinatorId={} error={}",
                    dtName, coordinatorId, e.getMessage());
        }
    }
}
