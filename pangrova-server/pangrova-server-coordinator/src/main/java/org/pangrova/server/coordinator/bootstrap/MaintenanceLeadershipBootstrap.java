package org.pangrova.server.coordinator.bootstrap;

import org.pangrova.common.concurrency.ManagedExecutorFactory;
import org.pangrova.coordinator.indexing.lifecycle.MaintenanceLeaderElection;
import org.pangrova.coordination.api.leadership.MaintenanceLeadershipListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Independent lifecycle for singleton maintenance control loops.
 *
 * <p>Query ownership and maintenance ownership are intentionally separate.
 * Query coordinators may scale independently; only the holder of the dedicated
 * maintenance lease starts singleton catalog/background schedulers.</p>
 */
final class MaintenanceLeadershipBootstrap implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(
            MaintenanceLeadershipBootstrap.class);
    private static final Duration DEFAULT_POLL_INTERVAL = Duration.ofSeconds(10);

    private final MaintenanceLeaderElection election;
    private final MaintenanceLeadershipListener listener;
    private final Duration pollInterval;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean leader = new AtomicBoolean();
    private final AtomicLong activeEpoch = new AtomicLong();
    private final Object lifecycleLock = new Object();

    MaintenanceLeadershipBootstrap(
            MaintenanceLeaderElection election,
            MaintenanceLeadershipListener listener) {
        this(election, listener, DEFAULT_POLL_INTERVAL,
                ManagedExecutorFactory.newSingleThreadScheduledExecutor(
                        "maintenance-leadership"));
    }

    MaintenanceLeadershipBootstrap(
            MaintenanceLeaderElection election,
            MaintenanceLeadershipListener listener,
            Duration pollInterval,
            ScheduledExecutorService scheduler) {
        this.election = Objects.requireNonNull(election, "election must not be null");
        this.listener = Objects.requireNonNull(listener, "listener must not be null");
        if (pollInterval == null || pollInterval.isZero() || pollInterval.isNegative()
                || pollInterval.compareTo(election.leaseDuration().dividedBy(2)) >= 0) {
            throw new IllegalArgumentException(
                    "pollInterval must be positive and less than half the lease duration");
        }
        this.pollInterval = pollInterval;
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler must not be null");
    }

    void start() {
        synchronized (lifecycleLock) {
            if (closed.get()) {
                throw new IllegalStateException("maintenance leadership is closed");
            }
            if (!started.compareAndSet(false, true)) {
                return;
            }
            scheduler.scheduleWithFixedDelay(
                    this::pollSafely, 0, pollInterval.toMillis(), TimeUnit.MILLISECONDS);
        }
        LOG.info("event=maintenance_leadership_started pollIntervalMs={}",
                pollInterval.toMillis());
    }

    void poll() {
        synchronized (lifecycleLock) {
            if (closed.get()) {
                return;
            }
            try {
                pollWhileOpen();
            } catch (RuntimeException failure) {
                // A failed renewal cannot leave singleton background writers locally enabled.
                try {
                    demoteLocally();
                } catch (RuntimeException cleanupFailure) {
                    failure.addSuppressed(cleanupFailure);
                }
                throw failure;
            }
        }
    }

    private void pollWhileOpen() {
        OptionalLong heldEpoch = election.acquireOrRenewEpoch();
        if (heldEpoch.isPresent()) {
            if (leader.get() && activeEpoch.get() != heldEpoch.orElseThrow()) {
                demoteLocally();
            }
            activeEpoch.set(heldEpoch.orElseThrow());
            if (leader.compareAndSet(false, true)) {
                listener.onLeaderElected();
                LOG.info("event=maintenance_leader_elected epoch={}", heldEpoch.orElseThrow());
            }
            return;
        }
        demoteLocally();
    }

    boolean isLeader() {
        return leader.get();
    }

    long activeEpoch() {
        return activeEpoch.get();
    }

    long verifiedActiveEpoch() {
        long term = activeEpoch();
        return term > 0 && election.holdsEpoch(term) ? term : 0;
    }

    private void pollSafely() {
        try {
            poll();
        } catch (RuntimeException failure) {
            LOG.error("event=maintenance_leadership_poll_failed error={}",
                    failure.getMessage(), failure);
        }
    }

    private void demoteLocally() {
        activeEpoch.set(0);
        if (leader.compareAndSet(true, false)) {
            listener.onLeadershipLost();
            LOG.warn("event=maintenance_leadership_lost");
        }
    }

    @Override
    public void close() {
        RuntimeException callbackFailure = null;
        synchronized (lifecycleLock) {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            started.set(false);
            scheduler.shutdownNow();
            try {
                demoteLocally();
            } catch (RuntimeException failure) {
                callbackFailure = failure;
            }
            try {
                election.release();
            } catch (RuntimeException releaseFailure) {
                if (callbackFailure == null) {
                    callbackFailure = releaseFailure;
                } else if (callbackFailure != releaseFailure) {
                    callbackFailure.addSuppressed(releaseFailure);
                }
            }
            LOG.info("event=maintenance_leadership_stopped");
        }
        if (callbackFailure != null) {
            throw callbackFailure;
        }
    }
}
