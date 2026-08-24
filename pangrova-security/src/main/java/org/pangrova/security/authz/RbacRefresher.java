package org.pangrova.security.authz;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Background poller that periodically calls {@link DynamicRbacAccessControl#refreshGrants()}
 * so all coordinators converge on the same DB-backed view of grants and role catalog within
 * a bounded interval — even when no in-process event is fired (e.g. a peer coordinator
 * changed state).
 *
 * <p>Default poll interval is 30 seconds, matching the documented behaviour of Apache Ranger
 * plugin policy polling. The interval is configurable per-instance.
 *
 * <p>Lifecycle: {@link #start()} schedules the recurring task; {@link #close()} shuts down the
 * executor and stops further refreshes. The class is idempotent — repeated start/close calls
 * are safe.
 */
public final class RbacRefresher implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(RbacRefresher.class);

    /** Default poll interval (matches Ranger default {@code policy.pollIntervalMs=30000}). */
    public static final Duration DEFAULT_POLL_INTERVAL = Duration.ofSeconds(30);

    private final DynamicRbacAccessControl accessControl;
    private final Duration pollInterval;
    private final ScheduledExecutorService scheduler;
    private volatile ScheduledFuture<?> handle;
    private volatile boolean closed;

    public RbacRefresher(DynamicRbacAccessControl accessControl) {
        this(accessControl, DEFAULT_POLL_INTERVAL);
    }

    public RbacRefresher(DynamicRbacAccessControl accessControl, Duration pollInterval) {
        this(accessControl, pollInterval, defaultScheduler());
    }

    /** Constructor for tests that supply a deterministic / virtual-time scheduler. */
    public RbacRefresher(DynamicRbacAccessControl accessControl,
                         Duration pollInterval,
                         ScheduledExecutorService scheduler) {
        this.accessControl = Objects.requireNonNull(accessControl, "accessControl");
        this.pollInterval = Objects.requireNonNull(pollInterval, "pollInterval");
        if (pollInterval.isZero() || pollInterval.isNegative()) {
            throw new IllegalArgumentException("pollInterval must be positive");
        }
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
    }

    /** Starts the periodic refresh. Idempotent. */
    public synchronized void start() {
        if (closed) {
            throw new IllegalStateException("RbacRefresher is closed");
        }
        if (handle != null) {
            return;
        }
        long intervalMs = pollInterval.toMillis();
        handle = scheduler.scheduleWithFixedDelay(
                this::refreshSafely, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
        LOG.info("event=rbac_refresher_started poll_interval_ms={}", intervalMs);
    }

    /** Forces an immediate refresh on the calling thread. */
    public void refreshNow() {
        refreshSafely();
    }

    private void refreshSafely() {
        try {
            accessControl.refreshGrants();
        } catch (RuntimeException e) {
            // refreshGrants already logs; defensively guard the scheduled task so a transient
            // failure does not cancel the recurring schedule (ScheduledExecutorService cancels
            // a task that throws).
            LOG.warn("event=rbac_refresher_iteration_failed error={}", e.getMessage(), e);
        }
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        if (handle != null) {
            handle.cancel(false);
            handle = null;
        }
        scheduler.shutdownNow();
        LOG.info("event=rbac_refresher_stopped");
    }

    private static ScheduledExecutorService defaultScheduler() {
        ThreadFactory tf = new ThreadFactory() {
            private final AtomicInteger counter = new AtomicInteger();

            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "rbac-refresher-" + counter.incrementAndGet());
                t.setDaemon(true);
                return t;
            }
        };
        return Executors.newSingleThreadScheduledExecutor(tf);
    }
}
