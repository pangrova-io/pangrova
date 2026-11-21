package org.pangrova.query.lifecycle;

import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.pangrova.common.concurrent.ExecutorDrain;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.query.api.lifecycle.QueryTimeoutHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Schedules query timeout sweeps; the injected query owner performs all transitions.
 * Owns and drains its scheduler. Server starts/stops this component; the handler is
 * borrowed and must remain available until stop returns. Start/stop are serialized
 * by the process lifecycle owner, while sweeps may race with query operations.
 */
@ThreadSafe
public final class QueryTimeoutScheduler {
    private static final long EXECUTOR_SHUTDOWN_TIMEOUT_MS = 5_000L;
    private static final Logger LOG = LoggerFactory.getLogger(QueryTimeoutScheduler.class);
    private final Duration timeout;
    private final Duration checkInterval;
    private final ScheduledExecutorService scheduler;
    private final Clock clock;
    private volatile ScheduledFuture<?> scheduledTask;

    public QueryTimeoutScheduler(Duration timeout, Duration checkInterval) {
        this(timeout, checkInterval, createDaemonScheduler(), Clock.systemUTC());
    }

    QueryTimeoutScheduler(Duration timeout, Duration checkInterval,
            ScheduledExecutorService scheduler, Clock clock) {
        this.timeout = Objects.requireNonNull(timeout, "timeout must not be null");
        this.checkInterval = Objects.requireNonNull(checkInterval, "checkInterval must not be null");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
    }

    /** Starts periodic sweeps with an explicit owner; there is no direct-state fallback. */
    public void start(QueryTimeoutHandler handler) {
        Objects.requireNonNull(handler, "handler must not be null");
        if (scheduledTask != null) throw new IllegalStateException("Query timeout scheduler already started");
        scheduledTask = scheduler.scheduleAtFixedRate(
                () -> ExecutorDrain.runOwned(scheduler, () -> enforceTimeouts(handler)),
                checkInterval.toMillis(), checkInterval.toMillis(), TimeUnit.MILLISECONDS);
        LOG.info("event=timeout_enforcer_start timeout={} interval={}", timeout, checkInterval);
    }

    /** Cancels future sweeps and waits for any active owner callback to exit. */
    public void stop() {
        ScheduledFuture<?> task = scheduledTask;
        if (task != null) task.cancel(false);
        ExecutorDrain.shutdownAndAwait(scheduler, EXECUTOR_SHUTDOWN_TIMEOUT_MS);
    }

    private void enforceTimeouts(QueryTimeoutHandler handler) {
        try {
            handler.timeoutQueriesCreatedBy(clock.instant().minus(timeout), timeout);
        } catch (Exception failure) {
            LOG.warn("event=timeout_enforcement_error error={}", failure.getMessage(), failure);
        }
    }

    private static ScheduledExecutorService createDaemonScheduler() {
        var executor = new java.util.concurrent.ScheduledThreadPoolExecutor(1, r -> {
            Thread t = new Thread(r, "query-timeout-enforcer");
            t.setDaemon(true);
            return t;
        });
        executor.setRemoveOnCancelPolicy(true);
        return executor;
    }
}
