package org.pangrova.coordinator.query;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.protocol.rest.error.ErrorDetail;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Periodic task scanning active queries, failing those exceeding timeout.
 * Uses a single daemon thread via ScheduledExecutorService.
 *
 * <p>Concurrency strategy: volatile for scheduled task reference;
 * scheduler is thread-safe by contract. Start/stop are expected
 * to be called from the bootstrap thread.</p>
 */
@ThreadSafe
public class QueryTimeoutEnforcer {

    private static final Logger LOG = LoggerFactory.getLogger(QueryTimeoutEnforcer.class);
    private static final String TIMEOUT_ERROR_CODE = "QUERY_TIMEOUT";

    private final Duration timeout;
    private final Duration checkInterval;
    private final ScheduledExecutorService scheduler;
    private volatile ScheduledFuture<?> scheduledTask;

    public QueryTimeoutEnforcer(Duration timeout, Duration checkInterval) {
        this(timeout, checkInterval, createDaemonScheduler());
    }

    QueryTimeoutEnforcer(Duration timeout, Duration checkInterval,
                         ScheduledExecutorService scheduler) {
        this.timeout = Objects.requireNonNull(timeout, "timeout must not be null");
        this.checkInterval = Objects.requireNonNull(checkInterval, "checkInterval must not be null");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler must not be null");
    }

    /**
     * Starts periodic timeout checks against queries provided by the supplier.
     * Terminal action is delegated to the provided callback instead of directly
     * mutating query state machines.
     *
     * @param queryProvider supplier of active query state machines
     */
    public void start(Supplier<Map<String, QueryStateMachine>> queryProvider) {
        Objects.requireNonNull(queryProvider, "queryProvider must not be null");
        scheduledTask = scheduler.scheduleAtFixedRate(
                () -> enforceTimeouts(queryProvider),
                checkInterval.toMillis(),
                checkInterval.toMillis(),
                TimeUnit.MILLISECONDS);
        LOG.info("event=timeout_enforcer_start timeout={} interval={}", timeout, checkInterval);
    }

    /**
     * Callback invoked when a query exceeds its timeout.
     * Allows the caller (QueryManager) to handle terminal transitions
     * with full cleanup/audit/metrics semantics.
     */
    @FunctionalInterface
    public interface TimeoutCallback {
        void onQueryTimeout(String queryId, ErrorDetail error);
    }

    private volatile TimeoutCallback timeoutCallback;

    /**
     * Registers a callback for timeout-triggered query termination.
     * When set, the enforcer delegates terminal action to this callback
     * instead of directly transitioning the state machine.
     *
     * @param callback the timeout handler, typically QueryManager.onQueryTimeout
     */
    public void setTimeoutCallback(TimeoutCallback callback) {
        this.timeoutCallback = callback;
    }

    /**
     * Shuts down the scheduler and cancels the periodic task.
     */
    public void stop() {
        cancelScheduledTask();
        scheduler.shutdown();
    }

    private void enforceTimeouts(Supplier<Map<String, QueryStateMachine>> queryProvider) {
        try {
            doEnforceTimeouts(queryProvider.get());
        } catch (Exception e) {
            LOG.warn("event=timeout_enforcement_error error={}", e.getMessage(), e);
        }
    }

    private void doEnforceTimeouts(Map<String, QueryStateMachine> queries) {
        Instant deadline = Instant.now().minus(timeout);
        for (Map.Entry<String, QueryStateMachine> entry : queries.entrySet()) {
            failIfExpired(entry.getKey(), entry.getValue(), deadline);
        }
    }

    private void failIfExpired(String queryId, QueryStateMachine sm, Instant deadline) {
        if (sm.isTerminal() || sm.getCreatedAt().isAfter(deadline)) {
            return;
        }
        ErrorDetail error = new ErrorDetail(
                TIMEOUT_ERROR_CODE, "TIMEOUT",
                "Query " + queryId + " exceeded timeout of " + timeout);
        applyTimeout(queryId, sm, error);
    }

    private void applyTimeout(String queryId, QueryStateMachine sm, ErrorDetail error) {
        TimeoutCallback cb = this.timeoutCallback;
        if (cb != null) {
            cb.onQueryTimeout(queryId, error);
            LOG.warn("event=query_timeout queryId={} timeout={} delegated=true", queryId, timeout);
            return;
        }
        sm.tryTransitionToFailed(error);
        LOG.warn("event=query_timeout queryId={} timeout={} delegated=false", queryId, timeout);
    }

    private void cancelScheduledTask() {
        ScheduledFuture<?> task = scheduledTask;
        if (task != null) {
            task.cancel(false);
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
