package org.pangrova.query.core.retention;

import org.pangrova.query.core.lifecycle.QueryTerminalCleanup;

import org.pangrova.query.core.lifecycle.QueryStateMachine;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Retains query history until logical publication and physical cleanup are complete.
 * Borrows the query registry and executors; cancelling its schedules does not close either.
 * Physical cleanup remains owned by {@link QueryTerminalCleanup}.
 */
@org.pangrova.common.api.annotation.Internal
public final class QueryRetentionScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(QueryRetentionScheduler.class);

    private volatile ScheduledFuture<?> cleanupFuture;
    private volatile ScheduledFuture<?> completionFuture;
    private static final long COMPLETION_RETRY_MILLIS = 100;
    private final java.util.concurrent.atomic.AtomicBoolean completionRunning =
            new java.util.concurrent.atomic.AtomicBoolean();
    private static final int MAX_CONCURRENT_SWEEPS = 2;
    private final AtomicInteger cleanupRunning = new AtomicInteger();
    private final ConcurrentHashMap<String, QueryStateMachine> queries;
    private final Predicate<String> hasPendingCompletion;
    private final Runnable retryPendingCompletions;

    /** Borrows the authoritative concurrent registry; eviction must update that same registry. */
    public QueryRetentionScheduler(ConcurrentHashMap<String, QueryStateMachine> queries,
                          Predicate<String> hasPendingCompletion,
                          Runnable retryPendingCompletions) {
        this.queries = queries;
        this.hasPendingCompletion = hasPendingCompletion;
        this.retryPendingCompletions = retryPendingCompletions;
    }

    /** Retries completion and evicts by creation-time retention only after terminal effects and physical cleanup. */
    public int cleanupTerminalQueries(Duration retention) {
        retryPendingCompletions.run();
        Instant cutoff = Instant.now().minus(retention);
        int[] removed = {0};
        queries.entrySet().removeIf(entry -> {
            boolean expired = entry.getValue().isTerminal()
                    // Terminal publication precedes QueryManager's journal/effect
                    // processing. Never evict in that small transition window.
                    && entry.getValue().terminalEffectsApplied()
                    && entry.getValue().physicalCleanupApplied()
                    && !hasPendingCompletion.test(entry.getKey())
                    && entry.getValue().getCreatedAt().isBefore(cutoff);
            if (expired) {
                removed[0]++;
            }
            return expired;
        });
        if (removed[0] > 0) {
            LOG.info("event=query_cleanup removed={} retention={}", removed[0], retention);
        }
        return removed[0];
    }

    /** Registers retention and completion-retry timers on caller-owned executors. */
    public void startPeriodicCleanup(ScheduledExecutorService executor,
                              Executor cleanupExecutor,
                              Duration retention, Duration interval) {
        completionFuture = executor.scheduleWithFixedDelay(
                () -> submitCompletionRetry(cleanupExecutor), COMPLETION_RETRY_MILLIS,
                COMPLETION_RETRY_MILLIS, TimeUnit.MILLISECONDS);
        cleanupFuture = executor.scheduleAtFixedRate(
                () -> submitCleanup(cleanupExecutor, retention),
                interval.toMillis(), interval.toMillis(), TimeUnit.MILLISECONDS);
        LOG.info("event=periodic_cleanup_start retention={} interval={}", retention, interval);
    }

    /** Cancels registered timers without interrupting running work or shutting down borrowed executors. */
    public void stopPeriodicCleanup() {
        ScheduledFuture<?> completion = completionFuture;
        if (completion != null) completion.cancel(false);
        ScheduledFuture<?> future = cleanupFuture;
        if (future != null) {
            future.cancel(false);
        }
    }

    private void submitCompletionRetry(Executor executor) {
        if (!completionRunning.compareAndSet(false, true)) return;
        try {
            executor.execute(() -> {
                try {
                    // QueryTerminalCleanup enforces its per-tick attempt cap and backoff.
                    // This path must not scan/evict retained query history.
                    retryPendingCompletions.run();
                } catch (RuntimeException failure) {
                    LOG.warn("event=query_completion_retry_error", failure);
                } finally {
                    completionRunning.set(false);
                }
            });
        } catch (RejectedExecutionException rejected) {
            completionRunning.set(false);
            LOG.warn("event=query_completion_retry_rejected", rejected);
        }
    }

    private void safeCleanup(Duration retention) {
        try {
            cleanupTerminalQueries(retention);
        } catch (Exception e) {
            LOG.warn("event=query_cleanup_error error={}", e.getMessage(), e);
        }
    }

    private void submitCleanup(Executor cleanupExecutor, Duration retention) {
        while (true) {
            int current = cleanupRunning.get();
            if (current >= MAX_CONCURRENT_SWEEPS) {
                return;
            }
            if (cleanupRunning.compareAndSet(current, current + 1)) {
                break;
            }
        }
        try {
            cleanupExecutor.execute(() -> {
                try {
                    safeCleanup(retention);
                } finally {
                    cleanupRunning.decrementAndGet();
                }
            });
        } catch (RejectedExecutionException rejected) {
            cleanupRunning.decrementAndGet();
            LOG.warn("event=query_cleanup_dispatch_rejected error={}", rejected.getMessage());
        }
    }
}
