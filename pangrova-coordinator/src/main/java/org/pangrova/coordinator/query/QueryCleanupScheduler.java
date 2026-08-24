package org.pangrova.coordinator.query;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.pangrova.coordinator.result.CoordinatorResultBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages periodic cleanup of terminal query state machines.
 * Extracted from QueryManager to enforce single-responsibility.
 *
 * <p>BUG-D46 fix: also removes the corresponding {@link CoordinatorResultBuffer}
 * entry when a terminal query is cleaned up, ensuring consistent state between
 * query status and result availability.</p>
 */
final class QueryCleanupScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(QueryCleanupScheduler.class);

    private volatile ScheduledFuture<?> cleanupFuture;
    private final ConcurrentHashMap<String, QueryStateMachine> queries;
    private final CoordinatorResultBuffer resultBuffer;

    QueryCleanupScheduler(ConcurrentHashMap<String, QueryStateMachine> queries,
                          CoordinatorResultBuffer resultBuffer) {
        this.queries = queries;
        this.resultBuffer = resultBuffer;
    }

    int cleanupTerminalQueries(Duration retention) {
        Instant cutoff = Instant.now().minus(retention);
        int[] removed = {0};
        queries.entrySet().removeIf(entry -> {
            boolean expired = entry.getValue().isTerminal()
                    && entry.getValue().getCreatedAt().isBefore(cutoff);
            if (expired) {
                // BUG-D46: unified cleanup — remove result buffer entry alongside query state
                String queryId = entry.getKey();
                if (resultBuffer != null) {
                    resultBuffer.remove(queryId);
                }
                removed[0]++;
            }
            return expired;
        });
        if (removed[0] > 0) {
            LOG.info("event=query_cleanup removed={} retention={}", removed[0], retention);
        }
        return removed[0];
    }

    void startPeriodicCleanup(ScheduledExecutorService executor,
                              Duration retention, Duration interval) {
        cleanupFuture = executor.scheduleAtFixedRate(
                () -> safeCleanup(retention),
                interval.toMillis(), interval.toMillis(), TimeUnit.MILLISECONDS);
        LOG.info("event=periodic_cleanup_start retention={} interval={}", retention, interval);
    }

    void stopPeriodicCleanup() {
        ScheduledFuture<?> future = cleanupFuture;
        if (future != null) {
            future.cancel(false);
        }
    }

    private void safeCleanup(Duration retention) {
        try {
            cleanupTerminalQueries(retention);
        } catch (Exception e) {
            LOG.warn("event=query_cleanup_error error={}", e.getMessage(), e);
        }
    }
}
