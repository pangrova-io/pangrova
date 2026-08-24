package org.pangrova.worker.buffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.pangrova.common.concurrent.ThreadSafe;

/**
 * Schedules result expiration for finished queries.
 * After the retention period, invokes the cleanup callback and marks the query as expired.
 * Callers should cancel expiration when results are fetched to prevent premature cleanup.
 *
 * <p>Concurrency strategy: ConcurrentHashMap for pending expirations,
 * ConcurrentHashMap.newKeySet for expired queries set.</p>
 */
@ThreadSafe
public class ResultExpirationScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(ResultExpirationScheduler.class);

    private final ScheduledExecutorService scheduler;
    private final Duration retentionPeriod;
    private final Consumer<String> expirationCallback;
    private final ConcurrentHashMap<String, ScheduledFuture<?>> pendingExpirations = new ConcurrentHashMap<>();
    private final Set<String> expiredQueries = ConcurrentHashMap.newKeySet();

    public ResultExpirationScheduler(ScheduledExecutorService scheduler,
                                     Duration retentionPeriod,
                                     Consumer<String> expirationCallback) {
        this.scheduler = scheduler;
        this.retentionPeriod = retentionPeriod;
        this.expirationCallback = expirationCallback;
        // Governance: prevent cancelled futures from accumulating in the delay queue.
        if (scheduler instanceof java.util.concurrent.ScheduledThreadPoolExecutor stpe) {
            stpe.setRemoveOnCancelPolicy(true);
        }
    }

    /**
     * Schedules expiration for the given query after the retention period.
     * If already scheduled or expired, this call is a no-op.
     */
    public void scheduleExpiration(String queryId) {
        if (expiredQueries.contains(queryId)) {
            return;
        }
        pendingExpirations.computeIfAbsent(queryId, id -> {
            LOG.info("event=result_expiration_scheduled queryId={} retention={}", id, retentionPeriod);
            return scheduler.schedule(() -> expire(id),
                    retentionPeriod.toMillis(), TimeUnit.MILLISECONDS);
        });
    }

    /**
     * Cancels a pending expiration. Called when the client fetches results before expiry.
     */
    public void cancelExpiration(String queryId) {
        ScheduledFuture<?> future = pendingExpirations.remove(queryId);
        if (future != null) {
            future.cancel(false);
            LOG.info("event=result_expiration_cancelled queryId={}", queryId);
        }
    }

    /**
     * Returns true if the query's results have already expired.
     */
    public boolean isExpired(String queryId) {
        return expiredQueries.contains(queryId);
    }

    private void expire(String queryId) {
        pendingExpirations.remove(queryId);
        expiredQueries.add(queryId);
        LOG.info("event=result_expired queryId={}", queryId);
        expirationCallback.accept(queryId);
    }
}
