package org.pangrova.worker.exchange.source;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.pangrova.common.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracks upstream commit notifications and provides future-based blocking
 * for exchange sources waiting on upstream partition commits.
 *
 * <p>When a downstream exchange source needs data from an upstream stage that
 * has not yet committed, it calls {@link #waitForCommit(String, int, Duration)}
 * which returns a {@link CompletableFuture} that completes when the upstream
 * commits. This eliminates the 30-minute polling timeout by providing an
 * event-driven notification mechanism.</p>
 *
 * <p>Thread safety: all internal state is managed via concurrent collections
 * and CompletableFuture's built-in thread safety.</p>
 */
@ThreadSafe
public final class ExchangeSourceRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(ExchangeSourceRegistry.class);

    /** Default max wait duration for commit notification (5 minutes). */
    static final Duration DEFAULT_MAX_WAIT = Duration.ofMinutes(5);

    private final Set<String> committedUpstreams = ConcurrentHashMap.newKeySet();

    /**
     * Maps upstream key (queryId:stageId) to a shared future that completes
     * when the upstream commits. Multiple waiters on the same upstream share
     * the same future instance.
     */
    private final ConcurrentMap<String, CompletableFuture<Void>> commitFutures =
            new ConcurrentHashMap<>();

    /**
     * Records that the upstream stage has committed and resolves all waiting futures.
     */
    public void notifyCommit(String queryId, int upstreamStageId) {
        String key = toKey(queryId, upstreamStageId);
        committedUpstreams.add(key);

        CompletableFuture<Void> future = commitFutures.remove(key);
        if (future != null) {
            future.complete(null);
            LOG.debug("event=commit_future_resolved queryId={} upstreamStageId={}", queryId, upstreamStageId);
        }

        LOG.debug("event=upstream_commit_notified queryId={} upstreamStageId={}",
                queryId, upstreamStageId);
    }

    /**
     * Returns true if the upstream stage has already committed.
     */
    public boolean isCommitted(String queryId, int upstreamStageId) {
        return committedUpstreams.contains(toKey(queryId, upstreamStageId));
    }

    /**
     * Returns a future that completes when the upstream stage commits.
     * If the upstream has already committed, returns an already-completed future.
     *
     * <p>The returned future will be completed exceptionally with
     * {@link TimeoutException} if the commit does not arrive within
     * {@code maxWait}.</p>
     *
     * @param queryId         the query identifier
     * @param upstreamStageId the upstream stage identifier
     * @param maxWait         maximum duration to wait for the commit
     * @return a future that completes when the upstream commits
     */
    public CompletableFuture<Void> waitForCommit(String queryId, int upstreamStageId, Duration maxWait) {
        String key = toKey(queryId, upstreamStageId);

        // Fast path: already committed
        if (committedUpstreams.contains(key)) {
            return CompletableFuture.completedFuture(null);
        }

        CompletableFuture<Void> future = commitFutures.computeIfAbsent(key, k -> new CompletableFuture<>());

        // Double-check after registration to avoid race with notifyCommit
        if (committedUpstreams.contains(key)) {
            future.complete(null);
            commitFutures.remove(key);
            return future;
        }

        Duration effectiveWait = maxWait != null ? maxWait : DEFAULT_MAX_WAIT;
        CompletableFuture<Void> timedFuture = future.orTimeout(effectiveWait.toMillis(), TimeUnit.MILLISECONDS);

        // Ensure upstreamCommitted is updated when future completes successfully
        timedFuture.whenComplete((result, throwable) -> {
            if (throwable == null) {
                committedUpstreams.add(key);
            }
            commitFutures.remove(key);
        });

        return timedFuture;
    }

    /**
     * Convenience overload using the default max wait duration.
     */
    public CompletableFuture<Void> waitForCommit(String queryId, int upstreamStageId) {
        return waitForCommit(queryId, upstreamStageId, DEFAULT_MAX_WAIT);
    }

    /**
     * Removes all entries for the given query. Called on query cleanup.
     * Cancels any pending futures for the query.
     */
    public void cleanup(String queryId) {
        String prefix = queryId + ":";
        committedUpstreams.removeIf(k -> k.startsWith(prefix));

        commitFutures.entrySet().removeIf(entry -> {
            if (entry.getKey().startsWith(prefix)) {
                entry.getValue().cancel(false);
                return true;
            }
            return false;
        });
    }

    private static String toKey(String queryId, int upstreamStageId) {
        return queryId + ":" + upstreamStageId;
    }
}
