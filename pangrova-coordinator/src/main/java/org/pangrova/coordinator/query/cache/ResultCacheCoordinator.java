package org.pangrova.coordinator.query.cache;

import org.pangrova.metadata.core.cache.InvalidationEntry.InvalidationType;
import org.pangrova.metadata.core.cache.JdbcInvalidationPoller;
import org.pangrova.planner.cache.CacheEntryMetadata;
import org.pangrova.planner.cache.CacheKey;
import org.pangrova.planner.cache.CachedResult;
import org.pangrova.planner.cache.L2CacheStore;
import org.pangrova.planner.cache.QueryResultCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Set;

/**
 * Unified facade for all query result cache operations.
 * Consolidates L1 (JVM heap), L2 (Redis), and cross-coordinator
 * invalidation propagation into a single entry point.
 *
 * <p>All cache mutations (write, invalidate) go through this class.
 * Other components should not call QueryResultCache or L2CacheStore directly.</p>
 *
 * <p>Invalidation flow on table mutation:
 * <ol>
 *   <li>Clear L1 (QueryResultCache.invalidateByTable)</li>
 *   <li>Clear L2 (L2CacheStore.removeByTable)</li>
 *   <li>Write invalidation_log (RESULT_CACHE type) for cross-coordinator propagation</li>
 * </ol>
 * Other coordinators pick up the invalidation_log entry via JdbcInvalidationPoller
 * and clear their own L1. They do NOT clear L2 — the originating coordinator
 * already did that (single-writer model).</p>
 */
public final class ResultCacheCoordinator {

    private static final Logger LOG = LoggerFactory.getLogger(ResultCacheCoordinator.class);

    private final QueryResultCache l1Cache;
    private final L2CacheStore l2Store;
    private final JdbcInvalidationPoller invalidationPoller;

    /**
     * @param l1Cache            L1 JVM heap cache (nullable — disables caching)
     * @param l2Store            L2 external store, e.g. Redis (nullable — L2 disabled)
     * @param invalidationPoller JDBC poller for cross-coordinator propagation (nullable)
     */
    public ResultCacheCoordinator(QueryResultCache l1Cache,
                                  L2CacheStore l2Store,
                                  JdbcInvalidationPoller invalidationPoller) {
        this.l1Cache = l1Cache;
        this.l2Store = l2Store;
        this.invalidationPoller = invalidationPoller;
        wirePollerCallback();
    }

    private void wirePollerCallback() {
        if (invalidationPoller != null && l1Cache != null) {
            invalidationPoller.setResultCacheInvalidator(tableName -> {
                l1Cache.invalidateByTable(tableName);
                LOG.debug("event=cross_coordinator_l1_invalidation table={}", tableName);
            });
        }
    }

    // ── Write path ──

    /**
     * Writes a query result to both L1 and L2 with metadata for precise invalidation.
     *
     * @param key              isolation-aware cache key
     * @param resultBytes      serialized columnar IPC result
     * @param referencedTables table names referenced by the query (for invalidation matching)
     * @param principal        user who executed the query
     */
    public void put(CacheKey key, byte[] resultBytes,
                    Set<String> referencedTables, String principal) {
        if (l1Cache == null || resultBytes == null) {
            return;
        }
        CacheEntryMetadata metadata = new CacheEntryMetadata(
                referencedTables, principal != null ? principal : "anonymous", "");
        CachedResult result = new CachedResult(resultBytes, Instant.now());
        l1Cache.put(key, result, metadata);
        if (l2Store != null) {
            l2Store.put(key.keyString(), result);
        }
        LOG.debug("event=result_cache_put tables={}", referencedTables);
    }

    // ── Invalidation path ──

    /**
     * Invalidates all cached results referencing the given table.
     * Clears L1, L2, and writes invalidation_log for cross-coordinator propagation.
     */
    public void invalidateByTable(String tableName) {
        if (l1Cache != null) {
            l1Cache.invalidateByTable(tableName);
        }
        invalidateL2WithFallback(tableName);
        publishInvalidation(tableName);
        LOG.info("event=result_cache_invalidate_table table={}", tableName);
    }

    private void invalidateL2WithFallback(String tableName) {
        if (l2Store == null) {
            return;
        }
        try {
            l2Store.removeByTable(tableName);
        } catch (Exception e) {
            if (l1Cache != null) {
                l1Cache.invalidateAll();
            }
            LOG.warn("event=l2_invalidation_failed table={} error={} l1_force_invalidated={}",
                    tableName, e.getMessage(), l1Cache != null);
        }
    }

    /**
     * Invalidates all cached results for the given user.
     */
    public void invalidateByUser(String userId) {
        if (l1Cache != null) {
            l1Cache.invalidateByUser(userId);
        }
        if (l2Store != null) {
            l2Store.removeByUser(userId);
        }
        LOG.info("event=result_cache_invalidate_user userId={}", userId);
    }

    /**
     * Invalidates all cached results. Used after DDL schema changes.
     */
    public void invalidateAll() {
        if (l1Cache != null) {
            l1Cache.invalidateAll();
        }
        LOG.info("event=result_cache_invalidate_all");
    }

    /**
     * Returns the underlying L1 cache for read-only operations (cache lookup).
     */
    public QueryResultCache l1Cache() {
        return l1Cache;
    }

    // ── Cross-coordinator propagation ──

    private void publishInvalidation(String tableName) {
        if (invalidationPoller == null) {
            return;
        }
        try {
            invalidationPoller.writeInvalidation(tableName, InvalidationType.RESULT_CACHE);
        } catch (Exception e) {
            LOG.warn("event=result_cache_invalidation_publish_failed table={} error={}",
                    tableName, e.getMessage());
        }
    }
}
