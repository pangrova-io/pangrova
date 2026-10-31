package org.pangrova.coordinator.query.execution.result;

import org.pangrova.coordinator.query.cache.ResultCacheCoordinator;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.planner.cache.QueryResultCache;

/**
 * Coordinator-side invalidation facade for the legacy result-cache store.
 *
 * <p>The coordinator deliberately does not read or write cached result bytes.
 * Query rows are produced and served by the selected Worker endpoint. Until a
 * Worker-owned cache protocol exists, this facade is restricted to invalidation
 * so stale legacy entries cannot be observed by another component.</p>
 */
public final class QueryCacheService {

    private final ResultCacheCoordinator cacheCoordinator;

    public QueryCacheService(ResultCacheCoordinator cacheCoordinator) {
        this.cacheCoordinator = cacheCoordinator;
    }

    // ── Invalidation ──

    /**
     * Invalidates cache entries referencing the given table across L1, L2,
     * and cross-coordinator propagation.
     */
    public void invalidateByTable(String tableName) {
        if (cacheCoordinator != null) {
            cacheCoordinator.invalidateByTable(tableName);
        }
    }

    /**
     * Invalidates all cache entries. Used after DDL schema changes.
     * Clears L1 via the coordinator; L2 entries expire or are cleared
     * by table-level invalidation on subsequent mutations.
     */
    public void invalidateAll() {
        if (cacheCoordinator != null) {
            cacheCoordinator.invalidateAll();
        }
    }

    // ── Utilities ──

    /**
     * Resolves process-level invalidation without accessing a query planner.
     */
    public static QueryResultCache resolveCache(SqlCompiler compiler) {
        return compiler == null ? null : compiler.queryResultCache();
    }

}
