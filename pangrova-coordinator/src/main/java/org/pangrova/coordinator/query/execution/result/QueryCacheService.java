package org.pangrova.coordinator.query.execution.result;

import org.pangrova.coordinator.query.CoordinatorSessionContext;
import org.pangrova.coordinator.query.QueryStateMachine;
import org.pangrova.coordinator.query.cache.CacheWritebackConfig;
import org.pangrova.coordinator.query.cache.CacheWritebackHandler;
import org.pangrova.coordinator.query.cache.ResultCacheCoordinator;
import org.pangrova.coordinator.query.execution.dispatch.DispatchResult;
import org.pangrova.coordinator.result.CoordinatorResultService;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.planner.cache.CacheHint;
import org.pangrova.planner.cache.CacheKey;
import org.pangrova.planner.cache.QueryResultCache;
import org.pangrova.planner.PlannedQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unified cache service for query result caching.
 *
 * <p>Provides four operations: {@link #lookup}, {@link #writeback},
 * {@link #invalidateByTable}, and {@link #invalidateAll}. Internally
 * delegates to {@link ResultCacheCoordinator} for L2 and cross-coordinator
 * invalidation, keeping those details hidden from callers.</p>
 *
 * <p>Unified entry point for query result caching: lookup, writeback,
 * and invalidation (L1, L2, cross-coordinator propagation).</p>
 */
public final class QueryCacheService {

    private static final Logger LOG = LoggerFactory.getLogger(QueryCacheService.class);

    private final ResultCacheCoordinator cacheCoordinator;
    private final CacheWritebackHandler writebackHandler;
    private final CacheWritebackConfig writebackConfig;
    private final CoordinatorResultService resultService;

    /**
     * @param cacheCoordinator L1+L2+cross-coordinator cache facade (nullable — disables all caching)
     * @param writebackConfig  writeback toggle (nullable — defaults to disabled)
     * @param resultService    result buffer for reading back serialized bytes (nullable)
     */
    public QueryCacheService(ResultCacheCoordinator cacheCoordinator,
                             CacheWritebackConfig writebackConfig,
                             CoordinatorResultService resultService) {
        this.cacheCoordinator = cacheCoordinator;
        this.writebackConfig = writebackConfig != null
                ? writebackConfig : CacheWritebackConfig.DISABLED;
        this.resultService = resultService;
        QueryResultCache l1 = cacheCoordinator != null ? cacheCoordinator.l1Cache() : null;
        this.writebackHandler = l1 != null ? new CacheWritebackHandler(l1) : null;
    }

    // ── Lookup ──

    /**
     * Attempts a cache lookup for the given SQL and session.
     *
     * @return a {@link DispatchResult} on cache hit, or {@code null} on miss
     */
    public DispatchResult lookup(String sql, CoordinatorSessionContext session,
                                 QueryStateMachine sm) {
        if (cacheCoordinator == null || resultService == null) {
            return null;
        }
        QueryResultCache l1 = cacheCoordinator.l1Cache();
        if (l1 == null) {
            return null;
        }
        CacheKey cacheKey = buildCacheKey(sql, session);
        var cached = l1.get(cacheKey, CacheHint.NONE);
        if (cached.isPresent()) {
            LOG.info("event=cache_hit queryId={} principal={}",
                    session.queryId(), principalOrAnon(session));
            return DispatchResult.fromLocal(
                    resultService.dispatchLocal(
                            session.queryId(), cached.get().data(), sm.toFinisher()));
        }
        return null;
    }

    /**
     * Builds a cache key for the given SQL and session context.
     */
    public CacheKey buildCacheKey(String sql, CoordinatorSessionContext session) {
        return CacheKey.of(sql, session.tenantId(),
                principalOrAnon(session), session.sessionProperties(), null);
    }

    // ── Writeback ──

    /**
     * Writes the query result from the coordinator result buffer to the cache.
     * Fire-and-forget: failures are logged but never propagate.
     */
    public void writeback(CacheKey cacheKey, String queryId,
                          PlannedQuery plan, String principal) {
        if (!writebackConfig.enabled() || writebackHandler == null
                || resultService == null) {
            return;
        }
        try {
            byte[] resultBytes = resultService.getResultBytes(queryId);
            if (resultBytes != null) {
                writebackHandler.writeback(cacheKey, resultBytes, plan.tableReferences(),
                        principal, null);
            }
        } catch (Exception e) {
            LOG.warn("event=cache_writeback_skip queryId={} error={}",
                    queryId, e.getMessage());
        }
    }

    /**
     * Whether cache writeback is enabled and should be attempted after dispatch.
     */
    public boolean isWritebackEnabled() {
        return writebackConfig.enabled() && writebackHandler != null;
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
     * Resolves the {@link QueryResultCache} from the compiler's planner.
     */
    public static QueryResultCache resolveCache(SqlCompiler compiler) {
        if (compiler == null || compiler.planner() == null) {
            return null;
        }
        return compiler.planner().getQueryResultCache();
    }

    /**
     * Extracts the principal from the session, defaulting to "anonymous".
     */
    public static String principalOrAnon(CoordinatorSessionContext session) {
        return session.userIdentity() != null
                ? session.userIdentity().principal() : "anonymous";
    }
}
