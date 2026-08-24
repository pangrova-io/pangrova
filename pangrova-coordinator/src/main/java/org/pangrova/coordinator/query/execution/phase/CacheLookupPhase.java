package org.pangrova.coordinator.query.execution.phase;

import org.pangrova.coordinator.query.execution.dispatch.DispatchResult;
import org.pangrova.coordinator.query.execution.result.QueryCacheService;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Phase 3: Cache lookup.
 * Short-circuits with cached result if available.
 * Skipped when time-travel overrides are present (cache key cannot distinguish snapshots).
 */
public final class CacheLookupPhase implements QueryExecutionPhase {

    private final QueryCacheService cacheService;

    public CacheLookupPhase(QueryCacheService cacheService) {
        this.cacheService = checkNotNull(cacheService, "cacheService");
    }

    @Override
    public PhaseResult execute(QueryExecutionContext context) {
        if (!context.snapshotOverrides().isEmpty()) {
            return new PhaseResult.Continue(context);
        }
        DispatchResult cacheHit = cacheService.lookup(
                context.sql(), context.session(), context.stateMachine());
        if (cacheHit != null) {
            return new PhaseResult.Terminal(cacheHit);
        }
        String cacheKey = cacheService.isWritebackEnabled()
                ? cacheService.buildCacheKey(context.sql(), context.session()).keyString()
                : null;
        return new PhaseResult.Continue(
                context.withStrippedSql(context.sql(), cacheKey));
    }

    @Override
    public String name() {
        return "cache-lookup";
    }
}
