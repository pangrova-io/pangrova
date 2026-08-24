package org.pangrova.coordinator.query.execution.phase;

import java.util.function.Function;

import org.pangrova.coordinator.query.execution.dispatch.DispatchResult;
import org.pangrova.coordinator.query.execution.result.QueryCacheService;
import org.pangrova.planner.PlannedQuery;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Phase 4: Distributed query dispatch with cache writeback.
 * Delegates the compiled plan to the existing dispatch infrastructure
 * (constant-fold, LIMIT 0, or DAG dispatch via worker nodes).
 * After successful dispatch, writes result to cache if a cache key is present.
 * This phase is always the last in the pipeline.
 */
public final class DistributedDispatchPhase implements QueryExecutionPhase {

    private final Function<QueryExecutionContext, DispatchResult> dispatchFunction;
    private final QueryCacheService cacheService;

    public DistributedDispatchPhase(
            Function<QueryExecutionContext, DispatchResult> dispatchFunction,
            QueryCacheService cacheService) {
        this.dispatchFunction = checkNotNull(dispatchFunction, "dispatchFunction");
        this.cacheService = cacheService;
    }

    @Override
    public PhaseResult execute(QueryExecutionContext context) {
        DispatchResult result = dispatchFunction.apply(context);
        writeCacheIfApplicable(context, result);
        return new PhaseResult.Terminal(result);
    }

    private void writeCacheIfApplicable(QueryExecutionContext context, DispatchResult result) {
        if (cacheService == null || context.cacheKey() == null || result == null) {
            return;
        }
        var cacheKey = cacheService.buildCacheKey(context.sql(), context.session());
        cacheService.writeback(cacheKey, context.session().queryId(),
                (PlannedQuery) context.compiledPlan(),
                QueryCacheService.principalOrAnon(context.session()));
    }

    @Override
    public String name() {
        return "distributed-dispatch";
    }
}
