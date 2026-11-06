package org.pangrova.coordinator.query.execution.phase;

import java.util.function.Function;

import org.pangrova.query.core.task.DispatchResult;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Final phase: distributed query dispatch.
 * Delegates the compiled plan to the existing dispatch infrastructure
 * Every relational query is dispatched through the Worker/Native path.
 * This phase is always the last in the pipeline.
 */
public final class DistributedDispatchPhase implements QueryExecutionPhase {

    private final Function<QueryExecutionContext, DispatchResult> dispatchFunction;

    public DistributedDispatchPhase(
            Function<QueryExecutionContext, DispatchResult> dispatchFunction) {
        this.dispatchFunction = checkNotNull(dispatchFunction, "dispatchFunction");
    }

    @Override
    public PhaseResult execute(QueryExecutionContext context) {
        DispatchResult result = dispatchFunction.apply(context);
        return new PhaseResult.Terminal(result);
    }

    @Override
    public String name() {
        return "distributed-dispatch";
    }
}
