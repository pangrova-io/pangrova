package org.pangrova.coordinator.query.execution.phase;

import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.pangrova.execution.task.DispatchResult;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Final phase: distributed query dispatch.
 * Delegates the compiled plan to the existing dispatch infrastructure
 * Every relational query is dispatched through the Worker/Native path.
 * This phase is always the last in the pipeline.
 */
public final class DistributedDispatchPhase implements QueryExecutionPhase {

    private final Function<QueryExecutionContext, CompletionStage<DispatchResult>> dispatchFunction;

    public DistributedDispatchPhase(
            Function<QueryExecutionContext, CompletionStage<DispatchResult>> dispatchFunction) {
        this.dispatchFunction = checkNotNull(dispatchFunction, "dispatchFunction");
    }

    @Override
    public PhaseResult execute(QueryExecutionContext context) {
        return new PhaseResult.Terminal(dispatchFunction.apply(context));
    }

    @Override
    public String name() {
        return "distributed-dispatch";
    }
}
