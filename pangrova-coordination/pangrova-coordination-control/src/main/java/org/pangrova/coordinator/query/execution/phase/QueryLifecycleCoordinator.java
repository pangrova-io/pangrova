package org.pangrova.coordinator.query.execution.phase;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.pangrova.common.lifecycle.ResourceCloser;
import org.pangrova.execution.task.DispatchResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Coordinates query lifecycle: ensures onQueryStart/onQueryEnd are always paired.
 * Runs planning phases inline and observes the final phase until it settles.
 */
public final class QueryLifecycleCoordinator {

    private static final Logger LOG = LoggerFactory.getLogger(QueryLifecycleCoordinator.class);

    private final List<QueryExecutionPhase> phases;
    private final Runnable onQueryStart;
    private final Runnable onQueryEnd;

    public QueryLifecycleCoordinator(
            List<QueryExecutionPhase> phases,
            Runnable onQueryStart,
            Runnable onQueryEnd) {
        this.phases = List.copyOf(phases);
        this.onQueryStart = onQueryStart;
        this.onQueryEnd = onQueryEnd;
    }

    /**
     * Executes the phase pipeline with guaranteed lifecycle pairing.
     * Completes with null if no phase produces a terminal result.
     */
    public CompletionStage<DispatchResult> execute(QueryExecutionContext initialContext) {
        onQueryStart.run();
        try {
            return runPipeline(initialContext).whenComplete((result, failure) ->
                    ResourceCloser.closeAll(failure, onQueryEnd));
        } catch (RuntimeException | Error failure) {
            ResourceCloser.closeAll(failure, onQueryEnd);
            throw failure;
        }
    }

    private CompletionStage<DispatchResult> runPipeline(QueryExecutionContext ctx) {
        for (QueryExecutionPhase phase : phases) {
            PhaseResult result = phase.execute(ctx);
            if (result instanceof PhaseResult.Terminal t) {
                LOG.debug("event=phase_terminal phase={} queryId={}",
                        phase.name(), ctx.session().queryId());
                return t.completion();
            } else if (result instanceof PhaseResult.Continue c) {
                ctx = c.updated();
            }
        }
        LOG.warn("event=pipeline_no_terminal queryId={} phasesExecuted={}",
                ctx.session().queryId(), phases.size());
        return CompletableFuture.completedFuture(null);
    }
}
