package org.pangrova.coordinator.query.execution.phase;

import java.util.List;

import org.pangrova.execution.task.DispatchResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Coordinates query lifecycle: ensures onQueryStart/onQueryEnd are always paired.
 * Runs the phase pipeline and guarantees cleanup via try-finally.
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
     * Returns null if no phase produces a Terminal result.
     */
    public DispatchResult execute(QueryExecutionContext initialContext) {
        onQueryStart.run();
        try {
            return runPipeline(initialContext);
        } finally {
            onQueryEnd.run();
        }
    }

    private DispatchResult runPipeline(QueryExecutionContext ctx) {
        for (QueryExecutionPhase phase : phases) {
            PhaseResult result = phase.execute(ctx);
            if (result instanceof PhaseResult.Terminal t) {
                LOG.debug("event=phase_terminal phase={} queryId={}",
                        phase.name(), ctx.session().queryId());
                return t.result();
            } else if (result instanceof PhaseResult.Continue c) {
                ctx = c.updated();
            }
        }
        LOG.warn("event=pipeline_no_terminal queryId={} phasesExecuted={}",
                ctx.session().queryId(), phases.size());
        return null;
    }
}
