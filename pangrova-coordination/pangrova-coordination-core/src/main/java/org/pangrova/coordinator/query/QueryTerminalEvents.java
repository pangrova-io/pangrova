package org.pangrova.coordinator.query;

import org.pangrova.client.core.error.ErrorDetail;
import org.pangrova.spi.observability.LongCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Emits logical terminal telemetry once; no event constitutes physical cleanup evidence. */
final class QueryTerminalEvents {
    private static final Logger LOG = LoggerFactory.getLogger(QueryTerminalEvents.class);
    private final QueryAuditDispatcher audit;
    private final QueryAdmission admission;
    private final LongCounter completed;
    private final LongCounter failed;

    QueryTerminalEvents(QueryAuditDispatcher audit, QueryAdmission admission,
            LongCounter completed, LongCounter failed) {
        this.audit = audit;
        this.admission = admission;
        this.completed = completed;
        this.failed = failed;
    }

    void emit(QueryTerminalCleanup.Context context) {
        String queryId = context.queryId();
        if (context.state().getState() == QueryStateMachine.State.FINISHED) {
            effect(queryId, "completed_metric", () -> increment(completed));
            effect(queryId, "admission", () -> admission.recordOutcome(true));
            LOG.info("event=query_completed queryId={}", queryId);
            effect(queryId, "audit", () -> audit.emitAuditCompleted(queryId, context.state()));
        } else {
            unsuccessful(context);
        }
    }

    private void unsuccessful(QueryTerminalCleanup.Context context) {
        String queryId = context.queryId();
        effect(queryId, "failed_metric", () -> increment(failed));
        effect(queryId, "admission", () -> admission.recordOutcome(
                context.state().getState() == QueryStateMachine.State.CANCELLED));
        if (context.state().getState() == QueryStateMachine.State.FAILED) {
            ErrorDetail error = context.state().getError().orElse(null);
            LOG.warn("event=query_failed queryId={} reason={} trigger={}", queryId,
                    error != null ? error.message() : "unknown", context.reason());
            effect(queryId, "audit", () -> audit.emitAuditFailed(queryId, context.state(), error));
        } else {
            LOG.info("event=query_cancelled queryId={}", queryId);
            effect(queryId, "audit", () -> audit.emitAuditCancelled(queryId, context.state()));
        }
    }

    private static void increment(LongCounter counter) {
        if (counter != null) {
            counter.add(1);
        }
    }

    private static void effect(String queryId, String name, Runnable operation) {
        try {
            operation.run();
        } catch (RuntimeException failure) {
            LOG.warn("event=query_terminal_effect_failed queryId={} effect={}", queryId, name, failure);
        }
    }
}
