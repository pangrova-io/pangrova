package org.pangrova.query.runtime;

import org.pangrova.query.lifecycle.QueryTerminalCleanup;

import org.pangrova.query.api.enums.QueryState;

import org.pangrova.query.api.error.QueryFailure;
import org.pangrova.observability.api.metrics.LongCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Emits logical terminal telemetry once; no event constitutes physical cleanup evidence. */
final class QueryTerminalEvents {
    private static final Logger LOG = LoggerFactory.getLogger(QueryTerminalEvents.class);
    private final org.pangrova.query.lifecycle.QueryAuditPort audit;
    private final org.pangrova.query.lifecycle.QueryAdmissionPort admission;
    private final LongCounter completed;
    private final LongCounter failed;

    QueryTerminalEvents(org.pangrova.query.lifecycle.QueryAuditPort audit,
            org.pangrova.query.lifecycle.QueryAdmissionPort admission,
            LongCounter completed, LongCounter failed) {
        this.audit = audit;
        this.admission = admission;
        this.completed = completed;
        this.failed = failed;
    }

    void emit(QueryTerminalCleanup.Context context) {
        String queryId = context.queryId();
        QueryState state = context.state().getState();
        QueryFailure failure = context.state().getError().orElse(null);
        if (context.state().takeAdmissionHealthProbe()) {
            effect(queryId, "admission", () -> admission.recordOutcome(state, failure));
        }
        if (state == QueryState.FINISHED) {
            effect(queryId, "completed_metric", () -> increment(completed));
            LOG.info("event=query_completed queryId={}", queryId);
            effect(queryId, "audit", () -> audit.emitAuditCompleted(queryId, context.state()));
        } else {
            unsuccessful(context, failure);
        }
    }

    private void unsuccessful(QueryTerminalCleanup.Context context, QueryFailure error) {
        String queryId = context.queryId();
        effect(queryId, "failed_metric", () -> increment(failed));
        if (context.state().getState() == QueryState.FAILED) {
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
