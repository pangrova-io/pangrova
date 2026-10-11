package org.pangrova.coordinator.query;

import java.util.List;
import java.util.Optional;

import org.pangrova.coordinator.query.audit.QueryAuditEmitter;
import org.pangrova.coordinator.query.audit.QueryLineageInfo;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.governance.lineage.LineageEventEmitter;
import org.pangrova.spi.coordination.QueryLineageFact;
import org.pangrova.observability.eventlistener.QueryEventDispatcher;
import org.pangrova.protocol.rest.error.ErrorDetail;
import org.pangrova.security.audit.AuditEventEmitter;
import org.pangrova.spi.execution.QueryState;
import org.pangrova.spi.execution.eventlistener.QueryCompletedEvent;
import org.pangrova.spi.execution.eventlistener.QueryCreatedEvent;
import org.pangrova.spi.execution.eventlistener.QueryLineageMetadata;
import org.pangrova.spi.execution.eventlistener.QueryStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Logical query admission and terminal telemetry; task completion never publishes query lineage. */
final class QueryAuditDispatcher {
    private static final Logger LOG = LoggerFactory.getLogger(QueryAuditDispatcher.class);
    private final QueryAuditEmitter auditEmitter;
    private final QueryDispatcher queryDispatcher;
    private final QueryEventDispatcher events;
    private final boolean lineageEnabled;
    private final LineageEventEmitter lineageEmitter;

    QueryAuditDispatcher(QueryAuditEmitter auditEmitter, LineageEventEmitter lineageEmitter,
            QueryDispatcher queryDispatcher) {
        this(auditEmitter, lineageEmitter, queryDispatcher, QueryEventDispatcher.of());
    }

    QueryAuditDispatcher(QueryAuditEmitter auditEmitter, LineageEventEmitter lineageEmitter,
            QueryDispatcher queryDispatcher, QueryEventDispatcher events) {
        this.auditEmitter = auditEmitter;
        this.queryDispatcher = queryDispatcher;
        QueryEventDispatcher parent = events == null ? QueryEventDispatcher.of() : events;
        lineageEnabled = lineageEmitter != null;
        this.lineageEmitter = lineageEmitter;
        this.events = parent;
    }

    void emitAuditSubmitted(String queryId, String sql, CoordinatorSessionContext session, QueryStateMachine state) {
        String principal = session.userIdentity() == null ? "anonymous" : session.userIdentity().principal();
        if (lineageEnabled) {
            QueryLineageFact fact = state.freezeLineage(QueryLineageFact.Phase.START,
                    () -> new QueryLineageFact(lineageEmitter.namespace(), queryId, QueryLineageFact.Phase.START,
                            state.getCreatedAt().toEpochMilli(), sql, null, QueryLineageMetadata.empty()));
            lineageEmitter.record(fact);
        }
        events.fireQueryCreated(new QueryCreatedEvent(queryId, sql, principal, state.getCreatedAt().toEpochMilli()));
        if (auditEmitter != null) auditEmitter.emitQuerySubmitted(queryId, sql, session);
    }

    void emitAuditCompleted(String queryId, QueryStateMachine state) {
        emitTerminal(queryId, state, QueryState.FINISHED, Optional.empty());
        if (auditEmitter != null) auditEmitter.emitQueryCompleted(queryId, state.getDurationMs(), subject(state));
    }

    void emitAuditFailed(String queryId, QueryStateMachine state, ErrorDetail error) {
        String message = error == null ? "unknown" : error.message();
        emitTerminal(queryId, state, QueryState.FAILED, Optional.of(message));
        if (auditEmitter != null) auditEmitter.emitQueryFailed(queryId, message, state.getDurationMs(), subject(state));
    }

    void emitAuditCancelled(String queryId, QueryStateMachine state) {
        emitTerminal(queryId, state, QueryState.CANCELLED, Optional.empty());
    }

    void emitTableAccessedAudit(String queryId, String catalog, String schema,
            String table, CoordinatorSessionContext session) {
        if (auditEmitter != null) auditEmitter.emitTableAccessed(queryId, catalog, schema, table, session);
    }

    QueryLineageMetadata captureTerminalLineage(String queryId) {
        if (!lineageEnabled && events.isEmpty()) {
            LOG.debug("event=lineage_skip reason=governance_disabled queryId={}", queryId);
            return QueryLineageMetadata.empty();
        }
        QueryLineageInfo info = queryDispatcher.removeLineageInfo(queryId);
        if (info == null) return QueryLineageMetadata.empty();
        var inputs = info.sourceTables().stream().map(name -> new QueryLineageMetadata.Dataset(
                "pangrova", name, List.of())).toList();
        var columns = info.columnDerivations().stream().map(derivation -> new QueryLineageMetadata.Column(
                derivation.outputColumn(), derivation.sourceColumns().stream().map(source ->
                        new QueryLineageMetadata.SourceColumn("pangrova", source.table(), source.column())).toList(),
                switch (derivation.expression()) {
                    case DIRECT -> QueryLineageMetadata.DerivationKind.DIRECT;
                    case TRANSFORM -> QueryLineageMetadata.DerivationKind.TRANSFORM;
                    case AGGREGATE -> QueryLineageMetadata.DerivationKind.AGGREGATE;
                }))
                .toList();
        var outputs = info.targetTables().stream().map(name -> new QueryLineageMetadata.Dataset(
                "pangrova", name, columns)).toList();
        return new QueryLineageMetadata(inputs, outputs);
    }

    QueryLineageFact terminalFact(String queryId, QueryStateMachine state, QueryStateMachine.State target) {
        if (!lineageEnabled) return null;
        QueryLineageFact.Phase phase = switch (target) {
            case FINISHED -> QueryLineageFact.Phase.COMPLETE;
            case FAILED -> QueryLineageFact.Phase.FAIL;
            case CANCELLED -> QueryLineageFact.Phase.ABORT;
            default -> throw new IllegalArgumentException("Lineage requires a terminal query state");
        };
        return state.freezeLineage(phase, () -> new QueryLineageFact(lineageEmitter.namespace(), queryId, phase,
                System.currentTimeMillis(), null, phase == QueryLineageFact.Phase.FAIL
                        ? state.getError().map(ErrorDetail::message).orElse("unknown") : null, state.lineageMetadata()));
    }

    /** Unadmitted failures also require a committed fact before logical cleanup can be released. */
    void persistTerminalLineage(String queryId, QueryStateMachine state) {
        QueryLineageFact fact = terminalFact(queryId, state, state.getState());
        if (fact != null) {
            QueryLineageFact start = state.lineageStart();
            if (start == null) throw new IllegalStateException("Query has no frozen START lineage fact");
            lineageEmitter.record(start);
            lineageEmitter.record(fact);
        }
    }

    private void emitTerminal(String queryId, QueryStateMachine state, QueryState finalState,
            Optional<String> error) {
        QueryLineageFact fact = terminalFact(queryId, state, state.getState());
        if (fact != null) lineageEmitter.record(fact);
        queryDispatcher.removeLineageInfo(queryId);
        events.fireQueryCompleted(new QueryCompletedEvent(queryId, null, state.getAuditPrincipal(), finalState,
                state.getCreatedAt().toEpochMilli(), fact == null ? System.currentTimeMillis() : fact.timestampMs(),
                QueryStatistics.wallTimeOnly(state.getDurationMs()), error, state.lineageMetadata()));
    }

    private static QueryAuditEmitter.AuditSubject subject(QueryStateMachine state) {
        return new QueryAuditEmitter.AuditSubject(state.getAuditPrincipal(), state.getTenantId());
    }

    static QueryAuditEmitter createAuditEmitter(AuditEventEmitter emitter,
            org.pangrova.observability.audit.QueryAuditService service) {
        return emitter == null ? null : new QueryAuditEmitter(emitter, service);
    }
}
