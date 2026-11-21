package org.pangrova.coordinator.query;

import org.pangrova.governance.api.enums.LineageEventType;
import org.pangrova.governance.api.enums.ColumnDerivationKind;
import org.pangrova.query.lifecycle.QueryStateMachine;

import java.util.List;
import java.util.Optional;

import org.pangrova.query.audit.QueryAuditEmitter;
import org.pangrova.coordinator.query.audit.QueryLineageInfo;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.governance.runtime.lineage.LineageEventEmitter;
import org.pangrova.query.api.lineage.QueryLineageFact;
import org.pangrova.query.event.QueryEventDispatcher;
import org.pangrova.query.api.error.QueryFailure;
import org.pangrova.security.audit.AuditEventEmitter;
import org.pangrova.query.api.enums.QueryState;
import org.pangrova.query.api.event.QueryCompletedEvent;
import org.pangrova.query.api.event.QueryCreatedEvent;
import org.pangrova.query.api.lineage.QueryLineageMetadata;
import org.pangrova.query.api.event.QueryStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Logical query admission and terminal telemetry; task completion never publishes query lineage. */
final class QueryAuditDispatcher implements org.pangrova.query.lifecycle.QueryAuditPort {
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

    @Override
    public void emitAuditSubmitted(String queryId, String sql, QuerySession session, QueryStateMachine state) {
        String principal = session.userIdentity() == null ? "anonymous" : session.userIdentity().principal();
        if (lineageEnabled) {
            QueryLineageFact fact = state.freezeLineage(LineageEventType.START,
                    () -> new QueryLineageFact(lineageEmitter.namespace(), queryId, LineageEventType.START,
                            state.getCreatedAt().toEpochMilli(), sql, null, QueryLineageMetadata.empty()));
            lineageEmitter.record(fact);
        }
        events.fireQueryCreated(new QueryCreatedEvent(queryId, sql, principal, state.getCreatedAt().toEpochMilli()));
        if (auditEmitter != null) auditEmitter.emitQuerySubmitted(queryId, sql, session);
    }

    @Override
    public void emitAuditCompleted(String queryId, QueryStateMachine state) {
        emitTerminal(queryId, state, QueryState.FINISHED, Optional.empty());
        if (auditEmitter != null) {
            auditEmitter.emitQueryCompleted(queryId, state.getDurationMs(), subject(state), state.getSql());
        }
    }

    @Override
    public void emitAuditFailed(String queryId, QueryStateMachine state, QueryFailure error) {
        String message = error == null ? "unknown" : error.message();
        emitTerminal(queryId, state, QueryState.FAILED, Optional.of(message));
        if (auditEmitter != null) {
            auditEmitter.emitQueryFailed(queryId, message, state.getDurationMs(), subject(state), state.getSql());
        }
    }

    @Override
    public void emitAuditCancelled(String queryId, QueryStateMachine state) {
        emitTerminal(queryId, state, QueryState.CANCELLED, Optional.empty());
        if (auditEmitter != null) {
            auditEmitter.emitQueryCancelled(queryId, state.getDurationMs(), subject(state), state.getSql());
        }
    }

    @Override
    public void emitTableAccessedAudit(String queryId, String catalog, String schema,
            String table, QuerySession session) {
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
        var columns = info.columnDerivations().stream().map(derivation -> {
            var kind = switch (derivation.expression()) {
                case DIRECT -> ColumnDerivationKind.DIRECT;
                case TRANSFORM -> ColumnDerivationKind.TRANSFORM;
                case AGGREGATE -> ColumnDerivationKind.AGGREGATE;
            };
            return new QueryLineageMetadata.Column(
                    derivation.outputColumn(), derivation.sourceColumns().stream().map(source ->
                            new QueryLineageMetadata.SourceColumn(
                                    "pangrova", source.table(), source.column())).toList(), kind);
        })
                .toList();
        var outputs = info.targetTables().stream().map(name -> new QueryLineageMetadata.Dataset(
                "pangrova", name, columns)).toList();
        return new QueryLineageMetadata(inputs, outputs);
    }

    @Override
    public QueryLineageFact terminalFact(String queryId, QueryStateMachine state, QueryState target) {
        if (!lineageEnabled) return null;
        LineageEventType phase = switch (target) {
            case FINISHED -> LineageEventType.COMPLETE;
            case FAILED -> LineageEventType.FAIL;
            case CANCELLED -> LineageEventType.ABORT;
            default -> throw new IllegalArgumentException("Lineage requires a terminal query state");
        };
        return state.freezeLineage(phase, () -> {
            String failureMessage = phase == LineageEventType.FAIL
                    ? state.getError().map(QueryFailure::message).orElse("unknown") : null;
            return new QueryLineageFact(lineageEmitter.namespace(), queryId, phase,
                    System.currentTimeMillis(), null, failureMessage, state.lineageMetadata());
        });
    }

    /** Unadmitted failures also require a committed fact before logical cleanup can be released. */
    @Override
    public void persistTerminalLineage(String queryId, QueryStateMachine state) {
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

    public static QueryAuditEmitter createAuditEmitter(AuditEventEmitter emitter,
            org.pangrova.query.api.audit.QueryAuditSink service) {
        if (emitter == null && service == null) return null;
        return new QueryAuditEmitter(emitter == null ? null : emitter::emit, service);
    }
}
