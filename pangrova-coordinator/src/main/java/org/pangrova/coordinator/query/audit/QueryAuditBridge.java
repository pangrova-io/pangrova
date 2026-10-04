package org.pangrova.coordinator.query.audit;

import java.util.List;
import java.util.Optional;

import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.coordinator.query.QueryStateMachine;
import org.pangrova.observability.eventlistener.QueryEventDispatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.pangrova.governance.lineage.DatasetRef;
import org.pangrova.governance.lineage.LineageEventEmitter;
import org.pangrova.protocol.rest.error.ErrorDetail;
import org.pangrova.security.audit.AuditEventEmitter;
import org.pangrova.spi.execution.QueryState;
import org.pangrova.spi.execution.eventlistener.QueryCompletedEvent;
import org.pangrova.spi.execution.eventlistener.QueryCreatedEvent;
import org.pangrova.spi.execution.eventlistener.QueryStatistics;

/**
 * Bridges query lifecycle events to audit, lineage, and EventListener SPI subsystems.
 * Extracted from QueryManager to enforce single-responsibility.
 *
 * <p>All methods are fault-isolated: audit/lineage failures never
 * propagate to the query execution path.</p>
 */
final class QueryAuditBridge {

    private static final Logger LOG = LoggerFactory.getLogger(QueryAuditBridge.class);

    private final QueryAuditEmitter auditEmitter;
    private final LineageEventEmitter lineageEmitter;
    private final QueryLineageCapture lineageCapture;
    private final QueryEventDispatcher eventDispatcher;

    QueryAuditBridge(QueryAuditEmitter auditEmitter,
                     LineageEventEmitter lineageEmitter,
                     QueryLineageCapture lineageCapture) {
        this(auditEmitter, lineageEmitter, lineageCapture, null);
    }

    QueryAuditBridge(QueryAuditEmitter auditEmitter,
                     LineageEventEmitter lineageEmitter,
                     QueryLineageCapture lineageCapture,
                     QueryEventDispatcher eventDispatcher) {
        this.auditEmitter = auditEmitter;
        this.lineageEmitter = lineageEmitter;
        this.lineageCapture = lineageCapture;
        this.eventDispatcher = eventDispatcher;
    }

    void emitAuditSubmitted(String queryId, String sql, CoordinatorSessionContext session) {
        if (auditEmitter == null) {
            return;
        }
        auditEmitter.emitQuerySubmitted(queryId, sql, session);
        if (eventDispatcher != null) {
            try {
                String principal = principalFrom(session);
                eventDispatcher.fireQueryCreated(new QueryCreatedEvent(
                        queryId, sql, principal, System.currentTimeMillis()));
            } catch (RuntimeException e) {
                LOG.debug("event=event_listener_created_failed queryId={} error={}",
                        queryId, e.getMessage());
            }
        }
    }

    void emitAuditCompleted(String queryId, QueryStateMachine sm) {
        if (auditEmitter == null) {
            return;
        }
        auditEmitter.emitQueryCompleted(queryId, sm.getDurationMs(),
                new QueryAuditEmitter.AuditSubject(sm.getAuditPrincipal(), sm.getTenantId()));
        fireCompletedEvent(queryId, sm, QueryState.FINISHED, Optional.empty());
    }

    void emitAuditFailed(String queryId, QueryStateMachine sm, ErrorDetail error) {
        if (auditEmitter == null) {
            return;
        }
        String msg = error != null ? error.message() : "unknown";
        auditEmitter.emitQueryFailed(queryId, msg, sm.getDurationMs(),
                new QueryAuditEmitter.AuditSubject(sm.getAuditPrincipal(), sm.getTenantId()));
        fireCompletedEvent(queryId, sm, QueryState.FAILED, Optional.of(msg));
    }

    void emitTableAccessedAudit(String queryId, String catalog, String schema,
                                String table, CoordinatorSessionContext session) {
        if (auditEmitter == null) {
            return;
        }
        auditEmitter.emitTableAccessed(queryId, catalog, schema, table, session);
    }

    void emitLineageOnFinished(String queryId) {
        if (lineageEmitter == null) {
            LOG.debug("event=lineage_skip reason=governance_disabled queryId={}", queryId);
            return;
        }
        QueryLineageInfo info = lineageCapture.removeLineageInfo(queryId);
        if (info == null) {
            return;
        }
        List<DatasetRef> inputs = toDatasetRefs(info.sourceTables());
        List<DatasetRef> outputs = toDatasetRefs(info.targetTables());
        lineageEmitter.emitStartEvent(queryId, info.sql(), inputs, outputs);
        lineageEmitter.emitCompleteEvent(queryId);
    }

    static QueryAuditEmitter createAuditEmitter(
            AuditEventEmitter emitter,
            org.pangrova.observability.audit.QueryAuditService auditService) {
        if (emitter == null) {
            return null;
        }
        return new QueryAuditEmitter(emitter, auditService);
    }

    private void fireCompletedEvent(String queryId, QueryStateMachine sm,
                                     QueryState finalState, Optional<String> failureMessage) {
        if (eventDispatcher == null) {
            return;
        }
        try {
            long wallTimeMs = sm.getDurationMs();
            long createTime = sm.getCreatedAt().toEpochMilli();
            long endTime = System.currentTimeMillis();
            QueryStatistics stats = QueryStatistics.wallTimeOnly(wallTimeMs);
            eventDispatcher.fireQueryCompleted(new QueryCompletedEvent(
                    queryId, null, null, finalState,
                    createTime, endTime, stats, failureMessage));
        } catch (RuntimeException e) {
            LOG.debug("event=event_listener_completed_failed queryId={} error={}",
                    queryId, e.getMessage());
        }
    }

    private static String principalFrom(CoordinatorSessionContext session) {
        if (session == null || session.userIdentity() == null) {
            return "anonymous";
        }
        return session.userIdentity().principal();
    }

    private static List<DatasetRef> toDatasetRefs(List<String> tableNames) {
        return tableNames.stream()
                .map(name -> new DatasetRef("pangrova", name, List.of()))
                .toList();
    }
}
