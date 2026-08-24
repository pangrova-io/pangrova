package org.pangrova.coordinator.query;

import java.util.List;

import org.pangrova.coordinator.query.audit.QueryAuditEmitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.pangrova.coordinator.query.audit.QueryLineageInfo;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.governance.lineage.DatasetRef;
import org.pangrova.governance.lineage.LineageEventEmitter;
import org.pangrova.protocol.rest.error.ErrorDetail;
import org.pangrova.security.audit.AuditEventEmitter;

/**
 * Bridges query lifecycle events to audit and lineage subsystems.
 * Extracted from QueryManager to enforce single-responsibility.
 *
 * <p>All methods are fault-isolated: audit/lineage failures never
 * propagate to the query execution path.</p>
 */
final class QueryAuditDispatcher {

    private static final Logger LOG = LoggerFactory.getLogger(QueryAuditDispatcher.class);

    private final QueryAuditEmitter auditEmitter;
    private final LineageEventEmitter lineageEmitter;
    private final QueryDispatcher queryDispatcher;

    QueryAuditDispatcher(QueryAuditEmitter auditEmitter,
                     LineageEventEmitter lineageEmitter,
                     QueryDispatcher queryDispatcher) {
        this.auditEmitter = auditEmitter;
        this.lineageEmitter = lineageEmitter;
        this.queryDispatcher = queryDispatcher;
    }

    void emitAuditSubmitted(String queryId, String sql, CoordinatorSessionContext session) {
        if (auditEmitter == null) {
            return;
        }
        auditEmitter.emitQuerySubmitted(queryId, sql, session);
    }

    void emitAuditCompleted(String queryId, QueryStateMachine sm) {
        if (auditEmitter == null) {
            return;
        }
        auditEmitter.emitQueryCompleted(queryId, sm.getDurationMs());
    }

    void emitAuditFailed(String queryId, QueryStateMachine sm, ErrorDetail error) {
        if (auditEmitter == null) {
            return;
        }
        String msg = error != null ? error.message() : "unknown";
        auditEmitter.emitQueryFailed(queryId, msg, sm.getDurationMs());
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
        QueryLineageInfo info = queryDispatcher.removeLineageInfo(queryId);
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

    private static List<DatasetRef> toDatasetRefs(List<String> tableNames) {
        return tableNames.stream()
                .map(name -> new DatasetRef("pangrova", name, List.of()))
                .toList();
    }
}
