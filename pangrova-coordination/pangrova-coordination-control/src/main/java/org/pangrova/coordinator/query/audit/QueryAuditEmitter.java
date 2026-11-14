package org.pangrova.coordinator.query.audit;

import org.pangrova.query.api.session.QuerySession;
import org.pangrova.query.api.audit.QueryAuditRecord;
import org.pangrova.query.api.audit.QueryAuditSink;
import org.pangrova.security.api.audit.AccessedObject;
import org.pangrova.security.api.audit.AuditEvent;
import org.pangrova.security.audit.AuditEventEmitter;
import org.pangrova.security.api.enums.AuditEventType;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds and emits audit events for query lifecycle transitions.
 * Isolates audit concern from QueryManager core logic.
 */
public final class QueryAuditEmitter {

    /** Bounded attribution metadata, never a retained authentication token or session-property map. */
    public record AuditSubject(String principal, String tenantId) {
        public AuditSubject {
            java.util.Objects.requireNonNull(principal, "principal");
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(QueryAuditEmitter.class);

    private final AuditEventEmitter emitter;
    private final QueryAuditSink auditService;

    public QueryAuditEmitter(AuditEventEmitter emitter, QueryAuditSink auditService) {
        this.emitter = emitter;
        this.auditService = auditService;
        if (emitter == null && auditService == null) {
            LOG.warn("event=audit_emitter_noop reason=no_emitter_or_service");
        }
    }

    /**
     * Creates a no-op audit emitter that silently drops all events.
     * Callers no longer need null-check — use this instead of passing null.
     */
    public static QueryAuditEmitter noOp() {
        return new QueryAuditEmitter(null, null);
    }

    public void emitQuerySubmitted(String queryId, String sql, QuerySession session) {
        if (emitter == null) {
            return;
        }
        emitter.emit(buildEvent(AuditEventType.QUERY_SUBMITTED, queryId, sql,
                new AuditSubject(principalFrom(session), session != null ? session.tenantId() : null),
                true, null, 0));
    }

    /** Emits completion with the query-owned submitting identity and resolved tenant. */
    public void emitQueryCompleted(String queryId, long durationMs, AuditSubject subject) {
        emitQueryCompleted(queryId, durationMs, subject, null);
    }

    /** Emits completion and persists the submitted SQL with the durable record. */
    public void emitQueryCompleted(String queryId, long durationMs, AuditSubject subject, String sql) {
        java.util.Objects.requireNonNull(subject, "subject");
        if (emitter != null) {
            emitter.emit(buildEvent(AuditEventType.QUERY_COMPLETED, queryId, null,
                    subject, true, null, durationMs));
        }
        persistAuditRecord(queryId, durationMs, true, subject, sql);
    }

    /** Emits failure with the same attribution as the query, including anonymous rejection. */
    public void emitQueryFailed(String queryId, String errorMessage, long durationMs, AuditSubject subject) {
        emitQueryFailed(queryId, errorMessage, durationMs, subject, null);
    }

    /** Emits failure and persists the submitted SQL with the durable record. */
    public void emitQueryFailed(String queryId, String errorMessage, long durationMs,
                                AuditSubject subject, String sql) {
        java.util.Objects.requireNonNull(subject, "subject");
        if (emitter != null) {
            emitter.emit(buildEvent(AuditEventType.QUERY_FAILED, queryId, null,
                    subject, false, errorMessage, durationMs));
        }
        persistAuditRecord(queryId, durationMs, false, subject, sql);
    }

    public void emitTableAccessed(String queryId, String catalog, String schema,
                           String table, QuerySession session) {
        if (emitter == null) {
            return;
        }
        AuditEvent event = new AuditEvent(
                UUID.randomUUID().toString(), AuditEventType.TABLE_ACCESSED,
                Instant.now(), principalFrom(session), null, null, null,
                queryId, null, catalog, schema,
                List.of(new AccessedObject("TABLE", catalog, schema, table, List.of(), "SELECT")),
                true, null, 0, 0, 0, Map.of());
        emitter.emit(event);
    }

    private void persistAuditRecord(String queryId, long durationMs, boolean success,
                                    AuditSubject subject, String sql) {
        if (auditService == null) {
            return;
        }
        QueryAuditRecord record = new QueryAuditRecord(
                queryId, subject.principal(), sql, subject.tenantId(), 0, 0,
                0, durationMs, 0, 0, 0, 0, 0, 0, 0, 0,
                Instant.now(), success);
        auditService.persist(record);
    }

    private static AuditEvent buildEvent(AuditEventType type, String queryId,
                                         String sql, AuditSubject subject,
                                         boolean success, String error, long durationMs) {
        return new AuditEvent(
                UUID.randomUUID().toString(), type, Instant.now(),
                subject.principal(), null, null, null, queryId, sql,
                null, null, List.of(), success, error, durationMs, 0, 0,
                subject.tenantId() != null ? Map.of("tenant_id", subject.tenantId()) : Map.of());
    }

    private static String principalFrom(QuerySession session) {
        if (session == null || session.userIdentity() == null) {
            return "anonymous";
        }
        return session.userIdentity().principal();
    }
}
