package org.pangrova.query.audit;

import org.pangrova.query.api.session.QuerySession;
import org.pangrova.query.api.audit.QueryAuditRecord;
import org.pangrova.query.api.audit.QueryAuditSink;
import org.pangrova.security.api.audit.AccessedObject;
import org.pangrova.security.api.audit.AuditEvent;
import org.pangrova.security.api.audit.AuditEventWriter;
import org.pangrova.security.api.enums.AuditEventType;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds and emits audit events for query lifecycle transitions.
 * The caller owns event delivery and terminal record persistence independently.
 */
public final class QueryAuditEmitter {

    /** Bounded attribution metadata, never a retained authentication token or session-property map. */
    public record AuditSubject(String principal, String tenantId) {
        public AuditSubject {
            java.util.Objects.requireNonNull(principal, "principal");
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(QueryAuditEmitter.class);

    private final AuditEventWriter events;
    private final QueryAuditSink auditService;

    /** Borrows the independently configured event and history outputs for this process. */
    public QueryAuditEmitter(AuditEventWriter events, QueryAuditSink auditService) {
        this.events = events;
        this.auditService = auditService;
        if (events == null && auditService == null) {
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

    /** Records submission with the principal from the submitting session. */
    public void emitQuerySubmitted(String queryId, String sql, QuerySession session) {
        if (events == null) {
            return;
        }
        events.write(buildEvent(AuditEventType.QUERY_SUBMITTED, queryId, sql,
                new AuditSubject(principalFrom(session), session != null ? session.tenantId() : null),
                true, null, 0));
    }

    /** Emits completion with the query-owned submitting identity and resolved tenant. */
    public void emitQueryCompleted(String queryId, long durationMs, AuditSubject subject) {
        emitQueryCompleted(queryId, durationMs, subject, null);
    }

    /** Emits completion and persists the submitted SQL with the durable record. */
    public void emitQueryCompleted(String queryId, long durationMs, AuditSubject subject, String sql) {
        emitTerminal(AuditEventType.QUERY_COMPLETED, queryId, durationMs, subject, sql, null);
    }

    /** Emits failure with the same attribution as the query, including anonymous rejection. */
    public void emitQueryFailed(String queryId, String errorMessage, long durationMs, AuditSubject subject) {
        emitQueryFailed(queryId, errorMessage, durationMs, subject, null);
    }

    /** Emits failure and persists the submitted SQL with the durable record. */
    public void emitQueryFailed(String queryId, String errorMessage, long durationMs,
                                AuditSubject subject, String sql) {
        emitTerminal(AuditEventType.QUERY_FAILED, queryId, durationMs, subject, sql, errorMessage);
    }

    /** Records the cancellation decision while physical task cleanup may still be in flight. */
    public void emitQueryCancelled(String queryId, long durationMs, AuditSubject subject, String sql) {
        emitTerminal(AuditEventType.QUERY_CANCELLED, queryId, durationMs, subject, sql, null);
    }

    private void emitTerminal(AuditEventType type, String queryId, long durationMs,
                              AuditSubject subject, String sql, String errorMessage) {
        java.util.Objects.requireNonNull(subject, "subject");
        boolean success = type == AuditEventType.QUERY_COMPLETED;
        if (events != null) {
            events.write(buildEvent(type, queryId, null, subject, success, errorMessage, durationMs));
        }
        persistAuditRecord(queryId, durationMs, success, subject, sql);
    }

    /** Emits table access after the caller has resolved the catalog, schema and identity. */
    public void emitTableAccessed(String queryId, String catalog, String schema,
                           String table, QuerySession session) {
        if (events == null) {
            return;
        }
        AuditEvent event = new AuditEvent(
                UUID.randomUUID().toString(), AuditEventType.TABLE_ACCESSED,
                Instant.now(), principalFrom(session), null, null, null,
                queryId, null, catalog, schema,
                List.of(new AccessedObject("TABLE", catalog, schema, table, List.of(), "SELECT")),
                true, null, 0, 0, 0, Map.of());
        events.write(event);
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
