package org.pangrova.query.audit;

import org.pangrova.query.api.session.QuerySession;
import org.pangrova.query.api.audit.QueryAuditRecord;
import org.pangrova.query.api.audit.QueryAuditSink;
import org.pangrova.query.api.enums.QueryState;
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
    private record AuditSubject(String principal, String tenantId) {
        private AuditSubject {
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

    /** Publishes the query owner's frozen terminal audit record. */
    public void emitQueryTerminal(QueryState state, QueryAuditRecord record, String errorMessage) {
        AuditEventType type = switch (state) {
            case FINISHED -> AuditEventType.QUERY_COMPLETED;
            case FAILED -> AuditEventType.QUERY_FAILED;
            case CANCELLED -> AuditEventType.QUERY_CANCELLED;
            default -> throw new IllegalArgumentException("Query audit requires a terminal state");
        };
        java.util.Objects.requireNonNull(record, "record");
        if (record.success() != (state == QueryState.FINISHED)) {
            throw new IllegalArgumentException("Audit record outcome does not match the query state");
        }
        if (events != null) {
            var subject = new AuditSubject(record.userId(), record.tenantId());
            events.write(buildEvent(type, record.queryId(), null, subject, record.success(), errorMessage,
                    record.wallTimeMs()));
        }
        if (auditService != null) {
            auditService.persist(record);
        }
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
