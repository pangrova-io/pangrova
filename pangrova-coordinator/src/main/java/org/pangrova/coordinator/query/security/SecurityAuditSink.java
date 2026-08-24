package org.pangrova.coordinator.query.security;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.pangrova.security.audit.AccessedObject;
import org.pangrova.security.audit.AuditEvent;
import org.pangrova.security.audit.AuditEventEmitter;
import org.pangrova.security.audit.AuditEventType;

/**
 * Thin adapter that builds and emits security/authorization audit events through the
 * shared {@link AuditEventEmitter} plumbing.
 *
 * <p>Closes F-CHAIN-SEC-05 / G-SEC-05: previously authorization denials and data-write
 * (DML) operations produced no audit record at all. This sink is reused by both
 * authorization chokepoints — {@link AccessControlEnforcer} (RelNode + statement level)
 * and {@code PermissionChecker} (DDL/DML statement dispatch) — so every
 * {@link AccessDeniedException} site emits {@link AuditEventType#ACCESS_DENIED} with the
 * principal and the resource, and every DML write/delete emits
 * {@link AuditEventType#TABLE_WRITE} / {@link AuditEventType#TABLE_DELETE}.</p>
 *
 * <p>A null emitter yields a no-op sink ({@link #NOOP} / {@link #of(AuditEventEmitter)}),
 * so security enforcement never depends on auditing being configured.</p>
 */
public final class SecurityAuditSink {

    /** Sink that drops everything; used when no emitter is wired. */
    public static final SecurityAuditSink NOOP = new SecurityAuditSink(null);

    private final AuditEventEmitter emitter;

    public SecurityAuditSink(AuditEventEmitter emitter) {
        this.emitter = emitter;
    }

    /** Returns a sink for the emitter, or {@link #NOOP} when {@code emitter} is null. */
    public static SecurityAuditSink of(AuditEventEmitter emitter) {
        return emitter == null ? NOOP : new SecurityAuditSink(emitter);
    }

    /** Records an authorization denial with principal + resource + attempted action. */
    public void accessDenied(String principal, String catalog, String schema, String table,
                             String action, String reason) {
        if (emitter == null) {
            return;
        }
        emitter.emit(new AuditEvent(
                UUID.randomUUID().toString(), AuditEventType.ACCESS_DENIED, Instant.now(),
                principal != null ? principal : "anonymous", null, null, null, null, null,
                catalog, schema, objects(catalog, schema, table, action),
                false, reason, 0, 0, 0, Map.of()));
    }

    /** Records a successful data-write (INSERT/UPDATE/MERGE) authorization. */
    public void tableWrite(String principal, String catalog, String schema, String table) {
        emitDataEvent(AuditEventType.TABLE_WRITE, principal, catalog, schema, table, "WRITE");
    }

    /** Records a successful data-delete (DELETE/UPDATE/MERGE) authorization. */
    public void tableDelete(String principal, String catalog, String schema, String table) {
        emitDataEvent(AuditEventType.TABLE_DELETE, principal, catalog, schema, table, "DELETE");
    }

    private void emitDataEvent(AuditEventType type, String principal, String catalog,
                               String schema, String table, String action) {
        if (emitter == null) {
            return;
        }
        emitter.emit(new AuditEvent(
                UUID.randomUUID().toString(), type, Instant.now(),
                principal != null ? principal : "anonymous", null, null, null, null, null,
                catalog, schema, objects(catalog, schema, table, action),
                true, null, 0, 0, 0, Map.of()));
    }

    private static List<AccessedObject> objects(String catalog, String schema, String table,
                                                String action) {
        if (table == null) {
            return List.of();
        }
        return List.of(new AccessedObject("TABLE", catalog, schema, table, List.of(), action));
    }
}
