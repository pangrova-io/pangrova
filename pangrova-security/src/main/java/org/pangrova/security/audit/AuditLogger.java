package org.pangrova.security.audit;

/**
 * Facade for audit logging. Delegates to {@link AuditEventEmitter} for async write.
 */
public class AuditLogger {

    private final AuditEventEmitter emitter;

    public AuditLogger(AuditEventEmitter emitter) {
        this.emitter = emitter;
    }

    public void log(AuditEvent event) {
        emitter.emit(event);
    }
}
