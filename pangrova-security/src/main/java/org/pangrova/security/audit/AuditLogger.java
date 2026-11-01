package org.pangrova.security.audit;

import org.pangrova.spi.security.audit.AuditEvent;

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
