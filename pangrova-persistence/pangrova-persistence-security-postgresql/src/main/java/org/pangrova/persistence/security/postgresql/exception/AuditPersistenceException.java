package org.pangrova.persistence.security.postgresql.exception;

/** Unchecked wrapper so JDBC failures flow through the emitter's fault isolation. */
public final class AuditPersistenceException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public AuditPersistenceException(String message, Throwable cause) {
        super(message, cause);
    }
}
