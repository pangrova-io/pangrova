package org.pangrova.server.coordinator.assembly.security.audit;

import org.pangrova.security.core.audit.LoggingAuditEventWriter;

import java.util.Locale;
import org.pangrova.security.spi.audit.AuditEventWriter;
import org.pangrova.persistence.security.postgresql.audit.JdbcAuditEventWriter;

/**
 * Configuration selecting which {@link AuditEventWriter} backend to use.
 *
 * <p>The backend is chosen by a single string token so it can be driven from
 * deployment config (e.g. {@code audit.backend=jdbc}). {@link #LOG} is the safe
 * default (structured SLF4J, no external dependency); {@link #JDBC} provides a
 * durable + queryable store and requires a {@code DataSource} at wiring time.</p>
 */
public enum AuditWriterConfig {

    /** Log-only writer ({@link LoggingAuditEventWriter}). Default. */
    LOG,

    /** Durable, queryable JDBC writer ({@link JdbcAuditEventWriter}). */
    JDBC,

    /**
     * Composite writer that fans out to both SLF4J and JDBC sinks simultaneously.
     * Provides immediate log visibility (for ops/SIEM) plus durable persistence
     * (for compliance/query). Requires a {@code DataSource} at wiring time.
     */
    COMPOSITE;

    /**
     * Parses a config token into a backend, defaulting to {@link #LOG} for
     * null/blank values. Unknown nonblank tokens are configuration errors.
     */
    public static AuditWriterConfig fromToken(String token) {
        if (token == null || token.isBlank()) {
            return LOG;
        }
        return valueOf(token.trim().toUpperCase(Locale.ROOT));
    }
}
