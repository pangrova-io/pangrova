package org.pangrova.security.audit;

import java.time.Instant;

/**
 * Query criteria for reading persisted audit events back out of a
 * {@link QueryableAuditEventWriter}. All fields are optional (nullable); a null
 * field means "no constraint on that dimension".
 *
 * <p>This is the read-side contract that distinguishes a durable, queryable audit
 * store from the log-only {@link LoggingAuditEventWriter} (which is write-only and
 * cannot answer "what did principal X do between T1 and T2").</p>
 *
 * @param principal filter to events emitted for this principal, or null for any
 * @param eventType filter to events of this type, or null for any
 * @param from      inclusive lower bound on event timestamp, or null for unbounded
 * @param to        inclusive upper bound on event timestamp, or null for unbounded
 * @param limit     maximum number of rows to return (most recent first); must be &gt; 0
 */
public record AuditEventQuery(
        String principal,
        AuditEventType eventType,
        Instant from,
        Instant to,
        int limit
) {
    public static final int DEFAULT_LIMIT = 1000;

    public AuditEventQuery {
        if (limit <= 0) {
            limit = DEFAULT_LIMIT;
        }
    }

    /** Convenience: all events for a principal, default limit. */
    public static AuditEventQuery byPrincipal(String principal) {
        return new AuditEventQuery(principal, null, null, null, DEFAULT_LIMIT);
    }

    /** Convenience: all events of a type, default limit. */
    public static AuditEventQuery byType(AuditEventType type) {
        return new AuditEventQuery(null, type, null, null, DEFAULT_LIMIT);
    }
}
