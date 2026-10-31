package org.pangrova.security.api.audit;

import java.util.List;

/**
 * An {@link AuditEventWriter} whose persisted events can be read back.
 *
 * <p>The plain {@link AuditEventWriter} contract only guarantees that an event is
 * accepted for writing; a log-only writer satisfies it by emitting
 * a log line, which is neither durably structured nor queryable. Compliance and
 * incident-response workflows need to answer questions such as "show every
 * {@code ACCESS_DENIED} for principal {@code alice} in the last 24h" — that is the
 * contract added here.</p>
 *
 * <p>Implementations MUST persist events durably
 * so they survive process restart and MUST honour {@link AuditEventQuery} filters.</p>
 */
public interface QueryableAuditEventWriter extends AuditEventWriter {

    /**
     * Reads persisted audit events matching the given criteria, most recent first.
     *
     * @param query the filter criteria (never null)
     * @return matching events, newest first, bounded by {@link AuditEventQuery#limit()}
     */
    List<AuditEvent> query(AuditEventQuery query);
}
