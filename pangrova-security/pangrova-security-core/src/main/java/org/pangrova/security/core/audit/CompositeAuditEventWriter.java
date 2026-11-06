package org.pangrova.security.core.audit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import org.pangrova.security.api.audit.AuditEvent;
import org.pangrova.security.api.audit.AuditEventQuery;
import org.pangrova.security.spi.audit.AuditEventWriter;
import org.pangrova.persistence.security.api.audit.QueryableAuditEventWriter;

/**
 * An {@link AuditEventWriter} that fans out each audit event to multiple sinks.
 *
 * <p>Primary use case: writing to both SLF4J (immediate visibility, legacy pipelines)
 * and JDBC (durable, queryable compliance store). Sink failures are isolated: if one
 * sink throws, the remaining sinks still receive the event. Failures are logged but
 * never propagated to the caller (the {@link AuditEventEmitter} already provides
 * fault-isolation, and a composite writer must not short-circuit on a single sink
 * failure).</p>
 *
 * <p>If any delegate implements {@link QueryableAuditEventWriter}, the composite
 * delegates {@code query()} to the first queryable sink found (JDBC trumps log).</p>
 */
public final class CompositeAuditEventWriter implements QueryableAuditEventWriter {

    private static final Logger LOG = LoggerFactory.getLogger(CompositeAuditEventWriter.class);

    private final List<AuditEventWriter> delegates;

    /**
     * @param delegates the ordered list of writer sinks (at least one required)
     * @throws NullPointerException     if delegates is null or contains null
     * @throws IllegalArgumentException if delegates is empty
     */
    public CompositeAuditEventWriter(List<AuditEventWriter> delegates) {
        Objects.requireNonNull(delegates, "delegates must not be null");
        if (delegates.isEmpty()) {
            throw new IllegalArgumentException("at least one delegate writer is required");
        }
        for (AuditEventWriter w : delegates) {
            Objects.requireNonNull(w, "delegate writer must not be null");
        }
        this.delegates = List.copyOf(delegates);
    }

    @Override
    public void write(AuditEvent event) {
        Objects.requireNonNull(event, "event must not be null");
        RuntimeException first = null;
        int failCount = 0;
        for (AuditEventWriter delegate : delegates) {
            try {
                delegate.write(event);
            } catch (RuntimeException e) {
                failCount++;
                LOG.error("action=composite_audit_sink_failed sink={} eventId={} error={}",
                        delegate.getClass().getSimpleName(), event.eventId(), e.getMessage());
                if (first == null) {
                    first = e;
                }
            }
        }
        // If ALL sinks failed, propagate so the emitter can count the failure.
        // If at least one succeeded, swallow — the event is persisted somewhere.
        if (failCount == delegates.size() && first != null) {
            throw first;
        }
    }

    @Override
    public List<AuditEvent> query(AuditEventQuery query) {
        Objects.requireNonNull(query, "query must not be null");
        for (AuditEventWriter delegate : delegates) {
            if (delegate instanceof QueryableAuditEventWriter queryable) {
                return queryable.query(query);
            }
        }
        // No queryable delegate available — return empty rather than throw,
        // since the log-only config is still valid for write-only compliance.
        return List.of();
    }

    /**
     * Returns the delegate writers (for testing/inspection).
     */
    public List<AuditEventWriter> delegates() {
        return delegates;
    }

}
