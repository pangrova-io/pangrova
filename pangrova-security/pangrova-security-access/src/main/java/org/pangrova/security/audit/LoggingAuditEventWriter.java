package org.pangrova.security.audit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Collectors;
import org.pangrova.security.api.audit.AuditEvent;
import org.pangrova.security.api.audit.AuditEventWriter;

/**
 * AuditEventWriter that persists audit events to structured log output.
 * Default backend when no external audit store (DB, Kafka) is configured.
 *
 * <p>Logs all security-relevant fields in structured key=value format for
 * compliance with SOX/GDPR audit requirements.</p>
 */
public final class LoggingAuditEventWriter implements AuditEventWriter {

    private static final Logger AUDIT_LOG = LoggerFactory.getLogger("pangrova.audit");

    @Override
    public void write(AuditEvent event) {
        AUDIT_LOG.info(
                "eventId={} eventType={} timestamp={} principal={} clientIp={} "
                        + "traceId={} spanId={} queryId={} catalog={} schema={} "
                        + "success={} durationMs={} rowsRead={} bytesRead={} "
                        + "accessedObjects=[{}] sql={} errorMessage={} properties={}",
                event.eventId(),
                event.eventType(),
                event.timestamp(),
                event.principal(),
                event.clientIp(),
                event.traceId(),
                event.spanId(),
                event.queryId(),
                event.catalog(),
                event.schema(),
                event.success(),
                event.durationMs(),
                event.rowsRead(),
                event.bytesRead(),
                formatAccessedObjects(event),
                truncateSql(event.sql()),
                event.errorMessage(),
                event.properties());
    }

    private static String formatAccessedObjects(AuditEvent event) {
        if (event.accessedObjects().isEmpty()) {
            return "";
        }
        return event.accessedObjects().stream()
                .map(obj -> obj.catalog() + "." + obj.schema() + "." + obj.table())
                .collect(Collectors.joining(","));
    }

    private static String truncateSql(String sql) {
        if (sql == null || sql.length() <= 1000) {
            return sql;
        }
        return sql.substring(0, 1000) + "...[truncated]";
    }
}
