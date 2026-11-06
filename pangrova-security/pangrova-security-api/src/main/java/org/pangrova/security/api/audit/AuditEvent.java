package org.pangrova.security.api.audit;

import org.pangrova.common.api.annotation.Experimental;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Immutable audit trail entry capturing a security-relevant operation.
 */
@Experimental
public record AuditEvent(
        String eventId,
        AuditEventType eventType,
        Instant timestamp,
        String principal,
        String clientIp,
        String traceId,
        String spanId,
        String queryId,
        String sql,
        String catalog,
        String schema,
        List<AccessedObject> accessedObjects,
        boolean success,
        String errorMessage,
        long durationMs,
        long rowsRead,
        long bytesRead,
        Map<String, String> properties
) {
    /**
     * Defensive copies of mutable collections to ensure immutability.
     */
    public AuditEvent {
        accessedObjects = accessedObjects != null ? List.copyOf(accessedObjects) : List.of();
        properties = properties != null ? Map.copyOf(properties) : Map.of();
    }
}
