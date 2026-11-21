package org.pangrova.file.access;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.pangrova.file.api.enums.FileOperation;
import org.pangrova.file.api.space.FileSpace;
import org.pangrova.security.api.audit.AccessedObject;
import org.pangrova.security.api.audit.AuditEvent;
import org.pangrova.security.api.audit.AuditEventWriter;
import org.pangrova.security.api.enums.AuditEventType;

/** Records the authenticated issuance of tickets and URLs, without persisting any bearer material. */
public final class FileGrantAudit {
    private final AuditEventWriter writer;

    public FileGrantAudit(AuditEventWriter writer) {
        this.writer = Objects.requireNonNull(writer, "writer");
    }

    /** Records the issuance of one signed access ticket; the event never contains file contents. */
    void granted(String principal, String tenantId, FileSpace space, String path,
            FileOperation operation, long expiresAtEpochMillis, String workerEndpoint) {
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("space_id", space.id().toString());
        properties.put("tenant_id", tenantId);
        properties.put("path", path);
        properties.put("operation", operation.wireName());
        properties.put("expires_at_ms", Long.toString(expiresAtEpochMillis));
        properties.put("worker_endpoint", workerEndpoint);
        writer.write(new AuditEvent(
                UUID.randomUUID().toString(), AuditEventType.FILE_ACCESS_GRANTED, Instant.now(),
                principal,
                null, null, null, null, null,
                space.catalog(), space.schema(),
                List.of(new AccessedObject(space.type().name(), space.catalog(), space.schema(),
                        space.name(), List.of(), operation.name())),
                true, null, 0, 0, 0, Map.copyOf(properties)));
    }

    /**
     * Records one signed-URL issuance.
     *
     * <p>The URL itself is deliberately excluded: it is a bearer credential until it
     * expires, so only the mode, lifetime, path, and Worker endpoint are persisted.</p>
     */
    void urlIssued(String principal, String tenantId, FileSpace space, String path,
            String mode, Long expiresAtEpochMillis, String workerEndpoint) {
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("space_id", space.id().toString());
        properties.put("tenant_id", tenantId);
        properties.put("path", path);
        properties.put("mode", mode);
        if (expiresAtEpochMillis != null) properties.put("expires_at_ms", Long.toString(expiresAtEpochMillis));
        properties.put("worker_endpoint", workerEndpoint);
        writer.write(new AuditEvent(
                UUID.randomUUID().toString(), AuditEventType.FILE_URL_ISSUED, Instant.now(),
                principal,
                null, null, null, null, null,
                space.catalog(), space.schema(),
                List.of(new AccessedObject(space.type().name(), space.catalog(), space.schema(),
                        space.name(), List.of(), "URL_ISSUE")),
                true, null, 0, 0, 0, Map.copyOf(properties)));
    }
}
