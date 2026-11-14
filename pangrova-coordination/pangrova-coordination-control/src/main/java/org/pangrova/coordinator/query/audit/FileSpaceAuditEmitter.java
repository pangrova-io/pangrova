package org.pangrova.coordinator.query.audit;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.pangrova.query.api.audit.QueryAuditAttribution;
import org.pangrova.file.api.audit.FileOperationAudit;
import org.pangrova.security.api.audit.AccessedObject;
import org.pangrova.security.api.audit.AuditEvent;
import org.pangrova.security.audit.AuditEventEmitter;
import org.pangrova.security.api.enums.AuditEventType;
import org.pangrova.file.api.space.FileSpace;
import org.pangrova.file.api.enums.FileSpaceType;

/**
 * Converts Volume and Stage namespace mutations into durable audit events.
 *
 * <p>Events name the object type and the qualified namespace, never storage
 * credentials or file contents.</p>
 */
public final class FileSpaceAuditEmitter implements FileOperationAudit {

    private static final String SYSTEM_PRINCIPAL = "system";

    private final AuditEventEmitter emitter;

    /** A null emitter keeps bootstrap paths and unit tests silent. */
    public FileSpaceAuditEmitter(AuditEventEmitter emitter) {
        this.emitter = emitter;
    }

    public void created(QueryAuditAttribution attribution, FileSpace space) {
        emit(eventType(space.type(), Mutation.CREATE), attribution, space, "CREATE", Map.of());
    }

    public void altered(QueryAuditAttribution attribution, FileSpace space, String action,
            Map<String, String> extra) {
        emit(eventType(space.type(), Mutation.ALTER), attribution, space, action, extra);
    }

    public void dropped(QueryAuditAttribution attribution, FileSpace space) {
        emit(eventType(space.type(), Mutation.DROP), attribution, space, "DROP", Map.of());
    }

    /** Records one authorized file operation; the event never contains file contents. */
    @Override
    public void fileOperation(String principal, String tenantId, FileSpace space, String path,
            String operation, String workerEndpoint) {
        if (emitter == null) {
            return;
        }
        Map<String, String> properties = properties(space);
        if (tenantId != null) {
            properties.put("tenant_id", tenantId);
        }
        properties.put("path", path);
        properties.put("operation", operation);
        properties.put("worker_endpoint", workerEndpoint);
        emitter.emit(new AuditEvent(
                UUID.randomUUID().toString(), fileEventType(operation), Instant.now(),
                principal == null || principal.isBlank() ? SYSTEM_PRINCIPAL : principal,
                null, null, null, null, null,
                space.catalog(), space.schema(),
                List.of(new AccessedObject(space.type().name(), space.catalog(), space.schema(),
                        space.name(), List.of(), operation.toUpperCase(java.util.Locale.ROOT))),
                true, null, 0, 0, 0, Map.copyOf(properties)));
    }

    /**
     * Records one signed-URL issuance.
     *
     * <p>The URL itself is deliberately excluded: it is a bearer credential until it
     * expires, so only the mode, lifetime, path, and Worker endpoint are persisted.</p>
     */
    @Override
    public void urlIssued(String principal, String tenantId, FileSpace space, String path,
            String mode, long ttlSeconds, String workerEndpoint) {
        if (emitter == null) {
            return;
        }
        Map<String, String> properties = properties(space);
        if (tenantId != null) {
            properties.put("tenant_id", tenantId);
        }
        properties.put("path", path);
        properties.put("mode", mode);
        properties.put("ttl_seconds", Long.toString(ttlSeconds));
        properties.put("worker_endpoint", workerEndpoint);
        emitter.emit(new AuditEvent(
                UUID.randomUUID().toString(), AuditEventType.FILE_URL_ISSUED, Instant.now(),
                principal == null || principal.isBlank() ? SYSTEM_PRINCIPAL : principal,
                null, null, null, null, null,
                space.catalog(), space.schema(),
                List.of(new AccessedObject(space.type().name(), space.catalog(), space.schema(),
                        space.name(), List.of(), "URL_ISSUE")),
                true, null, 0, 0, 0, Map.copyOf(properties)));
    }

    private void emit(AuditEventType eventType, QueryAuditAttribution attribution, FileSpace space,
            String accessType, Map<String, String> extra) {
        if (emitter == null) {
            return;
        }
        Map<String, String> properties = properties(space);
        if (attribution.tenantId() != null) {
            properties.put("tenant_id", attribution.tenantId());
        }
        properties.putAll(extra);
        emitter.emit(new AuditEvent(
                UUID.randomUUID().toString(), eventType, Instant.now(),
                attribution.principal(), null, null, null, attribution.queryId(), null,
                space.catalog(), space.schema(),
                List.of(new AccessedObject(space.type().name(), space.catalog(), space.schema(),
                        space.name(), List.of(), accessType)),
                true, null, 0, 0, 0, Map.copyOf(properties)));
    }

    private static Map<String, String> properties(FileSpace space) {
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("space_type", space.type().name());
        properties.put("external", Boolean.toString(space.external()));
        properties.put("storage_profile", space.storageProfile());
        properties.put("location", space.location());
        properties.put("directory_enabled", Boolean.toString(space.directoryEnabled()));
        properties.put("owner", space.owner());
        return properties;
    }

    private static AuditEventType fileEventType(String operation) {
        return switch (operation) {
            case "write" -> AuditEventType.FILE_UPLOADED;
            case "read" -> AuditEventType.FILE_DOWNLOADED;
            case "delete" -> AuditEventType.FILE_DELETED;
            default -> AuditEventType.FILE_LISTED;
        };
    }

    private static AuditEventType eventType(FileSpaceType type, Mutation mutation) {
        if (type == FileSpaceType.VOLUME) {
            return switch (mutation) {
                case CREATE -> AuditEventType.VOLUME_CREATED;
                case ALTER -> AuditEventType.VOLUME_ALTERED;
                case DROP -> AuditEventType.VOLUME_DROPPED;
            };
        }
        return switch (mutation) {
            case CREATE -> AuditEventType.STAGE_CREATED;
            case ALTER -> AuditEventType.STAGE_ALTERED;
            case DROP -> AuditEventType.STAGE_DROPPED;
        };
    }

    private enum Mutation {
        CREATE, ALTER, DROP
    }
}
