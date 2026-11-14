package org.pangrova.coordinator.query.audit;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.pangrova.query.api.audit.QueryAuditAttribution;
import org.pangrova.security.api.audit.AccessedObject;
import org.pangrova.security.api.audit.AuditEvent;
import org.pangrova.security.api.audit.AuditEventWriter;
import org.pangrova.security.api.enums.AuditEventType;
import org.pangrova.file.api.space.FileSpace;

/**
 * Converts Volume and Stage namespace mutations into durable audit events.
 *
 * <p>Events name the object type and the qualified namespace, never storage
 * credentials or file contents.</p>
 */
public final class FileSpaceAuditEmitter {

    private final AuditEventWriter writer;

    public FileSpaceAuditEmitter(AuditEventWriter writer) {
        this.writer = java.util.Objects.requireNonNull(writer, "writer");
    }

    public void created(QueryAuditAttribution attribution, FileSpace space) {
        emit(switch (space.type()) {
            case VOLUME -> AuditEventType.VOLUME_CREATED;
            case STAGE -> AuditEventType.STAGE_CREATED;
        }, attribution, space, "CREATE", Map.of());
    }

    public void altered(QueryAuditAttribution attribution, FileSpace space, String action,
            Map<String, String> extra) {
        emit(switch (space.type()) {
            case VOLUME -> AuditEventType.VOLUME_ALTERED;
            case STAGE -> AuditEventType.STAGE_ALTERED;
        }, attribution, space, action, extra);
    }

    public void dropped(QueryAuditAttribution attribution, FileSpace space) {
        emit(switch (space.type()) {
            case VOLUME -> AuditEventType.VOLUME_DROPPED;
            case STAGE -> AuditEventType.STAGE_DROPPED;
        }, attribution, space, "DROP", Map.of());
    }

    private void emit(AuditEventType eventType, QueryAuditAttribution attribution, FileSpace space,
            String accessType, Map<String, String> extra) {
        Map<String, String> properties = properties(space);
        if (attribution.tenantId() != null) {
            properties.put("tenant_id", attribution.tenantId());
        }
        properties.putAll(extra);
        writer.write(new AuditEvent(
                UUID.randomUUID().toString(), eventType, Instant.now(),
                attribution.principal(), null, null, null, attribution.queryId(), null,
                space.catalog(), space.schema(),
                List.of(new AccessedObject(space.type().name(), space.catalog(), space.schema(),
                        space.name(), List.of(), accessType)),
                true, null, 0, 0, 0, Map.copyOf(properties)));
    }

    private static Map<String, String> properties(FileSpace space) {
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("space_id", space.id().toString());
        properties.put("space_type", space.type().name());
        properties.put("external", Boolean.toString(space.external()));
        properties.put("storage_profile", space.storageProfile());
        properties.put("location", space.location());
        properties.put("directory_enabled", Boolean.toString(space.directoryEnabled()));
        properties.put("owner", space.owner());
        return properties;
    }
}
