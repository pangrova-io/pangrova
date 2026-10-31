package org.pangrova.worker.nativebridge.connector.session;

import org.pangrova.protocol.v2.SecurityContext;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.connector.spi.ConnectorSession;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/** Immutable task identity/session passed to Worker-local connector plugins. */
public record TaskConnectorSession(
        String tenantId,
        String principal,
        java.util.Set<String> activeRoles,
        TaskAttemptId attempt,
        String catalog,
        Map<String, String> properties) implements ConnectorSession {
    @Override
    public java.util.Optional<org.pangrova.connector.spi.ConnectorQueryAuthority> getQueryAuthority() {
        return java.util.Optional.empty();
    }


    public TaskConnectorSession {
        if (tenantId == null || tenantId.isBlank() || principal == null || principal.isBlank()) {
            throw new IllegalArgumentException("Task connector session requires tenant and principal");
        }
        activeRoles = java.util.Set.copyOf(activeRoles);
        attempt = java.util.Objects.requireNonNull(attempt, "attempt");
        if (attempt.getTaskId().isBlank()) {
            throw new IllegalArgumentException("Task connector session requires canonical task attempt");
        }
        if (catalog == null || catalog.isBlank()) {
            throw new IllegalArgumentException("catalog must not be blank");
        }
        Map<String, String> scoped = new LinkedHashMap<>(properties);
        scoped.remove("pangrova.tenant_id");
        scoped.put("tenant_id", tenantId);
        properties = Map.copyOf(scoped);
    }

    /** Creates a connector session fenced to the canonical task identity. */
    public static TaskConnectorSession from(
            SecurityContext security,
            TaskAttemptId attempt,
            String catalog,
            Map<String, String> taskProperties) {
        Map<String, String> scoped = new LinkedHashMap<>(taskProperties);
        scoped.put("pangrova.task_id", attempt.getTaskId());
        scoped.put("pangrova.query_or_job_id", attempt.getQueryOrJobId());
        return new TaskConnectorSession(
                security.getTenantId(), security.getPrincipal(),
                java.util.Set.copyOf(security.getActiveRolesList()), attempt,
                catalog, scoped);
    }

    @Override
    public String getUser() { return principal; }

    @Override
    public Optional<String> getCatalog() { return Optional.of(catalog); }

    @Override
    public Optional<String> getSchema() { return Optional.empty(); }

    @Override
    public Map<String, String> getProperties() { return properties; }
}
