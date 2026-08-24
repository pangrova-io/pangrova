package org.pangrova.security.tenant.model;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Immutable tenant definition including quota, namespace restrictions, and metadata.
 */
public record Tenant(
        String tenantId,
        String name,
        TenantStatus status,
        ResourceQuota quota,
        List<String> allowedCatalogs,
        List<String> allowedSchemas,
        String resourcePool,
        Map<String, String> properties,
        Instant createdAt,
        String createdBy
) {
    /**
     * Defensive copies of mutable collections to ensure immutability.
     */
    public Tenant {
        allowedCatalogs = allowedCatalogs != null ? List.copyOf(allowedCatalogs) : List.of();
        allowedSchemas = allowedSchemas != null ? List.copyOf(allowedSchemas) : List.of();
        properties = properties != null ? Map.copyOf(properties) : Map.of();
    }
}
