package org.pangrova.metadata.service;

import java.util.Map;

/**
 * Engine-level session representation carrying user identity, scope, and properties.
 * tenantId is the authoritative namespace identity propagated to connectors;
 * securityToken and queryId are engine-internal and must not propagate.
 */
public record MetadataSessionContext(
        String user,
        String currentCatalog,
        String currentSchema,
        String timezone,
        String locale,
        Map<String, String> connectorProperties,
        String tenantId,
        String securityToken,
        String queryId) {

    public MetadataSessionContext {
        connectorProperties = connectorProperties == null ? Map.of() : Map.copyOf(connectorProperties);
    }
}
