package org.pangrova.metadata.service;

import java.util.Map;

/**
 * Engine-level session representation carrying user identity, scope, and properties.
 * tenantId, securityToken, queryId are engine-internal and must NOT propagate to connectors.
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
