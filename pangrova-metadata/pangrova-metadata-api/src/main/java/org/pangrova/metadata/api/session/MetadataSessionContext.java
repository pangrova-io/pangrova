package org.pangrova.metadata.api.session;

import java.util.Map;
import java.util.Optional;
import org.pangrova.connector.api.query.ConnectorQueryAuthority;

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
        String queryId,
        Optional<ConnectorQueryAuthority> queryAuthority) {

    public MetadataSessionContext {
        connectorProperties = connectorProperties == null ? Map.of() : Map.copyOf(connectorProperties);
        java.util.Objects.requireNonNull(queryAuthority, "queryAuthority");
        queryAuthority.ifPresent(authority -> {
            if (!authority.queryId().equals(queryId)) {
                throw new IllegalArgumentException("Metadata session differs from its issued query authority");
            }
        });
    }

    /** Creates a metadata-only actor before query admission, without issuing a lifetime. */
    public MetadataSessionContext(String user, String currentCatalog, String currentSchema,
            String timezone, String locale, Map<String, String> connectorProperties,
            String tenantId, String securityToken, String queryId) {
        this(user, currentCatalog, currentSchema, timezone, locale, connectorProperties,
                tenantId, securityToken, queryId, Optional.empty());
    }
}
