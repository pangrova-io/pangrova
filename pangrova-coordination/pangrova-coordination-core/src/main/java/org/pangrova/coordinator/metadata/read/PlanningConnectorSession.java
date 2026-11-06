package org.pangrova.coordinator.metadata.read;

import org.pangrova.connector.api.session.ConnectorSession;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** Immutable authenticated identity with the catalog/schema of the actual lookup. */
record PlanningConnectorSession(
        String user, Optional<String> catalog, Optional<String> schema,
        Map<String, String> properties,
        Optional<org.pangrova.connector.api.query.ConnectorQueryAuthority> queryAuthority) implements ConnectorSession {

    PlanningConnectorSession {
        if (user == null || user.isBlank()) {
            throw new IllegalArgumentException("Planning requires an authenticated user");
        }
        properties = Map.copyOf(properties);
        String tenant = properties.get("tenant_id");
        if (tenant == null || tenant.isBlank()) {
            throw new IllegalArgumentException("Planning requires an authenticated tenant_id");
        }
    }

    static PlanningConnectorSession bind(ConnectorSession source, String catalog, String schema) {
        var properties = new HashMap<>(source.getProperties());
        properties.remove("catalog");
        properties.remove("schema");
        properties.remove("pangrova.tenant_id");
        if (catalog != null) {
            properties.put("catalog", catalog);
        }
        if (schema != null) {
            properties.put("schema", schema);
        }
        return new PlanningConnectorSession(source.getUser(), Optional.ofNullable(catalog),
                Optional.ofNullable(schema), properties, source.getQueryAuthority());
    }

    @Override public String getUser() { return user; }
    @Override public Optional<String> getCatalog() { return catalog; }
    @Override public Optional<String> getSchema() { return schema; }
    @Override public Map<String, String> getProperties() { return properties; }
    @Override public Optional<org.pangrova.connector.api.query.ConnectorQueryAuthority> getQueryAuthority() {
        return queryAuthority;
    }
}
