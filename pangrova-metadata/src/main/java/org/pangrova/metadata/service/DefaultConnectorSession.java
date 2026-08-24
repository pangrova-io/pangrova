package org.pangrova.metadata.service;

import org.pangrova.spi.connector.ConnectorSession;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Converts engine MetadataSessionContext to connector-facing ConnectorSession.
 * Deliberately excludes tenantId, securityToken, queryId from propagation.
 */
public final class DefaultConnectorSession implements ConnectorSession {

    private final String user;
    private final String catalog;
    private final String schema;
    private final Map<String, String> properties;

    private DefaultConnectorSession(String user, String catalog, String schema, Map<String, String> properties) {
        this.user = user;
        this.catalog = catalog;
        this.schema = schema;
        this.properties = Map.copyOf(properties);
    }

    public static ConnectorSession fromSessionContext(MetadataSessionContext ctx) {
        Map<String, String> props = buildProperties(ctx);
        return new DefaultConnectorSession(ctx.user(), ctx.currentCatalog(), ctx.currentSchema(), props);
    }

    /**
     * Creates a minimal ConnectorSession for query lifecycle operations
     * (beginQuery / cleanupQuery) that do not require full user context.
     *
     * @param queryId the query identifier (used as the user identity placeholder)
     * @return a lightweight session suitable for lifecycle callbacks
     */
    public static ConnectorSession forQueryLifecycle(String queryId) {
        return new DefaultConnectorSession("system", null, null, Map.of("queryId", queryId));
    }

    @Override
    public String getUser() {
        return user;
    }

    @Override
    public Optional<String> getCatalog() {
        return Optional.ofNullable(catalog);
    }

    @Override
    public Optional<String> getSchema() {
        return Optional.ofNullable(schema);
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    private static Map<String, String> buildProperties(MetadataSessionContext ctx) {
        Map<String, String> props = new HashMap<>(ctx.connectorProperties());
        if (ctx.timezone() != null) {
            props.put("timezone", ctx.timezone());
        }
        if (ctx.locale() != null) {
            props.put("locale", ctx.locale());
        }
        return props;
    }
}
