package org.pangrova.metadata.service;

import org.pangrova.spi.connector.ConnectorSession;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Converts engine MetadataSessionContext to connector-facing ConnectorSession.
 * Propagates the authoritative tenant, but never the security token or query ID.
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
        return fromSessionContext(ctx, ctx.currentCatalog(), ctx.currentSchema());
    }

    /** Binds qualified metadata operations to their actual target scope. */
    public static ConnectorSession fromSessionContext(MetadataSessionContext ctx, String catalog, String schema) {
        Map<String, String> props = buildProperties(ctx);
        props.remove("catalog");
        props.remove("schema");
        if (catalog != null) props.put("catalog", catalog);
        if (schema != null) props.put("schema", schema);
        return new DefaultConnectorSession(ctx.user(), catalog, schema, props);
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
        props.remove("tenant_id");
        props.remove("pangrova.tenant_id");
        if (ctx.tenantId() != null && !ctx.tenantId().isBlank()) {
            props.put("tenant_id", ctx.tenantId());
        }
        if (ctx.timezone() != null) {
            props.put("timezone", ctx.timezone());
        }
        if (ctx.locale() != null) {
            props.put("locale", ctx.locale());
        }
        return props;
    }
}
