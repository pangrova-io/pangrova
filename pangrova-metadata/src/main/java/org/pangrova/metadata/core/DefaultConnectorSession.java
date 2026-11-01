package org.pangrova.metadata.core;

import org.pangrova.metadata.api.session.MetadataSessionContext;

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
    private final Optional<org.pangrova.spi.connector.ConnectorQueryAuthority> queryAuthority;

    private DefaultConnectorSession(String user, String catalog, String schema, Map<String, String> properties,
            Optional<org.pangrova.spi.connector.ConnectorQueryAuthority> queryAuthority) {
        this.user = user;
        this.catalog = catalog;
        this.schema = schema;
        this.properties = Map.copyOf(properties);
        this.queryAuthority = queryAuthority;
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
        return new DefaultConnectorSession(ctx.user(), catalog, schema, props, ctx.queryAuthority());
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

    @Override
    public Optional<org.pangrova.spi.connector.ConnectorQueryAuthority> getQueryAuthority() {
        return queryAuthority;
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
