package org.pangrova.coordinator.query;

import org.pangrova.metadata.service.MetadataSessionContext;
import org.pangrova.spi.connector.ConnectorSession;

import java.util.Map;
import java.util.Optional;

/**
 * Converts coordinator-internal {@link CoordinatorSessionContext} to other session representations.
 *
 * <p>Provides conversion paths:
 * <ul>
 *   <li>{@link #toConnectorSession} — coordinator → {@link ConnectorSession} SPI</li>
 *   <li>{@link #toMetadataSessionContext} — coordinator → metadata {@link MetadataSessionContext}</li>
 *   <li>{@link #fromSpiSessionContext} — SPI {@link org.pangrova.spi.execution.SessionContext} → coordinator</li>
 * </ul>
 * </p>
 */
public final class SessionContextConverter {

    private SessionContextConverter() {}

    /**
     * Converts coordinator SessionContext to ConnectorSession SPI interface.
     * Extracts user principal from UserIdentity; catalog/schema from session properties.
     */
    public static ConnectorSession toConnectorSession(CoordinatorSessionContext ctx) {
        String user = extractUser(ctx);
        Optional<String> catalog = extractProperty(ctx, "catalog");
        Optional<String> schema = extractProperty(ctx, "schema");
        Map<String, String> props = ctx.sessionProperties();
        return new ConnectorSessionAdapter(user, catalog, schema, props);
    }

    /**
     * Converts coordinator SessionContext to metadata SessionContext.
     * Maps coordinator fields to metadata record fields, preserving queryId and tenantId.
     */
    public static MetadataSessionContext toMetadataSessionContext(CoordinatorSessionContext ctx) {
        String user = extractUser(ctx);
        String catalog = ctx.sessionProperties().getOrDefault("catalog", null);
        String schema = ctx.sessionProperties().getOrDefault("schema", null);
        String timezone = ctx.sessionProperties().getOrDefault("timezone", null);
        String locale = ctx.sessionProperties().getOrDefault("locale", null);
        Map<String, String> connectorProps = ctx.sessionProperties();
        return new MetadataSessionContext(
                user, catalog, schema, timezone, locale,
                connectorProps, ctx.tenantId(), null, ctx.queryId());
    }

    /**
     * Converts SPI SessionContext to coordinator SessionContext.
     * Preserves user/catalog/schema from SPI into session properties for downstream propagation.
     */
    public static CoordinatorSessionContext fromSpiSessionContext(
            String queryId, org.pangrova.spi.execution.SessionContext spiSession) {
        Map<String, String> props = new java.util.HashMap<>(spiSession.getProperties());
        spiSession.getCatalog().ifPresent(c -> props.putIfAbsent("catalog", c));
        spiSession.getSchema().ifPresent(s -> props.putIfAbsent("schema", s));
        props.putIfAbsent("user", spiSession.getUser());
        return new CoordinatorSessionContext(queryId, props, null, null);
    }

    private static String extractUser(CoordinatorSessionContext ctx) {
        if (ctx.userIdentity() != null) {
            return ctx.userIdentity().principal();
        }
        return ctx.sessionProperties().getOrDefault("user", "anonymous");
    }

    private static Optional<String> extractProperty(CoordinatorSessionContext ctx, String key) {
        String value = ctx.sessionProperties().get(key);
        return Optional.ofNullable(value);
    }

    /**
     * Immutable ConnectorSession adapter backed by coordinator SessionContext fields.
     */
    private record ConnectorSessionAdapter(
            String user,
            Optional<String> catalog,
            Optional<String> schema,
            Map<String, String> properties
    ) implements ConnectorSession {

        ConnectorSessionAdapter {
            properties = properties == null ? Map.of() : Map.copyOf(properties);
        }

        @Override
        public String getUser() {
            return user;
        }

        @Override
        public Optional<String> getCatalog() {
            return catalog;
        }

        @Override
        public Optional<String> getSchema() {
            return schema;
        }

        @Override
        public Map<String, String> getProperties() {
            return properties;
        }
    }
}
