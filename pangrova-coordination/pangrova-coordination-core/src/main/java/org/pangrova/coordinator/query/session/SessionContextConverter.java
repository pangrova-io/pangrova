package org.pangrova.coordinator.query.session;

import org.pangrova.metadata.service.MetadataSessionContext;
import org.pangrova.connector.spi.ConnectorSession;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Converts coordinator-internal {@link CoordinatorSessionContext} to other session representations.
 *
 * <p>Provides conversion paths:
 * <ul>
 *   <li>{@link #toConnectorSession} — coordinator → {@link ConnectorSession} SPI</li>
 *   <li>{@link #toMetadataSessionContext} — coordinator → metadata {@link MetadataSessionContext}</li>
 *   <li>{@link #fromSpiSessionContext} — SPI {@link org.pangrova.common.session.SessionContext} → coordinator</li>
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
        return toConnectorSession(ctx, extractProperty(ctx, "catalog"),
                extractProperty(ctx, "schema"), false);
    }

    /**
     * Binds a connector operation to its resolved table rather than the session's
     * default catalog/schema. A qualified table may target a different catalog,
     * or the session may have no default catalog at all.
     */
    public static ConnectorSession toConnectorSession(
            CoordinatorSessionContext ctx, String catalog, String schema) {
        return toConnectorSession(ctx, Optional.of(catalog), Optional.of(schema), true);
    }

    private static ConnectorSession toConnectorSession(
            CoordinatorSessionContext ctx, Optional<String> catalog, Optional<String> schema,
            boolean bindResolvedTable) {
        String user = extractUser(ctx);
        Map<String, String> props = new HashMap<>(ctx.sessionProperties());
        props.put("user", user);
        props.keySet().removeIf(key -> key.startsWith(
                org.pangrova.coordinator.metadata.read.PlanningSnapshotSelectors.PROPERTY_PREFIX)
                || key.startsWith(
                        org.pangrova.coordinator.metadata.read.PlanningIncrementalSelectors.PROPERTY_PREFIX));
        props.put("queryId", ctx.queryId());
        if (bindResolvedTable) {
            props.put("catalog", catalog.orElseThrow());
            props.put("schema", schema.orElseThrow());
        }
        props.remove("tenant_id");
        props.remove("pangrova.tenant_id");
        props.remove(org.pangrova.planner.mv.DynamicTableRewriter.INTERNAL_EXECUTION_PROPERTY);
        if (ctx.tenantId() != null && !ctx.tenantId().isBlank()) {
            props.put("tenant_id", ctx.tenantId());
        }
        if (ctx.internalMaterializationExecution()) {
            props.put(org.pangrova.planner.mv.DynamicTableRewriter.INTERNAL_EXECUTION_PROPERTY, "true");
        }
        ConnectorSession session = new ConnectorSessionAdapter(user, catalog, schema, props,
                ctx.queryAuthority());
        ConnectorSession pinned = ctx.sourceSnapshots().isEmpty()
                ? session
                : org.pangrova.coordinator.metadata.read.PlanningSnapshotSelectors.bind(
                        session, ctx.sourceSnapshots());
        return ctx.sourceIncrementalRanges().isEmpty()
                ? pinned
                : org.pangrova.coordinator.metadata.read.PlanningIncrementalSelectors.bind(
                        pinned, ctx.sourceIncrementalRanges());
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
                connectorProps, ctx.tenantId(), null, ctx.queryId(), ctx.queryAuthority());
    }

    /**
     * Converts SPI SessionContext to coordinator SessionContext.
     * Preserves user/catalog/schema from SPI into session properties for downstream propagation.
     */
    public static CoordinatorSessionContext fromSpiSessionContext(
            String queryId, org.pangrova.common.session.SessionContext spiSession) {
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
            Map<String, String> properties,
            Optional<org.pangrova.connector.spi.ConnectorQueryAuthority> queryAuthority
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

        @Override
        public Optional<org.pangrova.connector.spi.ConnectorQueryAuthority> getQueryAuthority() {
            return queryAuthority;
        }
    }
}
