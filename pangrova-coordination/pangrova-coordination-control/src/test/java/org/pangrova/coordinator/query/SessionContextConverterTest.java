package org.pangrova.coordinator.query;

import org.pangrova.query.api.session.QuerySession;
import org.pangrova.coordinator.query.session.SessionContextConverter;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.security.api.identity.UserIdentity;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies conversion paths between the three SessionContext definitions:
 * <ul>
 *   <li>SPI SessionContext (interface) → Coordinator SessionContext (record)</li>
 *   <li>Coordinator SessionContext → ConnectorSession SPI</li>
 *   <li>Coordinator SessionContext → Metadata SessionContext (record)</li>
 * </ul>
 *
 * Validates: Requirements 3.4, 5.2
 */
class SessionContextConverterTest {

    @Test
    void connectorQueryIdentityAndSnapshotSelectorsCannotBeSpoofedByClientProperties() {
        var context = new QuerySession("admitted-query",
                Map.of("queryId", "forged", "pangrova.source_snapshot.cat.db.table", "42"),
                null, "tenant", Optional.empty(),
                new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture());
        var session = SessionContextConverter.toConnectorSession(context);
        assertThat(session.getProperties()).containsEntry("queryId", "admitted-query")
                .doesNotContainKey("pangrova.source_snapshot.cat.db.table");
    }

    // ── SPI → Coordinator ──

    @Test
    void fromSpiSessionContext_preservesUserCatalogSchema() {
        org.pangrova.common.session.SessionContext spi = buildSpiSession(
                "alice", Optional.of("iceberg_catalog"), Optional.of("prod"),
                Map.of("timezone", "UTC"));

        QuerySession result = SessionContextConverter.fromSpiSessionContext("q-1", spi, new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory());

        assertThat(result.queryId()).isEqualTo("q-1");
        assertThat(result.sessionProperties()).containsEntry("user", "alice");
        assertThat(result.sessionProperties()).containsEntry("catalog", "iceberg_catalog");
        assertThat(result.sessionProperties()).containsEntry("schema", "prod");
        assertThat(result.sessionProperties()).containsEntry("timezone", "UTC");
    }

    @Test
    void fromSpiSessionContext_handlesEmptyCatalogAndSchema() {
        org.pangrova.common.session.SessionContext spi = buildSpiSession(
                "bob", Optional.empty(), Optional.empty(), Map.of());

        QuerySession result = SessionContextConverter.fromSpiSessionContext("q-2", spi, new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory());

        assertThat(result.sessionProperties()).containsEntry("user", "bob");
        assertThat(result.sessionProperties()).doesNotContainKey("catalog");
        assertThat(result.sessionProperties()).doesNotContainKey("schema");
    }

    @Test
    void fromSpiSessionContext_doesNotOverrideExistingProperties() {
        org.pangrova.common.session.SessionContext spi = buildSpiSession(
                "charlie", Optional.of("cat1"), Optional.empty(),
                Map.of("catalog", "existing_catalog", "user", "existing_user"));

        QuerySession result = SessionContextConverter.fromSpiSessionContext("q-3", spi, new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory());

        // putIfAbsent should not override existing values
        assertThat(result.sessionProperties()).containsEntry("catalog", "existing_catalog");
        assertThat(result.sessionProperties()).containsEntry("user", "existing_user");
    }

    // ── Coordinator → ConnectorSession ──

    @Test
    void toConnectorSession_extractsUserFromIdentity() {
        UserIdentity identity = new UserIdentity("alice", Set.of(), Map.of(), Instant.now());
        QuerySession ctx = new QuerySession("q-1", Map.of("catalog", "cat1"), identity, null, java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture());

        ConnectorSession result = SessionContextConverter.toConnectorSession(ctx);

        assertThat(result.getUser()).isEqualTo("alice");
        assertThat(result.getCatalog()).contains("cat1");
    }

    @Test
    void toConnectorSession_fallsBackToAnonymousWhenNoIdentity() {
        QuerySession ctx = new QuerySession("q-2", Map.of(), null, null, java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture());

        ConnectorSession result = SessionContextConverter.toConnectorSession(ctx);

        assertThat(result.getUser()).isEqualTo("anonymous");
    }

    @Test
    void toConnectorSession_extractsUserFromPropertiesWhenNoIdentity() {
        QuerySession ctx = new QuerySession("q-3", Map.of("user", "bob"), null, null, java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture());

        ConnectorSession result = SessionContextConverter.toConnectorSession(ctx);

        assertThat(result.getUser()).isEqualTo("bob");
    }

    @Test
    void toConnectorSession_preservesCatalogAndSchema() {
        QuerySession ctx = new QuerySession(
                "q-4", Map.of("catalog", "iceberg", "schema", "analytics"), null, null, java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture());

        ConnectorSession result = SessionContextConverter.toConnectorSession(ctx);

        assertThat(result.getCatalog()).contains("iceberg");
        assertThat(result.getSchema()).contains("analytics");
    }

    @Test
    void toConnectorSession_bindsQualifiedTableWithoutTrustingDefaultCatalogOrTenantProperty() {
        UserIdentity identity = new UserIdentity("writer", Set.of(), Map.of(), Instant.now());
        QuerySession ctx = new QuerySession(
                "q-qualified",
                Map.of("catalog", "default_catalog", "schema", "default_schema",
                        "tenant_id", "spoofed", "timezone", "UTC"),
                identity, "resolved-tenant", java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture());

        ConnectorSession result = SessionContextConverter.toConnectorSession(
                ctx, "target_catalog", "target_schema");

        assertThat(result.getUser()).isEqualTo("writer");
        assertThat(result.getCatalog()).contains("target_catalog");
        assertThat(result.getSchema()).contains("target_schema");
        assertThat(result.getProperties()).containsEntry("tenant_id", "resolved-tenant")
                .containsEntry("catalog", "target_catalog")
                .containsEntry("schema", "target_schema")
                .containsEntry("timezone", "UTC");
    }

    @Test
    void toConnectorSession_usesResolvedTenantInsteadOfClientProperty() {
        QuerySession ctx = new QuerySession(
                "q-tenant", Map.of("tenant_id", "spoofed", "pangrova.tenant_id", "also-spoofed"),
                null, "tenant-1", java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture());

        ConnectorSession result = SessionContextConverter.toConnectorSession(ctx);

        assertThat(result.getProperties()).containsEntry("tenant_id", "tenant-1")
                .doesNotContainKey("pangrova.tenant_id");
    }

    @Test
    void toConnectorSession_dropsClientTenantWhenUnresolved() {
        QuerySession ctx = new QuerySession(
                "q-unresolved", Map.of("tenant_id", "spoofed"), null, null, java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture());

        ConnectorSession result = SessionContextConverter.toConnectorSession(ctx);

        assertThat(result.getProperties()).doesNotContainKey("tenant_id");
    }

    @Test
    void toConnectorSession_returnsEmptyOptionalWhenNoCatalogOrSchema() {
        QuerySession ctx = new QuerySession("q-5", Map.of(), null, null, java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture());

        ConnectorSession result = SessionContextConverter.toConnectorSession(ctx);

        assertThat(result.getCatalog()).isEmpty();
        assertThat(result.getSchema()).isEmpty();
    }

    @Test
    void toConnectorSession_propertiesAreImmutable() {
        QuerySession ctx = new QuerySession("q-6", Map.of("key", "value"), null, null, java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture());

        ConnectorSession result = SessionContextConverter.toConnectorSession(ctx);

        assertThat(result.getProperties()).containsEntry("key", "value");
        org.junit.jupiter.api.Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> result.getProperties().put("new", "entry"));
    }

    // ── Coordinator → Metadata SessionContext ──

    @Test
    void toMetadataSessionContext_mapsAllFields() {
        UserIdentity identity = new UserIdentity("alice", Set.of(), Map.of(), Instant.now());
        QuerySession ctx = new QuerySession(
                "q-1",
                Map.of("catalog", "iceberg", "schema", "prod", "timezone", "UTC", "locale", "en_US"),
                identity, "tenant-1", java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture());

        org.pangrova.metadata.api.session.MetadataSessionContext result =
                SessionContextConverter.toMetadataSessionContext(ctx);

        assertThat(result.user()).isEqualTo("alice");
        assertThat(result.currentCatalog()).isEqualTo("iceberg");
        assertThat(result.currentSchema()).isEqualTo("prod");
        assertThat(result.timezone()).isEqualTo("UTC");
        assertThat(result.locale()).isEqualTo("en_US");
        assertThat(result.tenantId()).isEqualTo("tenant-1");
        assertThat(result.queryId()).isEqualTo("q-1");
        assertThat(result.securityToken()).isNull();
    }

    @Test
    void toMetadataSessionContext_handlesNullIdentity() {
        QuerySession ctx = new QuerySession("q-2", Map.of(), null, null, java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture());

        org.pangrova.metadata.api.session.MetadataSessionContext result =
                SessionContextConverter.toMetadataSessionContext(ctx);

        assertThat(result.user()).isEqualTo("anonymous");
        assertThat(result.currentCatalog()).isNull();
        assertThat(result.currentSchema()).isNull();
    }

    @Test
    void toMetadataSessionContext_connectorPropertiesAreImmutable() {
        QuerySession ctx = new QuerySession("q-3", Map.of("key", "val"), null, null, java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture());

        org.pangrova.metadata.api.session.MetadataSessionContext result =
                SessionContextConverter.toMetadataSessionContext(ctx);

        assertThat(result.connectorProperties()).containsEntry("key", "val");
        org.junit.jupiter.api.Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> result.connectorProperties().put("new", "entry"));
    }

    // ── Round-trip: SPI → Coordinator → ConnectorSession ──

    @Test
    void roundTrip_spiToCoordinatorToConnectorSession_preservesUser() {
        org.pangrova.common.session.SessionContext spi = buildSpiSession(
                "alice", Optional.of("cat1"), Optional.of("schema1"), Map.of());

        QuerySession coordinator = SessionContextConverter.fromSpiSessionContext("q-rt", spi, new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory());
        ConnectorSession connector = SessionContextConverter.toConnectorSession(coordinator);

        assertThat(connector.getUser()).isEqualTo("alice");
        assertThat(connector.getCatalog()).contains("cat1");
        assertThat(connector.getSchema()).contains("schema1");
    }

    @Test
    void roundTrip_spiToCoordinatorToMetadata_preservesUser() {
        org.pangrova.common.session.SessionContext spi = buildSpiSession(
                "bob", Optional.of("paimon"), Optional.of("staging"),
                Map.of("timezone", "Asia/Shanghai"));

        QuerySession coordinator = SessionContextConverter.fromSpiSessionContext("q-rt2", spi, new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory());
        org.pangrova.metadata.api.session.MetadataSessionContext metadata =
                SessionContextConverter.toMetadataSessionContext(coordinator);

        assertThat(metadata.user()).isEqualTo("bob");
        assertThat(metadata.currentCatalog()).isEqualTo("paimon");
        assertThat(metadata.currentSchema()).isEqualTo("staging");
        assertThat(metadata.timezone()).isEqualTo("Asia/Shanghai");
        assertThat(metadata.queryId()).isEqualTo("q-rt2");
    }

    // ── Metadata → ConnectorSession (via DefaultConnectorSession) ──

    @Test
    void metadataToConnectorSession_excludesEngineInternalFields() {
        org.pangrova.metadata.api.session.MetadataSessionContext metaCtx =
                new org.pangrova.metadata.api.session.MetadataSessionContext(
                        "alice", "iceberg", "prod", "UTC", "en_US",
                        Map.of("key", "val"), "tenant-1", "secret-token", "q-1");

        ConnectorSession connector =
                org.pangrova.metadata.connector.DefaultConnectorSession.fromSessionContext(metaCtx);

        assertThat(connector.getUser()).isEqualTo("alice");
        assertThat(connector.getCatalog()).contains("iceberg");
        assertThat(connector.getSchema()).contains("prod");
        // tenantId, securityToken, queryId must NOT propagate to connector
        assertThat(connector.getProperties()).doesNotContainKey("tenantId");
        assertThat(connector.getProperties()).doesNotContainKey("securityToken");
        assertThat(connector.getProperties()).doesNotContainKey("queryId");
        // timezone and locale propagate as properties
        assertThat(connector.getProperties()).containsEntry("timezone", "UTC");
        assertThat(connector.getProperties()).containsEntry("locale", "en_US");
    }

    // ── Helper ──

    private static org.pangrova.common.session.SessionContext buildSpiSession(
            String user, Optional<String> catalog, Optional<String> schema,
            Map<String, String> properties) {
        return new org.pangrova.common.session.SessionContext() {
            @Override public String getUser() { return user; }
            @Override public Optional<String> getCatalog() { return catalog; }
            @Override public Optional<String> getSchema() { return schema; }
            @Override public Map<String, String> getProperties() { return properties; }
        };
    }
}
