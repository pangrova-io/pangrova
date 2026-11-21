package org.pangrova.metadata.connector;

import org.pangrova.metadata.api.session.MetadataSessionContext;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class DefaultConnectorSessionTest {
    @Test
    void authoritativeTenantOverridesBothPropertySpellingsWithoutPropagatingToken() {
        var context = context("tenant-a", "secret-token", Map.of(
                "tenant_id", "forged-a", "pangrova.tenant_id", "forged-b", "timezone", "UTC"));
        var session = DefaultConnectorSession.fromSessionContext(context);
        assertThat(session.getProperties()).containsEntry("tenant_id", "tenant-a")
                .containsEntry("timezone", "UTC").doesNotContainKey("pangrova.tenant_id")
                .doesNotContainValue("secret-token").doesNotContainValue("query-a");
    }

    @Test
    void missingAuthoritativeTenantCannotBeInjectedByProperties() {
        var session = DefaultConnectorSession.fromSessionContext(context(null, null,
                Map.of("tenant_id", "forged", "pangrova.tenant_id", "also-forged")));
        assertThat(session.getProperties()).doesNotContainKeys("tenant_id", "pangrova.tenant_id");
    }

    @Test
    void qualifiedLookupUsesResolvedCatalogAndSchemaNotSessionDefaults() {
        var session = DefaultConnectorSession.fromSessionContext(context("tenant-a", null,
                Map.of("catalog", "forged", "schema", "forged")), "target", "analytics");
        assertThat(session.getCatalog()).contains("target");
        assertThat(session.getSchema()).contains("analytics");
        assertThat(session.getProperties()).containsEntry("catalog", "target")
                .containsEntry("schema", "analytics").containsEntry("tenant_id", "tenant-a");
    }

    private static MetadataSessionContext context(String tenant, String token, Map<String, String> properties) {
        return new MetadataSessionContext("user-a", "cat", "schema", null, null, properties,
                tenant, token, "query-a");
    }
}
