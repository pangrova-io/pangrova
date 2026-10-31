package org.pangrova.metadata.service;

import org.junit.jupiter.api.Test;
import org.pangrova.metadata.api.object.QualifiedTableRef;

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

    @Test
    void resolutionKeyStoresOnlySecretDigestAndNormalizesPropertyOrder() {
        var table = new QualifiedTableRef("cat", "schema", "items");
        var key = MetadataResolutionCacheKey.of(context("tenant-a", "secret-token", Map.of("a", "1", "b", "2")), table);
        var reordered = MetadataResolutionCacheKey.of(context("tenant-a", "secret-token", Map.of("b", "2", "a", "1")), table);
        assertThat(key).isEqualTo(reordered);
        assertThat(key.toString()).doesNotContain("secret-token");
        assertThat(key).isNotEqualTo(MetadataResolutionCacheKey.of(context("tenant-a", "rotated-token", Map.of("a", "1", "b", "2")), table));
    }

    private static MetadataSessionContext context(String tenant, String token, Map<String, String> properties) {
        return new MetadataSessionContext("user-a", "cat", "schema", null, null, properties,
                tenant, token, "query-a");
    }
}
