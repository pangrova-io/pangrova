package org.pangrova.metadata.resolution;

import org.pangrova.metadata.api.session.MetadataSessionContext;

import org.junit.jupiter.api.Test;
import org.pangrova.metadata.api.object.QualifiedTableRef;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class MetadataResolutionCacheKeyTest {
    @Test
    void resolutionKeyStoresOnlySecretDigestAndNormalizesPropertyOrder() {
        var table = new QualifiedTableRef("cat", "schema", "items");
        var deployment = new org.pangrova.table.api.identity.CatalogDeploymentIdentity("first", 1);
        var key = MetadataResolutionCacheKey.of(context("tenant-a", "secret-token", Map.of("a", "1", "b", "2")), table, deployment);
        var reordered = MetadataResolutionCacheKey.of(context("tenant-a", "secret-token", Map.of("b", "2", "a", "1")), table, deployment);
        assertThat(key).isEqualTo(reordered);
        assertThat(key.toString()).doesNotContain("secret-token");
        assertThat(key).isNotEqualTo(MetadataResolutionCacheKey.of(context("tenant-a", "rotated-token", Map.of("a", "1", "b", "2")), table, deployment));
    }

    private static MetadataSessionContext context(String tenant, String token, Map<String, String> properties) {
        return new MetadataSessionContext("user-a", "cat", "schema", null, null, properties,
                tenant, token, "query-a");
    }
}
