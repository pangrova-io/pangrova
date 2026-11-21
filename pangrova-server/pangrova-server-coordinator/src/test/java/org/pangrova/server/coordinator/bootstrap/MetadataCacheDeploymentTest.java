package org.pangrova.server.coordinator.bootstrap;

import org.junit.jupiter.api.Test;
import org.pangrova.config.PangrovaConfiguration;
import org.pangrova.metadata.api.object.TableMetadata;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MetadataCacheDeploymentTest {
    @Test void deploymentLoadsConfiguredMemoryFactoryThroughProductionMainAndClosesItsStore() {
        var config = PangrovaConfiguration.fromYaml(Map.of("metadata.enabled", "true",
                "metadata.jdbc-url", "jdbc:postgresql://metadata/control", "metadata.cache.l1-max-size", "2",
                "metadata.cache.l1-ttl", "500ms", "metadata.cache.l2.provider", "memory",
                "metadata.cache.l2.properties.max-entries", "3"));
        var selected = PangrovaCoordinatorMain.buildBootstrapConfig(config).metadataJdbcConfig().cache();
        assertThat(selected.maxEntries()).isEqualTo(2);
        assertThat(selected.ttl()).isEqualTo(java.time.Duration.ofMillis(500));
        try (var cache = selected.open(getClass().getClassLoader())) {
            var value = new TableMetadata("cat", "s", "t", List.of(), null, null, null, null, null);
            cache.putTable("cat", "s", "t", value);
            assertThat(cache.getTable("cat", "s", "t")).contains(value);
            cache.close();
            assertThatThrownBy(() -> cache.getTable("cat", "s", "t")).isInstanceOf(IllegalStateException.class);
        }
    }

    @Test void missingFactoryUnknownPropertyAndMisplacedCredentialsFailExplicitly() {
        var loader = getClass().getClassLoader();
        assertThatThrownBy(() -> deployment(Map.of("metadata.cache.l2.provider", "absent")).open(loader))
                .hasMessageContaining("Missing metadata cache provider");
        assertThatThrownBy(() -> deployment(Map.of("metadata.cache.l2.provider", "memory",
                "metadata.cache.l2.properties.url", "secret")).open(loader))
                .hasMessageContaining("Unknown memory cache setting");
        assertThatThrownBy(() -> deployment(Map.of("metadata.cache.redis-url", "secret")))
                .hasMessageContaining("Unknown metadata cache option");
        assertThatThrownBy(() -> deployment(Map.of("metadata.cache.l2.properties.url", "secret")))
                .hasMessageContaining("Disabled metadata L2");
    }

    @Test
    void reservedIdFromInstalledProviderRejectsDeploymentBeforeOpeningAnyStore(
            @org.junit.jupiter.api.io.TempDir java.nio.file.Path directory) throws Exception {
        var descriptor = directory.resolve("META-INF/services/" + org.pangrova.cache.api.CacheStoreFactory.class.getName());
        java.nio.file.Files.createDirectories(descriptor.getParent());
        java.nio.file.Files.writeString(descriptor, ReservedCacheFactory.class.getName());
        try (var loader = new java.net.URLClassLoader(new java.net.URL[] {directory.toUri().toURL()},
                getClass().getClassLoader())) {
            assertThatThrownBy(() -> deployment(Map.of("metadata.cache.l2.provider", "memory")).open(loader))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Invalid metadata cache provider ID: disabled");
        }
    }

    public static final class ReservedCacheFactory implements org.pangrova.cache.api.CacheStoreFactory {
        public String id() { return "disabled"; }
        public int apiVersion() { return API_VERSION; }
        public org.pangrova.cache.api.CacheStore open(Map<String, String> settings) {
            throw new AssertionError("Reserved provider cannot own a store");
        }
    }

    private static MetadataCacheDeployment deployment(Map<String, String> properties) {
        return MetadataCacheDeployment.from(PangrovaConfiguration.fromYaml(properties));
    }
}
