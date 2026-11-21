package org.pangrova.coordinator.metadata;

import static org.pangrova.metadata.testinfra.TableStoreFixtures.memoryTableStores;

import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.catalog.CatalogHandle;
import org.pangrova.metadata.api.enums.CatalogState;
import org.pangrova.metadata.api.enums.CatalogType;
import org.pangrova.metadata.api.exception.CatalogProviderNotFoundException;
import org.pangrova.security.secret.resolution.SecretResolverRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests PluginConnectorHandleFactory with real IcebergConnectorFactory and PaimonConnectorFactory.
 * Validates: Requirements 16.1, 16.2, 16.3, 16.4, 16.5.
 */
class PluginConnectorHandleFactoryTest {

    private PluginConnectorHandleFactory factory;

    @Test
    void discoversAllBundledFactoryDescriptors() {
        var discovered = org.pangrova.connector.runtime.discovery.ConnectorFactoryRegistry
                .discover(getClass().getClassLoader());
        assertThat(discovered.names()).contains("iceberg", "paimon", "lance");
    }

    @Test
    void bundledFactoriesPreflightConfigurationInBothRolesWithoutOpeningCatalogs() {
        for (var role : org.pangrova.connector.api.enums.ConnectorRole.values()) {
            var registry = org.pangrova.connector.runtime.discovery.ConnectorFactoryRegistry
                    .discover(getClass().getClassLoader(), role);
            registry.validateConfiguration("iceberg", Map.of("type", "rest", "uri", "http://127.0.0.1:1"));
            registry.validateConfiguration("paimon", Map.of("metastore", "rest", "uri", "http://127.0.0.1:1"));
            registry.validateConfiguration("lance", Map.of("warehouse", "s3://bucket/lance"));
            assertThatThrownBy(() -> registry.validateConfiguration("iceberg", Map.of("catalog-impl", " ")))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("catalog-impl");
            assertThatThrownBy(() -> registry.validateConfiguration("paimon", Map.of("metastore", "rest")))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("uri");
            assertThatThrownBy(() -> registry.validateConfiguration("lance",
                    Map.of("warehouse", "s3://bucket/lance", "jdbc.url", "untrusted")))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("JDBC");
        }
    }

    @BeforeEach
    void setUp() {
        factory = new PluginConnectorHandleFactory(getClass().getClassLoader(), new SecretResolverRegistry(), descriptor -> java.util.Optional.empty());
    }

    @AfterEach
    void tearDown() {
        factory.shutdown();
    }

    @Test
    void createIcebergConnectorHandle() {
        CatalogDescriptor descriptor = connectorDescriptor("iceberg_catalog", "iceberg");

        CatalogHandle handle = factory.create(descriptor);

        assertThat(handle).isNotNull();
        assertThat(handle.getDescriptor().name()).isEqualTo("iceberg_catalog");
        assertThat(handle.getState()).isEqualTo(CatalogState.ACTIVE);
    }

    @Test
    void icebergConnectorProvidesMetadata() {
        CatalogDescriptor descriptor = connectorDescriptor("iceberg_catalog", "iceberg");

        CatalogHandle handle = factory.create(descriptor);

        assertThat(handle.getMetadata(null)).isNotNull();
        assertThat(handle.getSplitManager()).isNotNull();
    }

    @Test
    void paimonConnectorCreatesHandle() {
        CatalogDescriptor descriptor = connectorDescriptor("paimon_catalog", "paimon");

        CatalogHandle handle = factory.create(descriptor);

        assertThat(handle).isNotNull();
        assertThat(handle.getDescriptor().name()).isEqualTo("paimon_catalog");
        assertThat(handle.getState()).isEqualTo(CatalogState.ACTIVE);
    }

    @Test
    void unknownProviderThrowsCatalogProviderNotFound() {
        CatalogDescriptor descriptor = connectorDescriptor("unknown_catalog", "nonexistent");

        assertThatThrownBy(() -> factory.create(descriptor))
                .isInstanceOf(CatalogProviderNotFoundException.class)
                .hasMessageContaining("nonexistent");
    }

    @Test
    void bundledFactoryCanCreateMultipleIndependentCatalogs() {
        CatalogDescriptor iceberg1 = connectorDescriptor("ice1", "iceberg");
        CatalogDescriptor iceberg2 = connectorDescriptor("ice2", "iceberg");

        CatalogHandle handle1 = factory.create(iceberg1);
        CatalogHandle handle2 = factory.create(iceberg2);

        // A factory can create distinct catalog handles in one role.
        assertThat(handle1).isNotNull();
        assertThat(handle2).isNotSameAs(handle1);
        handle1.shutdown();
        handle2.shutdown();
    }

    @Test
    void metadataBootstrapWithConnectorFactory() {
        MetadataBootstrapTest.InMemoryCatalogDeployments store = new MetadataBootstrapTest.InMemoryCatalogDeployments();
        store.saveCatalogDescriptor(connectorDescriptor("test_iceberg", "iceberg"));

        MetadataBootstrap bootstrap = MetadataBootstrap.initialize(memoryTableStores(), store, new org.pangrova.coordinator.metadata.UnsupportedTableProjectionRepository(), new org.pangrova.coordinator.metadata.UnsupportedMetadataProjectionTransactions(), new org.pangrova.coordinator.metadata.UnsupportedMetadataSyncRepository(), factory);

        assertThat(bootstrap.catalogRegistry().listCatalogNames()).contains("test_iceberg");
        bootstrap.shutdown();
    }

    @Test
    void shutdownIsIdempotent() {
        factory.shutdown();
        factory.shutdown();
    }

    private static CatalogDescriptor connectorDescriptor(String name, String provider) {
        Map<String, String> properties = "iceberg".equals(provider)
                ? Map.of("type", "hadoop", "warehouse",
                        System.getProperty("java.io.tmpdir") + "/pangrova-plugin-" + name)
                : Map.of();
        return new CatalogDescriptor(
                name, CatalogType.CONNECTOR, provider,
                CatalogState.ACTIVE, properties, "", Set.of());
    }
}
