package org.pangrova.coordinator.metadata;

import static org.pangrova.metadata.testinfra.TableStoreFixtures.memoryTableStores;

import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.catalog.CatalogHandle;
import org.pangrova.metadata.api.enums.CatalogState;
import org.pangrova.metadata.api.enums.CatalogType;
import org.pangrova.metadata.api.exception.CatalogProviderNotFoundException;
import org.pangrova.security.secret.resolution.SecretResolverRegistry;
import org.junit.jupiter.api.Test;

import org.pangrova.metadata.catalog.federation.PluginFederationHandleFactory;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests PluginFederationHandleFactory with ServiceLoader-discovered FederatedCatalogFactory.
 * Validates: Requirements 97.1, 97.2, 97.3, 97.4.
 */
class PluginFederationHandleFactoryTest {

    @Test
    void unknownProviderThrowsCatalogProviderNotFound() {
        PluginFederationHandleFactory factory =
                new PluginFederationHandleFactory(getClass().getClassLoader(), new SecretResolverRegistry(),
                        new org.pangrova.metadata.cache.TieredMetadataCache());

        CatalogDescriptor descriptor = federationDescriptor("unknown_fed", "nonexistent");

        assertThatThrownBy(() -> factory.create(descriptor))
                .isInstanceOf(CatalogProviderNotFoundException.class)
                .hasMessageContaining("nonexistent");
    }

    @Test
    void metadataBootstrapWithFederationFactory() {
        MetadataBootstrapTest.InMemoryCatalogDeployments store = new MetadataBootstrapTest.InMemoryCatalogDeployments();
        PluginConnectorHandleFactory connectorFactory =
                new PluginConnectorHandleFactory(getClass().getClassLoader(), new SecretResolverRegistry(), descriptor -> java.util.Optional.empty());
        PluginFederationHandleFactory federationFactory =
                new PluginFederationHandleFactory(getClass().getClassLoader(), new SecretResolverRegistry(),
                        new org.pangrova.metadata.cache.TieredMetadataCache());

        MetadataBootstrap bootstrap = MetadataBootstrap.initialize(memoryTableStores(),
                store, new org.pangrova.coordinator.metadata.UnsupportedTableProjectionRepository(), new org.pangrova.coordinator.metadata.UnsupportedMetadataProjectionTransactions(), new org.pangrova.coordinator.metadata.UnsupportedMetadataSyncRepository(), connectorFactory, federationFactory);

        assertThat(bootstrap.catalogRegistry()).isNotNull();
        assertThat(bootstrap.metadataService()).isNotNull();

        bootstrap.shutdown();
        connectorFactory.shutdown();
    }

    @Test
    void connectorCatalogStillWorksWithBothFactories() {
        MetadataBootstrapTest.InMemoryCatalogDeployments store = new MetadataBootstrapTest.InMemoryCatalogDeployments();
        store.saveCatalogDescriptor(connectorDescriptor("test_iceberg", "iceberg"));

        PluginConnectorHandleFactory connectorFactory =
                new PluginConnectorHandleFactory(getClass().getClassLoader(), new SecretResolverRegistry(), descriptor -> java.util.Optional.empty());
        PluginFederationHandleFactory federationFactory =
                new PluginFederationHandleFactory(getClass().getClassLoader(), new SecretResolverRegistry(),
                        new org.pangrova.metadata.cache.TieredMetadataCache());

        MetadataBootstrap bootstrap = MetadataBootstrap.initialize(memoryTableStores(),
                store, new org.pangrova.coordinator.metadata.UnsupportedTableProjectionRepository(), new org.pangrova.coordinator.metadata.UnsupportedMetadataProjectionTransactions(), new org.pangrova.coordinator.metadata.UnsupportedMetadataSyncRepository(), connectorFactory, federationFactory);

        assertThat(bootstrap.catalogRegistry().listCatalogNames()).contains("test_iceberg");

        CatalogHandle handle = bootstrap.catalogRegistry().getCatalog("test_iceberg");
        assertThat(handle.getState()).isEqualTo(CatalogState.ACTIVE);
        assertThat(handle.getMetadata(null)).isNotNull();

        bootstrap.shutdown();
        connectorFactory.shutdown();
    }

    @Test
    void catalogHealthStatusTrackedPerCatalog() {
        MetadataBootstrapTest.InMemoryCatalogDeployments store = new MetadataBootstrapTest.InMemoryCatalogDeployments();
        store.saveCatalogDescriptor(connectorDescriptor("ice_cat", "iceberg"));

        PluginConnectorHandleFactory connectorFactory =
                new PluginConnectorHandleFactory(getClass().getClassLoader(), new SecretResolverRegistry(), descriptor -> java.util.Optional.empty());
        MetadataBootstrap bootstrap = MetadataBootstrap.initialize(memoryTableStores(), store, new org.pangrova.coordinator.metadata.UnsupportedTableProjectionRepository(), new org.pangrova.coordinator.metadata.UnsupportedMetadataProjectionTransactions(), new org.pangrova.coordinator.metadata.UnsupportedMetadataSyncRepository(), connectorFactory);

        CatalogHandle handle = bootstrap.catalogRegistry().getCatalog("ice_cat");
        assertThat(handle.checkHealth()).isNotNull();
        assertThat(handle.getState()).isEqualTo(CatalogState.ACTIVE);

        bootstrap.shutdown();
        connectorFactory.shutdown();
    }

    private static CatalogDescriptor federationDescriptor(String name, String provider) {
        return new CatalogDescriptor(
                name, CatalogType.FEDERATION, provider,
                CatalogState.ACTIVE, Map.of(), "", Set.of());
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
