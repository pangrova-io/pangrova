package org.pangrova.coordinator.metadata;

import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.core.catalog.CatalogHandle;
import org.pangrova.metadata.api.catalog.CatalogState;
import org.pangrova.metadata.api.catalog.CatalogType;
import org.pangrova.metadata.api.error.catalog.CatalogProviderNotFoundException;
import org.pangrova.security.secret.SecretResolver;
import org.junit.jupiter.api.Test;

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
                new PluginFederationHandleFactory(getClass().getClassLoader(), new SecretResolver());

        CatalogDescriptor descriptor = federationDescriptor("unknown_fed", "nonexistent");

        assertThatThrownBy(() -> factory.create(descriptor))
                .isInstanceOf(CatalogProviderNotFoundException.class)
                .hasMessageContaining("nonexistent");
    }

    @Test
    void metadataBootstrapWithFederationFactory() {
        MetadataBootstrapTest.InMemoryMetadataStore store = new MetadataBootstrapTest.InMemoryMetadataStore();
        PluginConnectorHandleFactory connectorFactory =
                new PluginConnectorHandleFactory(getClass().getClassLoader(), new SecretResolver(), descriptor -> java.util.Optional.empty());
        PluginFederationHandleFactory federationFactory =
                new PluginFederationHandleFactory(getClass().getClassLoader(), new SecretResolver());

        MetadataBootstrap bootstrap = MetadataBootstrap.initialize(
                store, connectorFactory, federationFactory);

        assertThat(bootstrap.catalogRegistry()).isNotNull();
        assertThat(bootstrap.metadataGateway()).isNotNull();

        bootstrap.shutdown();
        connectorFactory.shutdown();
    }

    @Test
    void connectorCatalogStillWorksWithBothFactories() {
        MetadataBootstrapTest.InMemoryMetadataStore store = new MetadataBootstrapTest.InMemoryMetadataStore();
        store.saveCatalogDescriptor(connectorDescriptor("test_iceberg", "iceberg"));

        PluginConnectorHandleFactory connectorFactory =
                new PluginConnectorHandleFactory(getClass().getClassLoader(), new SecretResolver(), descriptor -> java.util.Optional.empty());
        PluginFederationHandleFactory federationFactory =
                new PluginFederationHandleFactory(getClass().getClassLoader(), new SecretResolver());

        MetadataBootstrap bootstrap = MetadataBootstrap.initialize(
                store, connectorFactory, federationFactory);

        assertThat(bootstrap.catalogRegistry().listCatalogNames()).contains("test_iceberg");

        CatalogHandle handle = bootstrap.catalogRegistry().getCatalog("test_iceberg");
        assertThat(handle.getState()).isEqualTo(CatalogState.ACTIVE);
        assertThat(handle.getMetadata(null)).isNotNull();

        bootstrap.shutdown();
        connectorFactory.shutdown();
    }

    @Test
    void catalogHealthStatusTrackedPerCatalog() {
        MetadataBootstrapTest.InMemoryMetadataStore store = new MetadataBootstrapTest.InMemoryMetadataStore();
        store.saveCatalogDescriptor(connectorDescriptor("ice_cat", "iceberg"));

        PluginConnectorHandleFactory connectorFactory =
                new PluginConnectorHandleFactory(getClass().getClassLoader(), new SecretResolver(), descriptor -> java.util.Optional.empty());
        MetadataBootstrap bootstrap = MetadataBootstrap.initialize(store, connectorFactory);

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
