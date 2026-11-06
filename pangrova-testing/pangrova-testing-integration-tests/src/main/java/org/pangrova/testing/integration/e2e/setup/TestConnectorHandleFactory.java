package org.pangrova.testing.integration.e2e.setup;

import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.catalog.CatalogHandle;
import org.pangrova.metadata.catalog.handle.connector.ConnectorCatalogHandle;
import org.pangrova.metadata.spi.catalog.ConnectorHandleFactory;
import org.pangrova.connector.spi.plugin.Connector;
import org.pangrova.connector.api.plugin.ConnectorContext;
import org.pangrova.connector.spi.plugin.ConnectorFactory;
import org.pangrova.connector.spi.metadata.NativeDatasetControlPlane;
import org.pangrova.connector.spi.metadata.ManagedSourceCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Optional;
import java.util.Objects;
import java.util.function.Function;

/**
 * Test-only ConnectorHandleFactory that discovers ConnectorFactory via ServiceLoader
 * and creates real connector instances for E2E tests.
 */
public final class TestConnectorHandleFactory implements ConnectorHandleFactory {

    private static final Logger LOG = LoggerFactory.getLogger(TestConnectorHandleFactory.class);

    private final Map<String, ConnectorFactory> factories = new ConcurrentHashMap<>();
    private final Function<CatalogDescriptor, Optional<NativeDatasetControlPlane>> controlPlanes;
    private final Function<CatalogDescriptor, Optional<ManagedSourceCatalog>> sourceCatalogs;

    /** Discovers real plugins and supplies their catalog-bound Worker metadata control port. */
    public TestConnectorHandleFactory(
            Function<CatalogDescriptor, Optional<NativeDatasetControlPlane>> controlPlanes,
            Function<CatalogDescriptor, Optional<ManagedSourceCatalog>> sourceCatalogs) {
        this.controlPlanes = Objects.requireNonNull(controlPlanes, "controlPlanes");
        this.sourceCatalogs = Objects.requireNonNull(sourceCatalogs, "sourceCatalogs");
        discoverFactories();
    }

    @Override
    public CatalogHandle create(CatalogDescriptor descriptor) {
        ConnectorFactory factory = resolveFactory(descriptor.provider());
        Connector connector = factory.create(descriptor.name(), descriptor.properties(),
                new CatalogContext(controlPlanes.apply(descriptor),
                        sourceCatalogs.apply(descriptor)));
        LOG.info("Created test connector '{}' via provider '{}'",
                descriptor.name(), descriptor.provider());
        return new ConnectorCatalogHandle(descriptor, connector);
    }

    private void discoverFactories() {
        for (ConnectorFactory f : ServiceLoader.load(ConnectorFactory.class)) {
            factories.put(f.getName(), f);
            LOG.info("Discovered test connector factory: {}", f.getName());
        }
    }

    private ConnectorFactory resolveFactory(String provider) {
        ConnectorFactory factory = factories.get(provider);
        if (factory == null) {
            throw new IllegalStateException(
                    "No ConnectorFactory for provider '" + provider + "'. "
                    + "Available: " + factories.keySet());
        }
        return factory;
    }

    private record CatalogContext(Optional<NativeDatasetControlPlane> control,
            Optional<ManagedSourceCatalog> source) implements ConnectorContext {
        private CatalogContext {
            Objects.requireNonNull(control, "control");
            Objects.requireNonNull(source, "source");
        }
        @Override
        public Optional<NativeDatasetControlPlane> nativeDatasetControlPlane() { return control; }
        @Override
        public Optional<ManagedSourceCatalog> managedSourceCatalog() { return source; }
    }
}
