package org.pangrova.testing.e2e.setup;

import org.pangrova.metadata.catalog.CatalogDescriptor;
import org.pangrova.metadata.catalog.CatalogHandle;
import org.pangrova.metadata.catalog.ConnectorCatalogHandle;
import org.pangrova.metadata.catalog.ConnectorHandleFactory;
import org.pangrova.spi.connector.Connector;
import org.pangrova.spi.connector.ConnectorContext;
import org.pangrova.spi.connector.ConnectorFactory;
import org.pangrova.spi.connector.metadata.NativeDatasetControlPlane;
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

    /** Discovers real plugins and supplies their catalog-bound Worker metadata control port. */
    public TestConnectorHandleFactory(Function<CatalogDescriptor, Optional<NativeDatasetControlPlane>> controlPlanes) {
        this.controlPlanes = Objects.requireNonNull(controlPlanes, "controlPlanes");
        discoverFactories();
    }

    @Override
    public CatalogHandle create(CatalogDescriptor descriptor) {
        ConnectorFactory factory = resolveFactory(descriptor.provider());
        Connector connector = factory.create(
                descriptor.name(), descriptor.properties(), new CatalogContext(controlPlanes.apply(descriptor)));
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

    private record CatalogContext(Optional<NativeDatasetControlPlane> control) implements ConnectorContext {
        private CatalogContext { Objects.requireNonNull(control, "control"); }
        @Override
        public Optional<NativeDatasetControlPlane> nativeDatasetControlPlane() { return control; }
    }
}
