package org.pangrova.coordinator.metadata;

import org.pangrova.common.lifecycle.ResourceCloser;
import org.pangrova.coordinator.plugin.PluginClassLoader;
import org.pangrova.metadata.catalog.CatalogDescriptor;
import org.pangrova.metadata.catalog.CatalogHandle;
import org.pangrova.metadata.catalog.ConnectorCatalogHandle;
import org.pangrova.metadata.catalog.ConnectorHandleFactory;
import org.pangrova.metadata.exception.catalog.CatalogProviderNotFoundException;
import org.pangrova.security.secret.SecretResolver;
import org.pangrova.spi.connector.Connector;
import org.pangrova.spi.connector.ConnectorContext;
import org.pangrova.spi.connector.ConnectorFactory;
import org.pangrova.spi.connector.metadata.NativeDatasetControlPlane;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Optional;
import java.util.function.Function;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Bridges PluginClassLoader-based connector discovery to CatalogRegistry.
 * Each connector type gets an isolated ClassLoader to prevent dependency conflicts.
 */
public final class PluginConnectorHandleFactory implements ConnectorHandleFactory {

    private static final Logger LOG = LoggerFactory.getLogger(PluginConnectorHandleFactory.class);

    private final ClassLoader spiClassLoader;
    private final SecretResolver secretResolver;
    private final Function<CatalogDescriptor, Optional<NativeDatasetControlPlane>> nativeControl;
    private final Function<CatalogDescriptor, Optional<org.pangrova.spi.connector.metadata.ManagedSourceCatalog>>
            sourceCatalog;
    private final Map<String, ConnectorFactory> factoryCache = new ConcurrentHashMap<>();
    private final Map<String, PluginClassLoader> classLoaders = new ConcurrentHashMap<>();

    /** Creates a factory for catalogs with no managed source publication capability. */
    public PluginConnectorHandleFactory(ClassLoader spiClassLoader, SecretResolver secretResolver,
            Function<CatalogDescriptor, Optional<NativeDatasetControlPlane>> nativeControl) {
        this(spiClassLoader, secretResolver, nativeControl, descriptor -> Optional.empty());
    }

    /** Creates a factory with managed source publication wiring. */
    public PluginConnectorHandleFactory(ClassLoader spiClassLoader, SecretResolver secretResolver,
            Function<CatalogDescriptor, Optional<NativeDatasetControlPlane>> nativeControl,
            Function<CatalogDescriptor, Optional<org.pangrova.spi.connector.metadata.ManagedSourceCatalog>>
                    sourceCatalog) {
        this.spiClassLoader = spiClassLoader;
        this.secretResolver = secretResolver;
        this.nativeControl = java.util.Objects.requireNonNull(nativeControl, "nativeControl");
        this.sourceCatalog = java.util.Objects.requireNonNull(sourceCatalog);
        discoverFactories();
    }

    @Override
    public CatalogHandle create(CatalogDescriptor descriptor) {
        String provider = descriptor.provider();
        ConnectorFactory factory = resolveFactory(provider);
        Connector connector = createConnector(factory, descriptor);
        LOG.info("event=connector_created connector={} provider={}", descriptor.name(), provider);
        return new ConnectorCatalogHandle(descriptor, connector);
    }

    /**
     * Shuts down all isolated classloaders. Call on registry shutdown.
     */
    public void shutdown() {
        classLoaders.values().forEach(ResourceCloser::closeQuietly);
        classLoaders.clear();
        factoryCache.clear();
    }

    private void discoverFactories() {
        ServiceLoader<ConnectorFactory> loader =
                ServiceLoader.load(ConnectorFactory.class, spiClassLoader);
        for (ConnectorFactory factory : loader) {
            factoryCache.put(factory.getName(), factory);
            LOG.info("event=connector_factory_discovered factory={}", factory.getName());
        }
    }

    private ConnectorFactory resolveFactory(String provider) {
        ConnectorFactory factory = factoryCache.get(provider);
        if (factory == null) {
            throw new CatalogProviderNotFoundException(
                    "No ConnectorFactory found for provider '" + provider + "'");
        }
        return factory;
    }

    private Connector createConnector(ConnectorFactory factory, CatalogDescriptor descriptor) {
        ensureIsolatedClassLoader(descriptor.provider());
        Map<String, String> resolvedProps = resolveProperties(descriptor.properties());
        Optional<NativeDatasetControlPlane> port = java.util.Objects.requireNonNull(nativeControl.apply(descriptor));
        var source = java.util.Objects.requireNonNull(sourceCatalog.apply(descriptor));
        ConnectorContext context = new ConnectorContext() {
            @Override public Optional<NativeDatasetControlPlane> nativeDatasetControlPlane() { return port; }
            @Override public Optional<org.pangrova.spi.connector.metadata.ManagedSourceCatalog> managedSourceCatalog() { return source; }
        };
        return factory.create(descriptor.name(), resolvedProps, context);
    }

    private Map<String, String> resolveProperties(Map<String, String> properties) {
        Map<String, String> resolved = new HashMap<>(properties.size());
        properties.forEach((key, value) -> resolved.put(key, secretResolver.resolve(value)));
        return resolved;
    }

    private void ensureIsolatedClassLoader(String provider) {
        classLoaders.computeIfAbsent(provider, this::createIsolatedClassLoader);
    }

    private PluginClassLoader createIsolatedClassLoader(String provider) {
        // In classpath deployment, connector JARs are already available via spiClassLoader.
        // The PluginClassLoader provides isolation boundary per connector type.
        return new PluginClassLoader(provider, new URL[0], spiClassLoader);
    }

}
