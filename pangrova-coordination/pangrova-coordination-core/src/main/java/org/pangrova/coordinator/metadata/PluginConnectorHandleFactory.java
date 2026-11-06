package org.pangrova.coordinator.metadata;

import org.pangrova.common.lifecycle.ResourceCloser;
import org.pangrova.connector.runtime.classloader.ConnectorClassLoaderFactory;
import org.pangrova.connector.runtime.classloader.DefaultConnectorClassLoaderFactory;
import org.pangrova.connector.runtime.discovery.ConnectorFactoryRegistry;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.catalog.CatalogHandle;
import org.pangrova.metadata.catalog.handle.connector.ConnectorCatalogHandle;
import org.pangrova.metadata.spi.catalog.ConnectorHandleFactory;
import org.pangrova.metadata.api.error.catalog.CatalogProviderNotFoundException;
import org.pangrova.security.api.secret.SecretResolver;
import org.pangrova.security.core.secret.SecretTemplates;
import org.pangrova.connector.spi.plugin.Connector;
import org.pangrova.connector.api.plugin.ConnectorContext;
import org.pangrova.connector.spi.plugin.ConnectorFactory;
import org.pangrova.connector.spi.metadata.NativeDatasetControlPlane;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;
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
    private final ConnectorClassLoaderFactory classLoaderFactory;
    private final ConnectorFactoryRegistry factories;
    private final SecretResolver secretResolver;
    private final Function<CatalogDescriptor, Optional<NativeDatasetControlPlane>> nativeControl;
    private final Function<CatalogDescriptor, Optional<org.pangrova.connector.spi.metadata.ManagedSourceCatalog>>
            sourceCatalog;
    private final Map<String, ClassLoader> classLoaders = new ConcurrentHashMap<>();

    /** Creates a factory for catalogs with no managed source publication capability. */
    public PluginConnectorHandleFactory(ClassLoader spiClassLoader, SecretResolver secretResolver,
            Function<CatalogDescriptor, Optional<NativeDatasetControlPlane>> nativeControl) {
        this(spiClassLoader, secretResolver, nativeControl, descriptor -> Optional.empty());
    }

    /** Creates a factory with managed source publication wiring. */
    public PluginConnectorHandleFactory(ClassLoader spiClassLoader, SecretResolver secretResolver,
            Function<CatalogDescriptor, Optional<NativeDatasetControlPlane>> nativeControl,
            Function<CatalogDescriptor, Optional<org.pangrova.connector.spi.metadata.ManagedSourceCatalog>>
                    sourceCatalog) {
        this(spiClassLoader, secretResolver, nativeControl, sourceCatalog,
                new DefaultConnectorClassLoaderFactory());
    }

    /** Creates a factory with an explicit classloader isolation policy. */
    public PluginConnectorHandleFactory(ClassLoader spiClassLoader, SecretResolver secretResolver,
            Function<CatalogDescriptor, Optional<NativeDatasetControlPlane>> nativeControl,
            Function<CatalogDescriptor, Optional<org.pangrova.connector.spi.metadata.ManagedSourceCatalog>>
                    sourceCatalog,
            ConnectorClassLoaderFactory classLoaderFactory) {
        this.spiClassLoader = spiClassLoader;
        this.classLoaderFactory = java.util.Objects.requireNonNull(
                classLoaderFactory, "classLoaderFactory");
        this.secretResolver = secretResolver;
        this.nativeControl = java.util.Objects.requireNonNull(nativeControl, "nativeControl");
        this.sourceCatalog = java.util.Objects.requireNonNull(sourceCatalog);
        this.factories = ConnectorFactoryRegistry.discover(spiClassLoader);
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
        classLoaders.values().forEach(loader -> {
            if (loader instanceof AutoCloseable closeable) {
                ResourceCloser.closeQuietly(closeable);
            }
        });
        classLoaders.clear();
    }

    private ConnectorFactory resolveFactory(String provider) {
        return factories.find(provider).orElseThrow(() -> new CatalogProviderNotFoundException(
                "No ConnectorFactory found for provider '" + provider + "'"));
    }

    private Connector createConnector(ConnectorFactory factory, CatalogDescriptor descriptor) {
        ensureIsolatedClassLoader(descriptor.provider());
        Map<String, String> resolvedProps = resolveProperties(descriptor.properties());
        Optional<NativeDatasetControlPlane> port = java.util.Objects.requireNonNull(nativeControl.apply(descriptor));
        var source = java.util.Objects.requireNonNull(sourceCatalog.apply(descriptor));
        ConnectorContext context = new ConnectorContext() {
            @Override public Optional<NativeDatasetControlPlane> nativeDatasetControlPlane() { return port; }
            @Override public Optional<org.pangrova.connector.spi.metadata.ManagedSourceCatalog> managedSourceCatalog() { return source; }
        };
        return factory.create(descriptor.name(), resolvedProps, context);
    }

    private Map<String, String> resolveProperties(Map<String, String> properties) {
        Map<String, String> resolved = new HashMap<>(properties.size());
        properties.forEach((key, value) -> resolved.put(
                key, SecretTemplates.resolve(secretResolver, value, null)));
        return resolved;
    }

    private void ensureIsolatedClassLoader(String provider) {
        classLoaders.computeIfAbsent(provider, this::createIsolatedClassLoader);
    }

    private ClassLoader createIsolatedClassLoader(String provider) {
        // In classpath deployment, connector JARs are already available via spiClassLoader.
        // The plugin classloader provides an isolation boundary per connector type.
        return classLoaderFactory.createClassLoader(provider, new URL[0], spiClassLoader);
    }

}
