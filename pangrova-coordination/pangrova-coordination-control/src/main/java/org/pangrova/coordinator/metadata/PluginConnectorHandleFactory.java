package org.pangrova.coordinator.metadata;

import org.pangrova.connector.runtime.lifecycle.ConnectorPluginInventory;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.catalog.CatalogHandle;
import org.pangrova.metadata.connector.ConnectorCatalogHandle;
import org.pangrova.metadata.api.catalog.ConnectorHandleFactory;
import org.pangrova.metadata.api.exception.CatalogProviderNotFoundException;
import org.pangrova.security.api.secret.SecretResolver;
import org.pangrova.security.secret.resolution.SecretTemplates;
import org.pangrova.connector.api.plugin.Connector;
import org.pangrova.connector.api.plugin.ConnectorContext;
import org.pangrova.connector.api.plugin.ConnectorFactory;
import org.pangrova.connector.api.enums.ConnectorRole;
import org.pangrova.connector.api.metadata.NativeDatasetControlPlane;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * Creates Coordinator catalog handles from the role-owned connector inventory.
 */
public final class PluginConnectorHandleFactory implements ConnectorHandleFactory {

    private static final Logger LOG = LoggerFactory.getLogger(PluginConnectorHandleFactory.class);

    private final ConnectorPluginInventory plugins;
    private final SecretResolver secretResolver;
    private final Function<CatalogDescriptor, Optional<NativeDatasetControlPlane>> nativeControl;
    private final Function<CatalogDescriptor, Optional<org.pangrova.connector.api.metadata.ManagedSourceCatalog>>
            sourceCatalog;

    /** Creates a factory for catalogs with no managed source publication capability. */
    public PluginConnectorHandleFactory(ClassLoader spiClassLoader, SecretResolver secretResolver,
            Function<CatalogDescriptor, Optional<NativeDatasetControlPlane>> nativeControl) {
        this(spiClassLoader, secretResolver, nativeControl, descriptor -> Optional.empty());
    }

    /** Creates a factory with managed source publication wiring. */
    public PluginConnectorHandleFactory(ClassLoader spiClassLoader, SecretResolver secretResolver,
            Function<CatalogDescriptor, Optional<NativeDatasetControlPlane>> nativeControl,
            Function<CatalogDescriptor, Optional<org.pangrova.connector.api.metadata.ManagedSourceCatalog>>
                    sourceCatalog) {
        this(spiClassLoader, secretResolver, nativeControl, sourceCatalog,
                Optional.empty());
    }

    /** Creates a factory with an optional external plugin root. */
    public PluginConnectorHandleFactory(ClassLoader spiClassLoader, SecretResolver secretResolver,
            Function<CatalogDescriptor, Optional<NativeDatasetControlPlane>> nativeControl,
            Function<CatalogDescriptor, Optional<org.pangrova.connector.api.metadata.ManagedSourceCatalog>>
                    sourceCatalog,
            Optional<Path> pluginDirectory) {
        this.secretResolver = secretResolver;
        this.nativeControl = java.util.Objects.requireNonNull(nativeControl, "nativeControl");
        this.sourceCatalog = java.util.Objects.requireNonNull(sourceCatalog);
        this.plugins = ConnectorPluginInventory.open(spiClassLoader, ConnectorRole.COORDINATOR, pluginDirectory);
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
        plugins.close();
    }

    private ConnectorFactory resolveFactory(String provider) {
        return plugins.registry().find(provider).orElseThrow(() -> new CatalogProviderNotFoundException(
                "No ConnectorFactory found for provider '" + provider + "'"));
    }

    private Connector createConnector(ConnectorFactory factory, CatalogDescriptor descriptor) {
        Map<String, String> resolvedProps = resolveProperties(descriptor.properties());
        Optional<NativeDatasetControlPlane> port = java.util.Objects.requireNonNull(nativeControl.apply(descriptor));
        var source = java.util.Objects.requireNonNull(sourceCatalog.apply(descriptor));
        ConnectorContext context = new ConnectorContext() {
            @Override public Optional<NativeDatasetControlPlane> nativeDatasetControlPlane() { return port; }
            @Override public Optional<org.pangrova.connector.api.metadata.ManagedSourceCatalog> managedSourceCatalog() { return source; }
        };
        factory.validateConfiguration(resolvedProps);
        return factory.create(descriptor.name(), resolvedProps, context);
    }

    private Map<String, String> resolveProperties(Map<String, String> properties) {
        Map<String, String> resolved = new HashMap<>(properties.size());
        properties.forEach((key, value) -> resolved.put(
                key, SecretTemplates.resolve(secretResolver, value, null)));
        return resolved;
    }
}
