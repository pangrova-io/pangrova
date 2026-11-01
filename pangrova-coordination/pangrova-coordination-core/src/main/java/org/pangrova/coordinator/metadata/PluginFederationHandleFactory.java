package org.pangrova.coordinator.metadata;

import org.pangrova.metadata.adapter.FederatedCatalog;
import org.pangrova.metadata.adapter.FederatedCatalogFactory;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.catalog.CatalogHandle;
import org.pangrova.metadata.catalog.handle.federation.FederationCatalogHandle;
import org.pangrova.metadata.catalog.handle.federation.FederationHandleFactory;
import org.pangrova.metadata.exception.catalog.CatalogProviderNotFoundException;
import org.pangrova.spi.security.secret.SecretResolver;
import org.pangrova.spi.security.secret.SecretTemplates;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Bridges ServiceLoader-discovered FederatedCatalogFactory instances to CatalogRegistry.
 * Federation providers supply their own factory through a plugin.
 */
public final class PluginFederationHandleFactory implements FederationHandleFactory {

    private static final Logger LOG = LoggerFactory.getLogger(PluginFederationHandleFactory.class);

    private final Map<String, FederatedCatalogFactory> factoryCache = new ConcurrentHashMap<>();
    private final SecretResolver secretResolver;

    public PluginFederationHandleFactory(ClassLoader classLoader, SecretResolver secretResolver) {
        this.secretResolver = secretResolver;
        discoverFactories(classLoader);
    }

    @Override
    public CatalogHandle create(CatalogDescriptor descriptor) {
        String provider = descriptor.provider();
        FederatedCatalogFactory factory = resolveFactory(provider);
        Map<String, String> resolvedProps = resolveProperties(descriptor.properties());
        FederatedCatalog adapter = factory.create(resolvedProps);
        LOG.info("event=federation_catalog_created catalog={} provider={}", descriptor.name(), provider);
        return new FederationCatalogHandle(descriptor, adapter);
    }

    private Map<String, String> resolveProperties(Map<String, String> properties) {
        Map<String, String> resolved = new HashMap<>(properties.size());
        properties.forEach((key, value) -> resolved.put(
                key, SecretTemplates.resolve(secretResolver, value, null)));
        return resolved;
    }

    private void discoverFactories(ClassLoader classLoader) {
        ServiceLoader<FederatedCatalogFactory> loader =
                ServiceLoader.load(FederatedCatalogFactory.class, classLoader);
        for (FederatedCatalogFactory factory : loader) {
            factoryCache.put(factory.name(), factory);
            LOG.info("event=federation_adapter_factory_discovered factory={}", factory.name());
        }
    }

    private FederatedCatalogFactory resolveFactory(String provider) {
        FederatedCatalogFactory factory = factoryCache.get(provider);
        if (factory == null) {
            throw new CatalogProviderNotFoundException(
                    "No FederatedCatalogFactory found for provider '" + provider + "'");
        }
        return factory;
    }
}
