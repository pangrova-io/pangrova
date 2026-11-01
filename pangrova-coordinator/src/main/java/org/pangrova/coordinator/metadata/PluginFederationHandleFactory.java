package org.pangrova.coordinator.metadata;

import org.pangrova.metadata.core.federation.FederatedCatalog;
import org.pangrova.metadata.core.federation.FederatedCatalogFactory;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.core.catalog.CatalogHandle;
import org.pangrova.metadata.core.catalog.FederationCatalogHandle;
import org.pangrova.metadata.core.catalog.FederationHandleFactory;
import org.pangrova.metadata.api.error.catalog.CatalogProviderNotFoundException;
import org.pangrova.security.secret.SecretResolver;
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
        properties.forEach((key, value) -> resolved.put(key, secretResolver.resolve(value)));
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
