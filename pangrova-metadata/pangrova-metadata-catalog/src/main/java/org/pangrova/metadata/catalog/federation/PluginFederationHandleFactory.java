package org.pangrova.metadata.catalog.federation;

import org.pangrova.metadata.api.catalog.FederatedCatalog;
import org.pangrova.metadata.cache.TieredMetadataCache;
import org.pangrova.metadata.catalog.handle.CachingFederatedCatalog;
import org.pangrova.metadata.api.catalog.FederatedCatalogFactory;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.catalog.CatalogHandle;
import org.pangrova.metadata.api.catalog.FederationHandleFactory;
import org.pangrova.metadata.api.exception.CatalogProviderNotFoundException;
import org.pangrova.security.api.secret.SecretResolver;
import org.pangrova.security.secret.resolution.SecretTemplates;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Objects;

/**
 * Bridges ServiceLoader-discovered FederatedCatalogFactory instances to CatalogRegistry.
 * Federation providers supply their own factory through a plugin.
 */
public final class PluginFederationHandleFactory implements FederationHandleFactory {

    private static final Logger LOG = LoggerFactory.getLogger(PluginFederationHandleFactory.class);

    private final Map<String, FederatedCatalogFactory> factories;
    private final SecretResolver secretResolver;
    private final TieredMetadataCache cache;

    public PluginFederationHandleFactory(ClassLoader classLoader, SecretResolver secretResolver,
            TieredMetadataCache cache) {
        this.cache = Objects.requireNonNull(cache);
        this.secretResolver = Objects.requireNonNull(secretResolver, "secretResolver");
        factories = discoverFactories(classLoader);
    }

    @Override
    public CatalogHandle create(CatalogDescriptor descriptor) {
        String provider = descriptor.provider();
        FederatedCatalogFactory factory = resolveFactory(provider);
        Map<String, String> resolvedProps = resolveProperties(descriptor.properties());
        FederatedCatalog adapter = Objects.requireNonNull(factory.create(resolvedProps),
                "Federated catalog provider returned null: " + provider);
        LOG.info("event=federation_catalog_created catalog={} provider={}", descriptor.name(), provider);
        return new FederationCatalogHandle(descriptor, adapter, new CachingFederatedCatalog(
                adapter, cache, descriptor.identity()));
    }

    private Map<String, String> resolveProperties(Map<String, String> properties) {
        Map<String, String> resolved = new HashMap<>(properties.size());
        properties.forEach((key, value) -> resolved.put(
                key, SecretTemplates.resolve(secretResolver, value, null)));
        return resolved;
    }

    private static Map<String, FederatedCatalogFactory> discoverFactories(ClassLoader classLoader) {
        Map<String, FederatedCatalogFactory> discovered = new HashMap<>();
        ServiceLoader<FederatedCatalogFactory> loader =
                ServiceLoader.load(FederatedCatalogFactory.class, classLoader);
        for (FederatedCatalogFactory factory : loader) {
            String name = factory.name();
            if (name == null || name.isBlank()) {
                throw new IllegalStateException("Blank federated catalog provider ID: " + factory.getClass().getName());
            }
            var previous = discovered.putIfAbsent(name, factory);
            if (previous != null) {
                throw new IllegalStateException("Duplicate federated catalog provider '" + name + "': "
                        + previous.getClass().getName() + " and " + factory.getClass().getName());
            }
            LOG.info("event=federation_adapter_factory_discovered factory={}", name);
        }
        return Map.copyOf(discovered);
    }

    private FederatedCatalogFactory resolveFactory(String provider) {
        FederatedCatalogFactory factory = factories.get(provider);
        if (factory == null) {
            throw new CatalogProviderNotFoundException(
                    "No FederatedCatalogFactory found for provider '" + provider + "'");
        }
        return factory;
    }
}
