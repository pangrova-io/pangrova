package org.pangrova.indexing.registry;

import org.pangrova.indexing.spi.strategy.IndexStrategyProvider;
import org.pangrova.indexing.api.descriptor.IndexType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumMap;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * Registry of {@link IndexStrategyProvider} implementations discovered via
 * {@link ServiceLoader}. Immutable and thread-safe after construction.
 *
 * <p>Each {@link IndexType} maps to exactly one provider. Duplicate
 * registrations for the same type cause an {@link IllegalStateException}
 * at construction time.</p>
 *
 * <p>{@link ServiceLoader} instantiation failures fail fast — providers
 * declared in {@code META-INF/services} must be loadable with a no-arg
 * constructor. The previous reflective fallback silently masked
 * misconfigured providers (e.g. accidental parameterized constructors)
 * and is intentionally removed.</p>
 */
public final class IndexEngineRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(IndexEngineRegistry.class);

    private final Map<IndexType, IndexStrategyProvider> providers;

    /**
     * Creates a registry by discovering providers via ServiceLoader.
     *
     * @throws IllegalStateException if two providers claim the same IndexType,
     *         or if any declared provider cannot be instantiated
     */
    public IndexEngineRegistry() {
        var map = new EnumMap<IndexType, IndexStrategyProvider>(IndexType.class);
        ServiceLoader<IndexStrategyProvider> loader = ServiceLoader.load(IndexStrategyProvider.class);
        for (ServiceLoader.Provider<IndexStrategyProvider> sp : loader.stream().toList()) {
            try {
                registerProvider(map, sp.get());
            } catch (java.util.ServiceConfigurationError e) {
                // Fail fast — masking instantiation errors with reflective
                // fallback hides real config bugs (missing no-arg ctor,
                // missing dependency, classloader split).
                LOG.error("event=service_loader_failed class={} error={}",
                        sp.type().getName(), e.getMessage());
                throw new IllegalStateException(
                        "Failed to instantiate IndexStrategyProvider " + sp.type().getName()
                                + " — verify it has a public no-arg constructor and is on the classpath",
                        e);
            }
        }
        this.providers = Map.copyOf(map);
    }

    /**
     * Creates a registry from the given providers (for testing).
     *
     * @param providers iterable of providers to register
     * @throws IllegalStateException if two providers claim the same IndexType
     */
    public IndexEngineRegistry(Iterable<IndexStrategyProvider> providers) {
        var map = new EnumMap<IndexType, IndexStrategyProvider>(IndexType.class);
        for (IndexStrategyProvider provider : providers) {
            registerProvider(map, provider);
        }
        this.providers = Map.copyOf(map);
    }

    /**
     * Returns the provider for the given index type.
     *
     * @param type the index type
     * @return the registered provider
     * @throws IllegalArgumentException if no provider is registered for the type
     */
    public IndexStrategyProvider getProvider(IndexType type) {
        IndexStrategyProvider provider = providers.get(type);
        if (provider == null) {
            throw new IllegalArgumentException("No IndexStrategyProvider registered for type " + type);
        }
        return provider;
    }

    /** Returns the number of registered providers. */
    public int size() {
        return providers.size();
    }

    private static void registerProvider(
            EnumMap<IndexType, IndexStrategyProvider> map,
            IndexStrategyProvider provider) {
        IndexType type = provider.supportedType();
        IndexStrategyProvider existing = map.put(type, provider);
        if (existing != null) {
            throw new IllegalStateException(
                    "Duplicate IndexStrategyProvider for type " + type
                            + ": " + existing.getClass().getName()
                            + " and " + provider.getClass().getName());
        }
    }
}
