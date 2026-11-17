package org.pangrova.server.coordinator.bootstrap;

import org.pangrova.cache.api.CacheStoreFactory;
import org.pangrova.config.PangrovaConfiguration;
import org.pangrova.config.constants.CoordinatorOptions;
import org.pangrova.metadata.cache.TieredMetadataCache;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

/** Selects the metadata cache before Catalog handles are created. Provider settings may include credentials. */
public record MetadataCacheDeployment(int maxEntries, Duration ttl, String provider, Map<String, String> settings) {
    private static final int DEFAULT_MAX_ENTRIES = 10_000;
    private static final Duration DEFAULT_TTL = Duration.ofSeconds(60);

    private static final String SETTINGS_PREFIX = "metadata.cache.l2.properties.";

    public MetadataCacheDeployment {
        if (maxEntries <= 0 || ttl.isNegative() || ttl.isZero()) {
            throw new IllegalArgumentException("Metadata L1 requires positive capacity and TTL");
        }
        if (provider.isBlank()) throw new IllegalArgumentException("Missing metadata L2 provider ID");
        settings = Map.copyOf(settings);
        if (provider.equals("disabled") && !settings.isEmpty()) {
            throw new IllegalArgumentException("Disabled metadata L2 cannot accept provider properties");
        }
    }

    static MetadataCacheDeployment defaults() {
        return new MetadataCacheDeployment(DEFAULT_MAX_ENTRIES, DEFAULT_TTL, "disabled", Map.of());
    }

    static MetadataCacheDeployment from(PangrovaConfiguration config) {
        Map<String, String> properties = new HashMap<>();
        config.toMap().forEach((key, value) -> {
            if (key.startsWith(SETTINGS_PREFIX)) properties.put(key.substring(SETTINGS_PREFIX.length()), value);
            else if (key.startsWith("metadata.cache.") && !key.equals(
                    CoordinatorOptions.METADATA_CACHE_L1_MAX_SIZE.key())
                    && !key.equals(CoordinatorOptions.METADATA_CACHE_L1_TTL.key()) && !key.equals(
                            CoordinatorOptions.METADATA_CACHE_L2_PROVIDER.key())) {
                throw new IllegalArgumentException("Unknown metadata cache option: " + key);
            }
        });
        return new MetadataCacheDeployment(config.getInt(CoordinatorOptions.METADATA_CACHE_L1_MAX_SIZE),
                config.getDuration(CoordinatorOptions.METADATA_CACHE_L1_TTL),
                config.getString(CoordinatorOptions.METADATA_CACHE_L2_PROVIDER), properties);
    }

    TieredMetadataCache open(ClassLoader loader) {
        if (provider.equals("disabled")) return new TieredMetadataCache(maxEntries, ttl, null);
        Map<String, CacheStoreFactory> factories = new HashMap<>();
        for (var factory : ServiceLoader.load(CacheStoreFactory.class, loader)) {
            String id = factory.id();
            if (id == null || id.isBlank() || !id.equals(id.strip()) || id.equals("disabled")) {
                throw new IllegalArgumentException("Invalid metadata cache provider ID: " + id);
            }
            if (factory.apiVersion() != CacheStoreFactory.API_VERSION) {
                throw new IllegalArgumentException("Incompatible cache provider: " + id);
            }
            if (factories.putIfAbsent(id, factory) != null) {
                throw new IllegalArgumentException("Duplicate cache provider: " + id);
            }
        }
        var factory = factories.get(provider);
        if (factory == null) throw new IllegalArgumentException("Missing metadata cache provider: " + provider);
        var store = java.util.Objects.requireNonNull(factory.open(settings), "Cache provider returned null");
        try {
            return new TieredMetadataCache(maxEntries, ttl, store);
        } catch (RuntimeException | Error failure) {
            try {
                store.close();
            } catch (RuntimeException | Error cleanup) {
                if (cleanup != failure) failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    @Override public String toString() {
        return "MetadataCacheDeployment[maxEntries=" + maxEntries + ", ttl=" + ttl + ", provider=" + provider + "]";
    }
}
