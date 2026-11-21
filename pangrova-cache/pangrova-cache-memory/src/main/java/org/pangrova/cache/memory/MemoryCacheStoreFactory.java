package org.pangrova.cache.memory;

import org.pangrova.cache.api.CacheStore;
import org.pangrova.cache.api.CacheStoreFactory;

import java.util.Map;
import java.util.Set;

public final class MemoryCacheStoreFactory implements CacheStoreFactory {
    @Override public String id() { return "memory"; }
    @Override public int apiVersion() { return API_VERSION; }

    @Override public CacheStore open(Map<String, String> settings) {
        if (!Set.of("max-entries").containsAll(settings.keySet())) {
            throw new IllegalArgumentException("Unknown memory cache setting");
        }
        int maxEntries = Integer.parseInt(settings.getOrDefault("max-entries", "50000"));
        if (maxEntries < 1) throw new IllegalArgumentException("memory max-entries must be positive");
        return new MemoryCacheStore(maxEntries);
    }
}
