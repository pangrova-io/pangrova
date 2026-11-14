package org.pangrova.metadata.cache;

import java.util.Optional;
import org.pangrova.metadata.api.cache.L2CacheStore;

/**
 * No-op L2 cache store when Redis is unavailable.
 */
public class DisabledL2CacheStore implements L2CacheStore {

    @Override
    public Optional<String> get(String key) {
        return Optional.empty();
    }

    @Override
    public void put(String key, String value, long ttlSeconds) {
        // no-op
    }

    @Override
    public void delete(String key) {
        // no-op
    }

    @Override
    public void deleteByPrefix(String prefix) {
        // no-op
    }

    @Override
    public void publishInvalidation(String key) {
        // no-op
    }
}
