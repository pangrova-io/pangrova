package org.pangrova.metadata.core.cache;

import java.util.Optional;

/**
 * Pluggable L2 cache store (e.g., Redis in production).
 */
public interface L2CacheStore {

    Optional<String> get(String key);

    void put(String key, String value, long ttlSeconds);

    void delete(String key);

    void deleteByPrefix(String prefix);

    void publishInvalidation(String key);
}
