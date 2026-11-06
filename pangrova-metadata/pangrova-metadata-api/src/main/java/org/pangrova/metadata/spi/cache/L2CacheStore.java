package org.pangrova.metadata.spi.cache;

import org.pangrova.common.api.annotation.Experimental;
import java.util.Optional;

/**
 * Pluggable L2 cache store (e.g., Redis in production).
 */
@Experimental
public interface L2CacheStore {

    Optional<String> get(String key);

    void put(String key, String value, long ttlSeconds);

    void delete(String key);

    void deleteByPrefix(String prefix);

    void publishInvalidation(String key);
}
