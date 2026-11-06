package org.pangrova.planner.core.cache;

import java.util.Optional;

/**
 * Interface for L2 cache storage (Redis in production, in-memory stub for testing).
 */
public interface L2CacheStore {

    Optional<CachedResult> get(String key);

    void put(String key, CachedResult value);

    void remove(String key);

    void removeByTable(String tableName);
    void removeByUser(String userId);
}
