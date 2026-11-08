package org.pangrova.cache.memory;

import org.pangrova.cache.api.CachedResult;
import org.pangrova.cache.api.L2CacheStore;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory {@link L2CacheStore} backed by a {@link ConcurrentHashMap}.
 *
 * <p>Used as the default L2 cache when Redis is not configured (see
 * {@code QueryBootstrap#buildL2CacheStore}). Suitable for single-node
 * deployments and development; for multi-node clusters where cache entries
 * must be shared across coordinators, configure
 * {@code org.pangrova.cache.redis.RedisL2CacheStore} instead.</p>
 *
 * <p>Concurrency: all operations delegate to {@link ConcurrentHashMap} and
 * are safe for concurrent access. Memory is bounded only by the JVM heap —
 * cap entry count externally if hosting untrusted query traffic.</p>
 */
public class InMemoryL2CacheStore implements L2CacheStore {

    private final ConcurrentHashMap<String, CachedResult> store = new ConcurrentHashMap<>();

    @Override
    public Optional<CachedResult> get(String key) {
        return Optional.ofNullable(store.get(key));
    }

    @Override
    public void put(String key, CachedResult value) {
        store.put(key, value);
    }

    @Override
    public void remove(String key) {
        store.remove(key);
    }

    @Override
    public void removeByTable(String tableName) {
        store.keySet().removeIf(key -> key.contains(tableName));
    }
    @Override
    public void removeByUser(String userId) {
        store.keySet().removeIf(key -> key.contains(userId));
    }
}
