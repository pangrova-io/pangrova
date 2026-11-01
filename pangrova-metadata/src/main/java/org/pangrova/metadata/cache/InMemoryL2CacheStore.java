package org.pangrova.metadata.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import java.time.Duration;
import java.util.Optional;
import org.pangrova.metadata.api.cache.L2CacheStore;
import org.pangrova.metadata.store.jdbc.cache.JdbcInvalidationPoller;

/**
 * In-process L2 cache store backed by Caffeine. Replaces the former
 * {@code DisabledL2CacheStore} (all no-ops) so that single-node and
 * non-Redis deployments still benefit from a real L2 tier with TTL
 * expiration. In production Redis-backed deployments, use
 * a Redis-backed L2CacheStore instead.
 *
 * <p>Cross-node invalidation ({@link #publishInvalidation}) is a no-op
 * because in-process caches are, by definition, node-local. Cross-node
 * coherence is handled by {@link JdbcInvalidationPoller}.</p>
 */
public final class InMemoryL2CacheStore implements L2CacheStore {

    private static final long DEFAULT_MAX_SIZE = 50_000;
    private static final long DEFAULT_TTL_SECONDS = 300;

    private final Cache<String, String> store;

    public InMemoryL2CacheStore() {
        this(DEFAULT_MAX_SIZE, DEFAULT_TTL_SECONDS);
    }

    /**
     * Creates a store with custom capacity and TTL bounds.
     */
    public InMemoryL2CacheStore(long maxSize, long defaultTtlSeconds) {
        this.store = Caffeine.newBuilder()
                .maximumSize(maxSize)
                .expireAfterWrite(Duration.ofSeconds(defaultTtlSeconds))
                .build();
    }

    @Override
    public Optional<String> get(String key) {
        String value = store.getIfPresent(key);
        return Optional.ofNullable(value);
    }

    @Override
    public void put(String key, String value, long ttlSeconds) {
        // Caffeine does not support per-entry TTL natively; we use the
        // global TTL set at construction. The per-entry ttlSeconds param
        // is accepted for interface compatibility but the cache-wide TTL
        // governs actual expiration.
        store.put(key, value);
    }

    @Override
    public void delete(String key) {
        store.invalidate(key);
    }

    @Override
    public void deleteByPrefix(String prefix) {
        store.asMap().keySet().removeIf(k -> k.startsWith(prefix));
    }

    @Override
    public void publishInvalidation(String key) {
        // In-process L2 does not need cross-node publishing.
        // Cross-node coherence is handled by JdbcInvalidationPoller.
    }
}
