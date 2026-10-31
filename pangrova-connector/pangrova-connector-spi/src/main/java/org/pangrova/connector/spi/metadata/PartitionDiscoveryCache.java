package org.pangrova.connector.spi.metadata;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.pangrova.common.api.annotation.Experimental;

/**
 * TTL-based cache for partition metadata discovery results.
 *
 * <p>Avoids repeated listing of storage directories on every query by caching
 * partition values with configurable time-to-live. Thread-safe for concurrent
 * access from multiple query planning threads.</p>
 *
 * @param <T> the type of cached partition entries
 */
@Experimental
public final class PartitionDiscoveryCache<T> {

    private final long ttlMillis;
    private final int maxEntries;
    private final ConcurrentHashMap<String, CacheEntry<T>> cache;

    /**
     * Creates a partition discovery cache with the specified TTL and max entries.
     *
     * @param ttl        time-to-live for cache entries
     * @param maxEntries maximum number of cached tables (LRU eviction beyond this)
     */
    public PartitionDiscoveryCache(Duration ttl, int maxEntries) {
        if (ttl.isNegative() || ttl.isZero()) {
            throw new IllegalArgumentException("TTL must be positive, got: " + ttl);
        }
        if (maxEntries <= 0) {
            throw new IllegalArgumentException("maxEntries must be positive, got: " + maxEntries);
        }
        this.ttlMillis = ttl.toMillis();
        this.maxEntries = maxEntries;
        this.cache = new ConcurrentHashMap<>();
    }

    /**
     * Creates a cache with default settings: 5 minute TTL, 1000 max entries.
     */
    public static <T> PartitionDiscoveryCache<T> withDefaults() {
        return new PartitionDiscoveryCache<>(Duration.ofMinutes(5), 1000);
    }

    /**
     * Retrieves cached partition data for the given table key, if present and not expired.
     *
     * @param tableKey unique key identifying the table (e.g., "catalog.schema.table")
     * @return cached partition data, or empty if not cached or expired
     */
    public Optional<List<T>> get(String tableKey) {
        CacheEntry<T> entry = cache.get(tableKey);
        if (entry == null) {
            return Optional.empty();
        }
        if (isExpired(entry)) {
            cache.remove(tableKey, entry);
            return Optional.empty();
        }
        return Optional.of(entry.partitions());
    }

    /**
     * Stores partition data in the cache for the given table key.
     *
     * @param tableKey   unique key identifying the table
     * @param partitions the partition data to cache
     */
    public void put(String tableKey, List<T> partitions) {
        evictIfFull();
        cache.put(tableKey, new CacheEntry<>(List.copyOf(partitions), System.currentTimeMillis()));
    }

    /**
     * Invalidates the cache entry for the given table key.
     *
     * @param tableKey unique key identifying the table
     */
    public void invalidate(String tableKey) {
        cache.remove(tableKey);
    }

    /**
     * Invalidates all cache entries.
     */
    public void invalidateAll() {
        cache.clear();
    }

    /**
     * Returns the current number of entries in the cache (including potentially expired ones).
     */
    public int size() {
        return cache.size();
    }

    private boolean isExpired(CacheEntry<T> entry) {
        return (System.currentTimeMillis() - entry.createdAtMillis()) > ttlMillis;
    }

    private void evictIfFull() {
        if (cache.size() >= maxEntries) {
            // Evict oldest entry
            String oldestKey = null;
            long oldestTime = Long.MAX_VALUE;
            for (Map.Entry<String, CacheEntry<T>> e : cache.entrySet()) {
                if (e.getValue().createdAtMillis() < oldestTime) {
                    oldestTime = e.getValue().createdAtMillis();
                    oldestKey = e.getKey();
                }
            }
            if (oldestKey != null) {
                cache.remove(oldestKey);
            }
        }
    }

    private record CacheEntry<T>(List<T> partitions, long createdAtMillis) {}
}
