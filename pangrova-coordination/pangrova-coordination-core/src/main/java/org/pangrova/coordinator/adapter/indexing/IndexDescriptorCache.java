package org.pangrova.coordinator.adapter.indexing;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.indexing.store.IndexMetadataStore;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TTL-based cache for {@link IndexDescriptor} lookups, reducing repeated
 * metadata store queries on the coordinator's query-critical path.
 *
 * <p>Cache entries expire after a configurable TTL. Stale entries are
 * lazily evicted on access. The cache is safe for concurrent use.</p>
 */
@ThreadSafe
public final class IndexDescriptorCache {

    private static final Duration DEFAULT_TTL = Duration.ofSeconds(30);

    private final IndexMetadataStore delegate;
    private final long ttlNanos;
    private final ConcurrentHashMap<String, CachedEntry> cache = new ConcurrentHashMap<>();

    public IndexDescriptorCache(IndexMetadataStore delegate) {
        this(delegate, DEFAULT_TTL);
    }

    public IndexDescriptorCache(IndexMetadataStore delegate, Duration ttl) {
        this.delegate = delegate;
        this.ttlNanos = ttl.toNanos();
    }

    /**
     * Batch-fetches descriptors by IDs, serving from cache when possible.
     * Cache misses are resolved via the delegate store in a single batch call.
     */
    public Map<String, IndexDescriptor> findByIds(List<String> indexIds) {
        long now = System.nanoTime();
        Map<String, IndexDescriptor> result = new HashMap<>(indexIds.size());
        List<String> misses = new java.util.ArrayList<>();

        for (String id : indexIds) {
            CachedEntry entry = cache.get(id);
            if (entry != null && (now - entry.insertedAt) < ttlNanos) {
                result.put(id, entry.descriptor);
            } else {
                misses.add(id);
            }
        }

        if (!misses.isEmpty()) {
            Map<String, IndexDescriptor> fetched = delegate.findIndexesByIds(misses);
            for (var e : fetched.entrySet()) {
                cache.put(e.getKey(), new CachedEntry(e.getValue(), now));
                result.put(e.getKey(), e.getValue());
            }
        }

        return Map.copyOf(result);
    }

    /** Invalidates a single entry (e.g., after state change). */
    public void invalidate(String indexId) {
        cache.remove(indexId);
    }

    /** Clears all cached entries. */
    public void invalidateAll() {
        cache.clear();
    }

    /** Returns current cache size (for metrics/testing). */
    public int size() {
        return cache.size();
    }

    private record CachedEntry(IndexDescriptor descriptor, long insertedAt) {}
}
