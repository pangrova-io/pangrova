package org.pangrova.indexing.core.config;

/**
 * Configuration for the index data cache.
 *
 * <p>Default size is the smaller of 256 MB or 12.5% of the JVM max heap,
 * preventing the cache from dominating memory in small-heap environments.</p>
 *
 * @param cacheSizeBytes maximum cache size in bytes
 */
public record CacheConfig(long cacheSizeBytes) {

    /** Hard upper bound: 256 MB regardless of heap size. */
    public static final long MAX_CACHE_SIZE = 256L * 1024 * 1024;

    /** Cache never exceeds this fraction of max heap. */
    private static final double HEAP_FRACTION = 0.125; // 12.5%

    public static final long DEFAULT_CACHE_SIZE = computeDefaultCacheSize();

    public static final CacheConfig DEFAULT = new CacheConfig(DEFAULT_CACHE_SIZE);

    public CacheConfig {
        if (cacheSizeBytes < 0) {
            throw new IllegalArgumentException("cacheSizeBytes must be >= 0, got " + cacheSizeBytes);
        }
    }

    private static long computeDefaultCacheSize() {
        long maxHeap = Runtime.getRuntime().maxMemory();
        long heapBased = (long) (maxHeap * HEAP_FRACTION);
        return Math.min(MAX_CACHE_SIZE, heapBased);
    }
}
