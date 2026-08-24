package org.pangrova.spi.cache;

import org.pangrova.spi.annotation.Experimental;

/**
 * Approximate statistics for a {@link DistributedCacheStore}, used for
 * monitoring dashboards and diagnostics.
 *
 * <p>All counters are best-effort -- implementations may report -1 for
 * any metric that is not available or too expensive to compute.</p>
 *
 * <p>G-CACHE-08: distributed/detached cache layer SPI.</p>
 *
 * @param hitCount        number of successful {@code get} calls
 * @param missCount       number of {@code get} calls that returned empty
 * @param putCount        number of {@code put} calls
 * @param evictionCount   number of entries evicted (by TTL, LRU, or explicit invalidation)
 * @param estimatedSizeBytes approximate total size of cached data in bytes (-1 if unknown)
 * @param estimatedEntryCount approximate number of entries (-1 if unknown)
 */
@Experimental(reason = "G-CACHE-08: distributed cache store statistics")
public record CacheStoreStats(
        long hitCount,
        long missCount,
        long putCount,
        long evictionCount,
        long estimatedSizeBytes,
        long estimatedEntryCount
) {

    /** Sentinel for implementations that do not track statistics. */
    public static final CacheStoreStats UNKNOWN =
            new CacheStoreStats(-1, -1, -1, -1, -1, -1);

    /** Returns the hit rate as a ratio in [0.0, 1.0], or -1.0 if unknown. */
    public double hitRate() {
        if (hitCount < 0 || missCount < 0) {
            return -1.0;
        }
        long total = hitCount + missCount;
        return total == 0 ? 0.0 : (double) hitCount / total;
    }
}
