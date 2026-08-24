package org.pangrova.planner.cache;

/**
 * Configuration for the optimized-plan cache.
 *
 * @param maxEntries maximum number of entries retained by the LRU; oldest are evicted
 *                   when size exceeds this bound.
 * @param enabled    when {@code false}, the cache is bypassed (every plan call
 *                   re-optimizes). Useful for benchmarking or troubleshooting.
 */
public record PlanCacheConfig(int maxEntries, boolean enabled) {

    /** Default: 1000 entries, enabled. */
    public static final PlanCacheConfig DEFAULT = new PlanCacheConfig(1000, true);

    /** Disabled: cache bypassed entirely. */
    public static final PlanCacheConfig DISABLED = new PlanCacheConfig(0, false);

    public PlanCacheConfig {
        if (maxEntries < 0) {
            throw new IllegalArgumentException("maxEntries must be >= 0, got " + maxEntries);
        }
    }
}
