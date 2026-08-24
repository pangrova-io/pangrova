package org.pangrova.planner.cache;

/**
 * SQL hint controlling cache behavior.
 */
public enum CacheHint {
    /** Normal caching (default). */
    NONE,
    /** Bypass cache entirely. */
    NO_CACHE,
    /** Force execute and update cache. */
    REFRESH_CACHE
}
