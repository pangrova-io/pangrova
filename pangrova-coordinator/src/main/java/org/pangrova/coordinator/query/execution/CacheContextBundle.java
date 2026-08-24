package org.pangrova.coordinator.query.execution;

import org.pangrova.coordinator.query.cache.CacheWritebackConfig;

/**
 * Groups cache-related dependencies for QueryDispatcher.
 * All fields are nullable — null means caching is disabled.
 */
public record CacheContextBundle(
        CacheWritebackConfig cacheWritebackConfig) {

    /** Convenience factory for when caching is disabled. */
    public static CacheContextBundle none() {
        return new CacheContextBundle(null);
    }
}
