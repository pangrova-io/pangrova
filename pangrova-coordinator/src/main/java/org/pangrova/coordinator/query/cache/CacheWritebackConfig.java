package org.pangrova.coordinator.query.cache;

/**
 * Immutable configuration for query result cache writeback.
 *
 * @param enabled whether cache writeback is active (default: false)
 */
public record CacheWritebackConfig(boolean enabled) {

    /** Default configuration with writeback disabled. */
    public static final CacheWritebackConfig DISABLED = new CacheWritebackConfig(false);
}
