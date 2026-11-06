package org.pangrova.metadata.api.cache;

/**
 * Statistics for a metadata cache instance.
 */
public record CacheStats(long hitCount, long missCount, long evictionCount, long size) {
}
