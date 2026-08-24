package org.pangrova.metadata.cache;

/**
 * Statistics for a metadata cache instance.
 */
public record CacheStats(long hitCount, long missCount, long evictionCount, long size) {
}
