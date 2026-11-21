package org.pangrova.observability.sdk.metrics;

/**
 * Immutable snapshot of cache statistics from the native CacheManager.
 * Decoupled from protocol DTOs to respect module boundary constraints.
 *
 * <p>All rate calculations are safe against division by zero.
 */
public record CacheStatsSnapshot(
    long ramHits,
    long ramMisses,
    long ssdHits,
    long ssdMisses,
    long promotions,
    long demotions,
    long evictions,
    long ramUsedBytes,
    long ssdUsedBytes,
    double prefetchHitRate
) {

    public double ramHitRate() {
        long total = ramHits + ramMisses;
        return total > 0 ? (double) ramHits / total : 0.0;
    }

    public double ssdHitRate() {
        long total = ssdHits + ssdMisses;
        return total > 0 ? (double) ssdHits / total : 0.0;
    }

    /** Combined RAM and SSD hit ratio; zero when no accesses were recorded. */
    public double overallHitRate() {
        long totalHits = ramHits + ssdHits;
        long totalMisses = ramMisses + ssdMisses;
        long total = totalHits + totalMisses;
        return total > 0 ? (double) totalHits / total : 0.0;
    }
}
