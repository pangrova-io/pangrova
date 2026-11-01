package org.pangrova.observability.sdk.metrics;

import org.pangrova.spi.observability.MetricsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Publishes cache metrics (hit rate, miss rate, eviction count, RAM/SSD usage)
 * to the observability subsystem via {@link MetricsProvider} gauges.
 *
 * <p>Thread-safe: all mutable fields are volatile, updated atomically via
 * {@link #publish(CacheStatsSnapshot)}.
 *
 * @see CacheStatsSnapshot
 */
public final class CacheMetricsPublisher {

    private static final Logger LOG = LoggerFactory.getLogger(CacheMetricsPublisher.class);

    private volatile double ramHitRate;
    private volatile double ssdHitRate;
    private volatile double overallHitRate;
    private volatile double evictions;
    private volatile double ramUsedBytes;
    private volatile double ssdUsedBytes;

    public CacheMetricsPublisher(MetricsProvider metricsProvider) {
        registerGauges(metricsProvider);
    }

    private void registerGauges(MetricsProvider metricsProvider) {
        metricsProvider.createGauge(
                "pangrova.cache.ram_hit_rate",
                "RAM cache hit rate", "ratio",
                () -> ramHitRate);
        metricsProvider.createGauge(
                "pangrova.cache.ssd_hit_rate",
                "SSD cache hit rate", "ratio",
                () -> ssdHitRate);
        metricsProvider.createGauge(
                "pangrova.cache.overall_hit_rate",
                "Overall cache hit rate (RAM + SSD)", "ratio",
                () -> overallHitRate);
        metricsProvider.createGauge(
                "pangrova.cache.evictions",
                "Total cache eviction count", "count",
                () -> evictions);
        metricsProvider.createGauge(
                "pangrova.cache.ram_used_bytes",
                "RAM cache used bytes", "bytes",
                () -> ramUsedBytes);
        metricsProvider.createGauge(
                "pangrova.cache.ssd_used_bytes",
                "SSD cache used bytes", "bytes",
                () -> ssdUsedBytes);
    }

    /**
     * Updates all gauge values from the given snapshot.
     *
     * @param snapshot immutable cache stats; must not be null
     */
    public void publish(CacheStatsSnapshot snapshot) {
        ramHitRate = snapshot.ramHitRate();
        ssdHitRate = snapshot.ssdHitRate();
        overallHitRate = snapshot.overallHitRate();
        evictions = snapshot.evictions();
        ramUsedBytes = snapshot.ramUsedBytes();
        ssdUsedBytes = snapshot.ssdUsedBytes();
    }

    /**
     * Checks overall hit rate against threshold and logs WARN if below.
     *
     * @param threshold minimum acceptable overall hit rate (0.0 to 1.0)
     */
    public void checkHitRateThreshold(double threshold) {
        double current = overallHitRate;
        if (current < threshold) {
            LOG.warn("cache_hit_rate_low overall_hit_rate={} threshold={}",
                    current, threshold);
        }
    }

    @org.pangrova.common.VisibleForTesting
    public double getRamHitRate() { return ramHitRate; }
    @org.pangrova.common.VisibleForTesting
    public double getSsdHitRate() { return ssdHitRate; }
    @org.pangrova.common.VisibleForTesting
    public double getOverallHitRate() { return overallHitRate; }
    @org.pangrova.common.VisibleForTesting
    public double getEvictions() { return evictions; }
    @org.pangrova.common.VisibleForTesting
    public double getRamUsedBytes() { return ramUsedBytes; }
    @org.pangrova.common.VisibleForTesting
    public double getSsdUsedBytes() { return ssdUsedBytes; }
}
