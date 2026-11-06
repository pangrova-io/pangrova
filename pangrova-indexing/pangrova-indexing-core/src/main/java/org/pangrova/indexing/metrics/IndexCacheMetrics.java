package org.pangrova.indexing.metrics;

import org.pangrova.indexing.storage.IndexCache;
import org.pangrova.observability.spi.metrics.MetricsProvider;

/**
 * Registers observable gauges for index cache statistics.
 *
 * <p>Gauges are pull-based: the metrics backend invokes the supplier at
 * collection time, reading current values from the {@link IndexCache}.</p>
 */
public final class IndexCacheMetrics {

    /** Registers cache gauges on the given metrics provider. */
    public IndexCacheMetrics(MetricsProvider metricsProvider, IndexCache cache) {
        metricsProvider.createGauge(
                "pangrova.index.cache.hit_rate",
                "Index cache hit ratio (0.0–1.0)", "ratio",
                cache::hitRate);
        metricsProvider.createGauge(
                "pangrova.index.cache.size_bytes",
                "Current index cache size in bytes", "bytes",
                () -> (double) cache.currentSizeBytes());
        metricsProvider.createGauge(
                "pangrova.index.cache.eviction_count",
                "Total number of cache evictions", "count",
                () -> (double) cache.evictionCount());
    }
}
