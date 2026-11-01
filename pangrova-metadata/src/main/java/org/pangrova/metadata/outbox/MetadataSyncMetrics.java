package org.pangrova.metadata.outbox;

import org.pangrova.spi.observability.DoubleHistogram;
import org.pangrova.spi.observability.LongCounter;
import org.pangrova.spi.observability.MetricsProvider;

import java.util.List;
import java.util.function.Supplier;

/**
 * Metrics collector for metadata synchronization, cache freshness,
 * invalidation lag, and schema drift detection.
 *
 * <p>Registered once at coordinator startup. {@link MetadataSyncService}
 * and related components call recording methods after operations.</p>
 */
public final class MetadataSyncMetrics {

    /** Histogram bucket boundaries (milliseconds) for sync duration. */
    private static final List<Double> SYNC_DURATION_HISTOGRAM_BUCKETS_MS =
            List.of(10.0, 50.0, 100.0, 500.0, 1000.0, 5000.0, 30000.0);

    private final DoubleHistogram syncDuration;
    private final LongCounter changesApplied;
    private final LongCounter syncErrors;
    private final LongCounter schemaDriftDetected;

    // Staleness and lag are tracked via gauges backed by suppliers
    // that read the current value at collection time.

    /**
     * Registers all metadata-sync metric instruments on the given provider.
     */
    public MetadataSyncMetrics(MetricsProvider metricsProvider,
                                Supplier<Double> cacheStalenessFn,
                                Supplier<Double> invalidationLagFn) {
        this.syncDuration = metricsProvider.createHistogram(
                "metadata.sync.duration_ms",
                "Metadata sync duration in milliseconds", "ms",
                SYNC_DURATION_HISTOGRAM_BUCKETS_MS);

        this.changesApplied = metricsProvider.createCounter(
                "metadata.sync.changes_applied",
                "Total metadata changes applied during sync", "changes");

        this.syncErrors = metricsProvider.createCounter(
                "metadata.sync.errors",
                "Total metadata sync errors", "errors");

        this.schemaDriftDetected = metricsProvider.createCounter(
                "metadata.schema_drift_detected",
                "Schema drift events detected during post-sync validation", "events");

        metricsProvider.createGauge(
                "metadata.cache.staleness_seconds",
                "Seconds since last successful metadata sync", "s",
                cacheStalenessFn);

        metricsProvider.createGauge(
                "metadata.invalidation.lag_seconds",
                "Seconds since oldest unprocessed invalidation entry", "s",
                invalidationLagFn);
    }

    public void recordSyncDuration(long durationMs) {
        syncDuration.record(durationMs);
    }

    public void recordChangesApplied(int count) {
        if (count > 0) {
            changesApplied.add(count);
        }
    }

    public void recordSyncError() {
        syncErrors.add(1);
    }

    public void recordSchemaDriftDetected() {
        schemaDriftDetected.add(1);
    }
}
