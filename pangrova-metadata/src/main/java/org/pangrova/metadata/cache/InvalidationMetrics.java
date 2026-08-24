package org.pangrova.metadata.cache;

import org.pangrova.spi.observability.DoubleHistogram;
import org.pangrova.spi.observability.LongCounter;
import org.pangrova.spi.observability.MetricsProvider;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Metrics collector for DDL invalidation propagation observability.
 *
 * <p>F-CHAIN-META-08: provides counters, gauges, and histograms so operators
 * can monitor whether DDL invalidations are being published, consumed by peers,
 * and how much lag exists in the propagation pipeline.</p>
 *
 * <p>Metric instruments:</p>
 * <ul>
 *   <li>{@code pangrova.metadata.invalidation.published} — counter of DDL
 *       invalidation entries written to the invalidation_log by this coordinator</li>
 *   <li>{@code pangrova.metadata.invalidation.consumed} — counter of DDL
 *       invalidation entries consumed (applied) by this coordinator's poller</li>
 *   <li>{@code pangrova.metadata.invalidation.lag} — gauge reporting how many
 *       unconsumed invalidation entries exist (difference between max log ID
 *       and last processed ID)</li>
 *   <li>{@code pangrova.metadata.invalidation.latency_ms} — histogram of time
 *       in milliseconds between an entry's creation and its consumption</li>
 * </ul>
 *
 * <p>Registered once at coordinator startup. {@link JdbcInvalidationPoller}
 * and {@link org.pangrova.metadata.service.DefaultMetadataService} call
 * recording methods after operations.</p>
 */
public final class InvalidationMetrics {

    /** Histogram bucket boundaries (seconds) for publish-to-consume latency. */
    private static final List<Double> LATENCY_HISTOGRAM_BUCKETS_SECONDS =
            List.of(0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0);

    private final LongCounter publishedCounter;
    private final LongCounter consumedCounter;
    private final DoubleHistogram durationHistogram;

    // Lag is backed by an atomic supplier; the gauge reads it at collection time.
    private final AtomicLong lagValue = new AtomicLong(0);

    /**
     * Registers all invalidation-propagation metric instruments on the given provider.
     */
    public InvalidationMetrics(MetricsProvider metricsProvider) {
        this.publishedCounter = metricsProvider.createCounter(
                "pangrova.metadata.invalidation.published",
                "DDL invalidation entries published to invalidation_log",
                "{entry}");

        this.consumedCounter = metricsProvider.createCounter(
                "pangrova.metadata.invalidation.consumed",
                "DDL invalidation entries consumed by this coordinator",
                "{entry}");

        metricsProvider.createGauge(
                "pangrova.metadata.invalidation.lag",
                "Number of unconsumed invalidation entries",
                "{entry}",
                () -> (double) lagValue.get());

        this.durationHistogram = metricsProvider.createHistogram(
                "pangrova.metadata.invalidation.duration",
                "Time from invalidation publish to consumption",
                "s",
                LATENCY_HISTOGRAM_BUCKETS_SECONDS);
    }

    /**
     * Records that an invalidation entry was published (written to the log).
     */
    public void recordPublished() {
        publishedCounter.add(1);
    }

    /**
     * Records that an invalidation entry was consumed (applied by the poller).
     */
    public void recordConsumed() {
        consumedCounter.add(1);
    }

    /**
     * Records the propagation latency for a single consumed entry.
     *
     * @param latencyMs milliseconds between entry creation and consumption (converted to seconds internally)
     */
    public void recordLatency(double latencyMs) {
        durationHistogram.record(latencyMs / 1000.0);
    }

    /**
     * Updates the lag gauge value. Called after each poll cycle.
     *
     * @param lag the current number of unconsumed entries
     */
    public void updateLag(long lag) {
        lagValue.set(Math.max(0, lag));
    }

    /**
     * Returns the current lag value. Exposed for testing.
     */
    public long currentLag() {
        return lagValue.get();
    }
}
