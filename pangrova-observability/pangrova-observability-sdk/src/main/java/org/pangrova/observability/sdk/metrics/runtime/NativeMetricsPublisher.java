package org.pangrova.observability.sdk.metrics.runtime;

import org.pangrova.common.concurrency.ThreadSafe;
import org.pangrova.observability.metrics.MetricsScope;
import org.pangrova.observability.api.metrics.MetricsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Bridges NativeMetricsPuller to MetricsProvider by registering observable gauges
 * and periodically pulling native stats into volatile fields read at collection time.
 *
 * <p>Concurrency strategy: all mutable state uses volatile fields for visibility.
 * The stopped flag ensures safe shutdown. Gauge suppliers read volatile fields
 * without synchronization.</p>
 */
@ThreadSafe
public final class NativeMetricsPublisher {

    private static final Logger LOG = LoggerFactory.getLogger(NativeMetricsPublisher.class);

    private final NativeMetricsPuller puller;
    private final MetricsScope metrics;

    private volatile boolean stopped;
    private volatile long memoryUsedBytes;
    private volatile long operatorRowsProcessed;
    private volatile long ioBytesRead;
    private volatile double cacheHitRatio;

    /** Registers owned gauges using the borrowed puller and metrics provider. */
    public NativeMetricsPublisher(NativeMetricsPuller puller, MetricsProvider metricsProvider) {
        this.puller = puller;
        this.metrics = new MetricsScope(metricsProvider);
        try {
            registerGauges(metrics);
        } catch (RuntimeException | Error failure) {
            try {
                metrics.close();
            } catch (RuntimeException | Error cleanup) {
                if (cleanup != failure) failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    private void registerGauges(MetricsProvider metricsProvider) {
        metricsProvider.createGauge(
                "pangrova.native.memory_used_bytes",
                "Native engine memory usage in bytes", "bytes",
                () -> (double) memoryUsedBytes);
        metricsProvider.createGauge(
                "pangrova.native.operator_rows_processed",
                "Total rows processed by native operators", "rows",
                () -> (double) operatorRowsProcessed);
        metricsProvider.createGauge(
                "pangrova.native.io_bytes_read",
                "Total I/O bytes read by native engine", "bytes",
                () -> (double) ioBytesRead);
        metricsProvider.createGauge(
                "pangrova.native.cache_hit_ratio",
                "Native engine cache hit ratio", "ratio",
                () -> cacheHitRatio);
    }

    /**
     * Pulls native stats and updates gauge values.
     * Safe to call from a scheduled executor — never throws.
     */
    public void pullAndPublish() {
        if (stopped) {
            return;
        }
        try {
            byte[] stats = puller.pull();
            updateFromStats(stats);
        } catch (Exception e) {
            LOG.warn("Failed to pull and publish native metrics: {}", e.getMessage());
        }
    }

    /**
     * Stops the publisher, unregistering its gauges and resetting local values.
     * After stop(), pullAndPublish() becomes a no-op.
     * Safe to call multiple times.
     */
    public void stop() {
        stopped = true;
        metrics.close();
        resetValues();
        LOG.info("NativeMetricsPublisher stopped");
    }


    private void updateFromStats(byte[] stats) {
        if (stats == null || stats.length == 0) {
            resetValues();
            return;
        }
        NativeMetricsPayload payload = NativeMetricsPayload.decode(stats);
        memoryUsedBytes = payload.getLong("memoryUsedBytes");
        operatorRowsProcessed = payload.getLong("operatorRowsProcessed");
        ioBytesRead = payload.getLong("ioBytesRead");
        cacheHitRatio = payload.getDouble("cacheHitRatio");

        if (LOG.isDebugEnabled()) {
            LOG.debug("event=native_metrics_decoded schemaVersion={} memoryUsedBytes={} "
                            + "operatorRowsProcessed={} ioBytesRead={} cacheHitRatio={}",
                    payload.schemaVersion(), memoryUsedBytes,
                    operatorRowsProcessed, ioBytesRead, cacheHitRatio);
        }
    }

    private void resetValues() {
        memoryUsedBytes = 0;
        operatorRowsProcessed = 0;
        ioBytesRead = 0;
        cacheHitRatio = 0;
    }

    @org.pangrova.common.VisibleForTesting
    public long getMemoryUsedBytes() { return memoryUsedBytes; }
    @org.pangrova.common.VisibleForTesting
    public long getOperatorRowsProcessed() { return operatorRowsProcessed; }
    @org.pangrova.common.VisibleForTesting
    public long getIoBytesRead() { return ioBytesRead; }
    @org.pangrova.common.VisibleForTesting
    public double getCacheHitRatio() { return cacheHitRatio; }
}
