package org.pangrova.observability.sdk.metrics.nativebridge;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.observability.api.MetricsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

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

    private volatile boolean stopped;
    private volatile long memoryUsedBytes;
    private volatile long operatorRowsProcessed;
    private volatile long ioBytesRead;
    private volatile double cacheHitRatio;

    public NativeMetricsPublisher(NativeMetricsPuller puller, MetricsProvider metricsProvider) {
        this.puller = puller;
        registerGauges(metricsProvider);
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
     * Stops the publisher, resetting all gauge values to zero.
     * After stop(), pullAndPublish() becomes a no-op.
     * Safe to call multiple times.
     */
    public void stop() {
        stopped = true;
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

    /**
     * Per-operator execution statistics for metrics publishing.
     * Decoupled from protocol DTOs to respect module boundary constraints.
     */
    public record OperatorMetricsSnapshot(
        String operatorType,
        long inputRows,
        long outputRows,
        long inputBytes,
        long outputBytes,
        long cpuNanos,
        long wallNanos,
        String planNodeId,
        long memoryBytes,
        long peakMemoryBytes,
        long spillBytes
    ) {
        /**
         * Backward-compatible constructor without memory/spill fields.
         */
        public OperatorMetricsSnapshot(String operatorType, long inputRows, long outputRows,
                                       long inputBytes, long outputBytes, long cpuNanos, long wallNanos) {
            this(operatorType, inputRows, outputRows, inputBytes, outputBytes,
                 cpuNanos, wallNanos, "", 0, 0, 0);
        }
    }

    /**
     * Publishes per-operator metrics from the given operator stats list.
     * Each operator's input/output rows and bytes are emitted as structured log entries.
     *
     * @param operatorStatsList the per-operator stats collected from native execution
     */
    public void publishOperatorStats(List<OperatorMetricsSnapshot> operatorStatsList) {
        if (stopped || operatorStatsList == null) {
            return;
        }
        for (OperatorMetricsSnapshot stats : operatorStatsList) {
            LOG.info("operator_stats operator_type={} plan_node_id={} input_rows={} output_rows={} "
                            + "input_bytes={} output_bytes={} cpu_nanos={} wall_nanos={} "
                            + "memory_bytes={} peak_memory_bytes={} spill_bytes={}",
                    stats.operatorType(), stats.planNodeId(),
                    stats.inputRows(), stats.outputRows(),
                    stats.inputBytes(), stats.outputBytes(),
                    stats.cpuNanos(), stats.wallNanos(),
                    stats.memoryBytes(), stats.peakMemoryBytes(), stats.spillBytes());
        }
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
