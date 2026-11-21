package org.pangrova.observability.sdk.metrics.jvm;

/**
 * Exposes Arrow memory allocation metrics for observability.
 * Immutable snapshot of allocator state at capture time.
 *
 * <p>This is a plain record with no Arrow dependency — the actual
 * allocator lives in the worker memory subsystem. Consumers create snapshots
 * by reading allocator state and constructing this record.</p>
 */
public record OffHeapMemoryMetrics(
    long usedBytes,
    long peakBytes,
    long limit
) {

    /**
     * Returns memory utilization as a percentage (0.0 to 100.0).
     * Returns 0.0 if limit is zero or negative.
     */
    public double utilizationPercent() {
        return limit > 0 ? (usedBytes * 100.0) / limit : 0.0;
    }
}
