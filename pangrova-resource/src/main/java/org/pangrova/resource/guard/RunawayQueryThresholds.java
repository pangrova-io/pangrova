package org.pangrova.resource.guard;

/**
 * Per-query runaway thresholds. A query that exceeds any positive threshold is a runaway and
 * should be auto-cancelled (StarRocks "big query" model). A non-positive value ({@code <= 0})
 * disables that dimension, so a fully-zero instance enforces nothing.
 *
 * @param maxCpuTimeMs    maximum cumulative CPU time per query in milliseconds (≤0 = unlimited)
 * @param maxScannedBytes maximum bytes scanned per query (≤0 = unlimited)
 * @param maxScannedRows  maximum rows scanned per query (≤0 = unlimited)
 * @param maxMemoryBytes  maximum memory reserved per query in bytes (≤0 = unlimited)
 */
public record RunawayQueryThresholds(
        long maxCpuTimeMs,
        long maxScannedBytes,
        long maxScannedRows,
        long maxMemoryBytes) {

    /** Thresholds that enforce nothing. */
    public static final RunawayQueryThresholds UNLIMITED = new RunawayQueryThresholds(0, 0, 0, 0);

    public boolean enforcesCpuTime() {
        return maxCpuTimeMs > 0;
    }

    public boolean enforcesScannedBytes() {
        return maxScannedBytes > 0;
    }

    public boolean enforcesScannedRows() {
        return maxScannedRows > 0;
    }

    public boolean enforcesMemory() {
        return maxMemoryBytes > 0;
    }

    /** @return true when at least one dimension is enforced. */
    public boolean enforcesAnything() {
        return enforcesCpuTime() || enforcesScannedBytes()
                || enforcesScannedRows() || enforcesMemory();
    }
}
