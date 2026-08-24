package org.pangrova.spi.coordination;

import java.util.Objects;

/**
 * Immutable snapshot of a worker's heartbeat payload.
 *
 * <p>Carries the dynamic runtime metrics that a worker reports on each
 * heartbeat cycle. Static identity fields (host, ports, capabilities) are
 * not included — they are captured once at registration time in
 * {@link WorkerSnapshot}.
 *
 * <p>{@code memoryPressure} is represented as a {@link String} rather than
 * an enum because the SPI layer must not depend on {@code pangrova-resource}
 * internal types.
 *
 * @param memoryPressure      current memory pressure level (e.g. "NONE", "LOW", "HIGH") (required)
 * @param cpuUtilization      CPU utilization ratio (0.0–1.0)
 * @param runningTaskCount    number of tasks currently executing
 * @param availableMemoryBytes free memory in bytes
 * @param cacheSummary        worker data cache summary (nullable when cache disabled)
 */
public record HeartbeatSnapshot(
        String memoryPressure,
        double cpuUtilization,
        int runningTaskCount,
        long availableMemoryBytes,
        CacheCatalogSummary cacheSummary
) {

    public HeartbeatSnapshot {
        Objects.requireNonNull(memoryPressure, "memoryPressure must not be null");
    }

    /** Backward-compatible constructor without cache summary. */
    public HeartbeatSnapshot(String memoryPressure, double cpuUtilization,
                             int runningTaskCount, long availableMemoryBytes) {
        this(memoryPressure, cpuUtilization, runningTaskCount, availableMemoryBytes, null);
    }
}
