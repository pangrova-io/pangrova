package org.pangrova.coordination.api.heartbeat;

import org.pangrova.common.api.annotation.Experimental;

import java.util.Objects;

/**
 * Immutable snapshot of a worker's heartbeat payload.
 *
 * <p>Carries the dynamic runtime metrics that a worker reports on each
 * heartbeat cycle. Static identity fields (host, ports, capability inventory) are
 * not included — they are captured once at registration time in
 * the membership registration snapshot.
 *
 * <p>{@code memoryPressure} is represented as a {@link String} rather than
 * an enum because the coordination contract must not depend on worker runtime types
 * internal types.
 *
 * @param memoryPressure      current memory pressure level (e.g. "NONE", "LOW", "HIGH") (required)
 * @param cpuUtilization      CPU utilization ratio (0.0–1.0)
 * @param inFlightTaskCount    number of attempts holding preparation, execution or cleanup
 * @param availableMemoryBytes free memory in bytes
 * @param cacheSummary        worker data cache summary (nullable when cache disabled)
 */
@Experimental
public record HeartbeatSnapshot(
        String memoryPressure,
        double cpuUtilization,
        int inFlightTaskCount,
        long availableMemoryBytes,
        CacheCatalogSummary cacheSummary
) {

    public HeartbeatSnapshot {
        Objects.requireNonNull(memoryPressure, "memoryPressure must not be null");
    }

}
