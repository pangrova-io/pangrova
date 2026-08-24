package org.pangrova.worker.heartbeat;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Enhanced health check snapshot including memory pressure, active task count,
 * disk space, and native process liveness — beyond simple HTTP 200.
 *
 * <p>Returned by the enhanced health check endpoint to provide comprehensive
 * worker health information for load balancing and failure detection.</p>
 *
 * @param memoryPressureLevel memory pressure classification (NONE, LOW, MEDIUM, HIGH, CRITICAL)
 * @param activeTaskCount     number of currently running tasks
 * @param availableDiskBytes  available disk space in bytes for spill and cache
 * @param nativeProcessAlive  whether the native execution process is responsive
 * @param heapUsedBytes       JVM heap memory currently in use
 * @param heapMaxBytes        JVM maximum heap memory
 * @param directMemoryUsedBytes off-heap direct memory in use
 * @param uptimeSeconds       worker uptime in seconds
 */
public record WorkerHealthSnapshot(
        @JsonProperty("memory_pressure_level") String memoryPressureLevel,
        @JsonProperty("active_task_count") int activeTaskCount,
        @JsonProperty("available_disk_bytes") long availableDiskBytes,
        @JsonProperty("native_process_alive") boolean nativeProcessAlive,
        @JsonProperty("heap_used_bytes") long heapUsedBytes,
        @JsonProperty("heap_max_bytes") long heapMaxBytes,
        @JsonProperty("direct_memory_used_bytes") long directMemoryUsedBytes,
        @JsonProperty("uptime_seconds") long uptimeSeconds) {

    public WorkerHealthSnapshot {
        if (memoryPressureLevel == null || memoryPressureLevel.isBlank()) {
            memoryPressureLevel = "NONE";
        }
        if (activeTaskCount < 0) {
            throw new IllegalArgumentException("activeTaskCount must be non-negative: " + activeTaskCount);
        }
    }

    /**
     * Returns true if the worker is healthy enough to accept new tasks.
     */
    public boolean isHealthy() {
        return nativeProcessAlive
                && !"CRITICAL".equals(memoryPressureLevel)
                && availableDiskBytes > 0;
    }
}
