package org.pangrova.spi.coordination;

import java.util.List;
import java.util.Objects;

/**
 * Immutable snapshot of a worker's registration state. F-CHAIN-CACHE-02:
 * cacheSummary carries recently-accessed file paths from the latest
 * heartbeat for cache-locality scheduling. Null when the worker has not
 * yet reported cache state.
 */
public record WorkerSnapshot(
        String workerId,
        String host,
        int httpPort,
        int transportPort,
        int protocolVersion,
        List<String> capabilities,
        String rackId,
        List<String> dataLocality,
        String memoryPressure,
        double cpuUtilization,
        int runningTaskCount,
        long availableMemoryBytes,
        long lastHeartbeatMs,
        long registeredAtMs,
        int resultServerPort,
        CacheCatalogSummary cacheSummary
) {

    public WorkerSnapshot(
            String workerId, String host, int httpPort, int transportPort,
            int protocolVersion, List<String> capabilities, String rackId,
            List<String> dataLocality, String memoryPressure, double cpuUtilization,
            int runningTaskCount, long availableMemoryBytes,
            long lastHeartbeatMs, long registeredAtMs) {
        this(workerId, host, httpPort, transportPort, protocolVersion, capabilities,
                rackId, dataLocality, memoryPressure, cpuUtilization, runningTaskCount,
                availableMemoryBytes, lastHeartbeatMs, registeredAtMs, 0, null);
    }

    public WorkerSnapshot(
            String workerId, String host, int httpPort, int transportPort,
            int protocolVersion, List<String> capabilities, String rackId,
            List<String> dataLocality, String memoryPressure, double cpuUtilization,
            int runningTaskCount, long availableMemoryBytes,
            long lastHeartbeatMs, long registeredAtMs, int resultServerPort) {
        this(workerId, host, httpPort, transportPort, protocolVersion, capabilities,
                rackId, dataLocality, memoryPressure, cpuUtilization, runningTaskCount,
                availableMemoryBytes, lastHeartbeatMs, registeredAtMs, resultServerPort, null);
    }

    public WorkerSnapshot {
        Objects.requireNonNull(workerId, "workerId must not be null");
        Objects.requireNonNull(host, "host must not be null");
        capabilities = List.copyOf(capabilities);
        dataLocality = List.copyOf(dataLocality);
    }
}
