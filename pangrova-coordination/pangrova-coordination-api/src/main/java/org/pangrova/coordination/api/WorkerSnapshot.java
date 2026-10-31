package org.pangrova.coordination.api;

import java.util.Base64;
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
        String rackId,
        List<String> dataLocality,
        String memoryPressure,
        double cpuUtilization,
        int runningTaskCount,
        long availableMemoryBytes,
        long lastHeartbeatMs,
        long registeredAtMs,
        int resultServerPort,
        CacheCatalogSummary cacheSummary,
        String capabilityInventory,
        int copyTaskTlsPort,
        String admissionDomainId,
        String incarnationId
) {
    private static final int MAX_TCP_PORT = 65535;

    /** Creates a snapshot for a worker without a dedicated COPY task TLS listener. */
    public WorkerSnapshot(String workerId, String host, int httpPort, int transportPort,
                          int protocolVersion, String rackId, List<String> dataLocality,
                          String memoryPressure, double cpuUtilization, int runningTaskCount,
                          long availableMemoryBytes, long lastHeartbeatMs, long registeredAtMs,
                          int resultServerPort, CacheCatalogSummary cacheSummary,
                          String capabilityInventory, String admissionDomainId, String incarnationId) {
        this(workerId, host, httpPort, transportPort, protocolVersion, rackId, dataLocality,
                memoryPressure, cpuUtilization, runningTaskCount, availableMemoryBytes,
                lastHeartbeatMs, registeredAtMs, resultServerPort, cacheSummary,
                capabilityInventory, 0, admissionDomainId, incarnationId);
    }

    public WorkerSnapshot {
        requireIdentity(admissionDomainId);
        requireIdentity(incarnationId);
        Objects.requireNonNull(workerId, "workerId must not be null");
        Objects.requireNonNull(host, "host must not be null");
        Objects.requireNonNull(memoryPressure, "memoryPressure must not be null");
        if (copyTaskTlsPort < 0 || copyTaskTlsPort > MAX_TCP_PORT) {
            throw new IllegalArgumentException("copyTaskTlsPort must be a valid optional TCP port");
        }
        if (protocolVersion != 2) {
            throw new IllegalArgumentException("protocolVersion must equal canonical v2");
        }
        dataLocality = List.copyOf(dataLocality);
        if (capabilityInventory == null || capabilityInventory.isBlank()) {
            throw new IllegalArgumentException("capabilityInventory must not be blank");
        }
        byte[] decodedInventory;
        try {
            decodedInventory = Base64.getDecoder().decode(capabilityInventory);
        } catch (IllegalArgumentException failure) {
            throw new IllegalArgumentException(
                    "capabilityInventory must be valid Base64", failure);
        }
        if (decodedInventory.length == 0) {
            throw new IllegalArgumentException(
                    "capabilityInventory must encode a non-empty inventory");
        }
    }

    private static void requireIdentity(String value) {
        if (value == null || !java.util.UUID.fromString(value).toString().equals(value)) {
            throw new IllegalArgumentException("Worker execution identity requires canonical UUIDs");
        }
    }
}
