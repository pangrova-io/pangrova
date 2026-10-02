package org.pangrova.spi.coordination;

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
        String capabilityInventory
) {

    public WorkerSnapshot {
        Objects.requireNonNull(workerId, "workerId must not be null");
        Objects.requireNonNull(host, "host must not be null");
        Objects.requireNonNull(memoryPressure, "memoryPressure must not be null");
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
}
