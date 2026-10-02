package org.pangrova.coordinator.worker;

import java.time.Instant;
import java.util.List;

import org.pangrova.protocol.CanonicalTaskRequests;
import org.pangrova.protocol.CapabilityInventoryCodec;
import org.pangrova.protocol.v2.CapabilityInventory;
import org.pangrova.resource.MemoryPressure;
import org.pangrova.spi.coordination.CacheCatalogSummary;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Immutable snapshot of a Worker's current state and metrics.
 * F-CHAIN-CACHE-02: cacheSummary carries recently-accessed file paths.
 */
public record WorkerInfo(
        String workerId,
        String host,
        int httpPort,
        int transportPort,
        int resultServerPort,
        HealthState healthState,
        MemoryPressure memoryPressure,
        double cpuUtilization,
        int runningTaskCount,
        long availableMemoryBytes,
        Instant lastHeartbeat,
        double performanceScore,
        String resourcePoolId,
        int protocolVersion,
        String rackId,
        List<String> dataLocality,
        CacheCatalogSummary cacheSummary,
        CapabilityInventory capabilityInventory) {

    public WorkerInfo {
        checkNotNull(workerId, "workerId must not be null");
        checkNotNull(host, "host must not be null");
        checkNotNull(healthState, "healthState must not be null");
        checkNotNull(memoryPressure, "memoryPressure must not be null");
        checkNotNull(lastHeartbeat, "lastHeartbeat must not be null");
        if (protocolVersion != CanonicalTaskRequests.PROTOCOL_MAJOR) {
            throw new IllegalArgumentException(
                    "protocolVersion must equal canonical v2");
        }
        if (dataLocality == null) {
            dataLocality = List.of();
        } else {
            dataLocality = List.copyOf(dataLocality);
        }
        CapabilityInventoryCodec.validate(
                checkNotNull(capabilityInventory,
                        "capabilityInventory must not be null"));
    }

    public WorkerInfo withHealthState(HealthState newState) {
        return new WorkerInfo(workerId, host, httpPort, transportPort, resultServerPort,
                newState, memoryPressure, cpuUtilization, runningTaskCount,
                availableMemoryBytes, lastHeartbeat, performanceScore,
                resourcePoolId, protocolVersion,
                rackId, dataLocality, cacheSummary, capabilityInventory);
    }

    public WorkerInfo withCacheSummary(CacheCatalogSummary summary) {
        return new WorkerInfo(workerId, host, httpPort, transportPort, resultServerPort,
                healthState, memoryPressure, cpuUtilization, runningTaskCount,
                availableMemoryBytes, lastHeartbeat, performanceScore,
                resourcePoolId, protocolVersion,
                rackId, dataLocality, summary, capabilityInventory);
    }
}
