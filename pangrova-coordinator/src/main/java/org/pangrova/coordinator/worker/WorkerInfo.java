package org.pangrova.coordinator.worker;

import java.time.Instant;
import java.util.List;

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
        List<String> capabilities,
        String rackId,
        List<String> dataLocality,
        CacheCatalogSummary cacheSummary,
        List<VectorShardInfo> vectorShards) {

    public WorkerInfo {
        checkNotNull(workerId, "workerId must not be null");
        checkNotNull(host, "host must not be null");
        checkNotNull(healthState, "healthState must not be null");
        checkNotNull(memoryPressure, "memoryPressure must not be null");
        checkNotNull(lastHeartbeat, "lastHeartbeat must not be null");
        if (protocolVersion <= 0) {
            protocolVersion = 1;
        }
        if (capabilities == null) {
            capabilities = List.of();
        } else {
            capabilities = List.copyOf(capabilities);
        }
        if (dataLocality == null) {
            dataLocality = List.of();
        } else {
            dataLocality = List.copyOf(dataLocality);
        }
        if (vectorShards == null) {
            vectorShards = List.of();
        } else {
            vectorShards = List.copyOf(vectorShards);
        }
    }

    public WorkerInfo(
            String workerId, String host, int httpPort, int transportPort, int resultServerPort,
            HealthState healthState, MemoryPressure memoryPressure, double cpuUtilization,
            int runningTaskCount, long availableMemoryBytes, Instant lastHeartbeat,
            double performanceScore, String resourcePoolId, int protocolVersion,
            List<String> capabilities, String rackId, List<String> dataLocality) {
        this(workerId, host, httpPort, transportPort, resultServerPort, healthState,
                memoryPressure, cpuUtilization, runningTaskCount, availableMemoryBytes,
                lastHeartbeat, performanceScore, resourcePoolId, protocolVersion,
                capabilities, rackId, dataLocality, null, null);
    }

    public WorkerInfo withHealthState(HealthState newState) {
        return new WorkerInfo(workerId, host, httpPort, transportPort, resultServerPort,
                newState, memoryPressure, cpuUtilization, runningTaskCount,
                availableMemoryBytes, lastHeartbeat, performanceScore,
                resourcePoolId, protocolVersion, capabilities,
                rackId, dataLocality, cacheSummary, vectorShards);
    }

    public WorkerInfo withCacheSummary(CacheCatalogSummary summary) {
        return new WorkerInfo(workerId, host, httpPort, transportPort, resultServerPort,
                healthState, memoryPressure, cpuUtilization, runningTaskCount,
                availableMemoryBytes, lastHeartbeat, performanceScore,
                resourcePoolId, protocolVersion, capabilities,
                rackId, dataLocality, summary, vectorShards);
    }

    public WorkerInfo withVectorShards(List<VectorShardInfo> shards) {
        return new WorkerInfo(workerId, host, httpPort, transportPort, resultServerPort,
                healthState, memoryPressure, cpuUtilization, runningTaskCount,
                availableMemoryBytes, lastHeartbeat, performanceScore,
                resourcePoolId, protocolVersion, capabilities,
                rackId, dataLocality, cacheSummary, shards);
    }
}
