package org.pangrova.protocol.rest.worker;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

/**
 * Heartbeat payload sent by Worker to Coordinator on each heartbeat tick.
 * F-CHAIN-CACHE-02: cachedFilePaths replaces the dead cacheBloomFilter field.
 */
public record HeartbeatRequest(
        @JsonProperty("worker_id") String workerId,
        @JsonProperty("memory_pressure") String memoryPressure,
        @JsonProperty("cpu_utilization") double cpuUtilization,
        @JsonProperty("running_task_count") int runningTaskCount,
        @JsonProperty("available_memory_bytes") long availableMemoryBytes,
        @JsonProperty("cached_file_paths") List<String> cachedFilePaths,
        @JsonProperty("cache_used_bytes") long cacheUsedBytes,
        @JsonProperty("cache_max_bytes") long cacheMaxBytes,
        @JsonProperty("cache_hit_count") long cacheHitCount,
        @JsonProperty("cache_miss_count") long cacheMissCount,
        @JsonProperty("tenant_memory_bytes") Map<String, Long> tenantMemoryBytes,
        @JsonProperty("tenant_active_queries") Map<String, Integer> tenantActiveQueries,
        @JsonProperty("worker_execution_identity") org.pangrova.protocol.execution.identity.WorkerExecutionIdentity workerExecutionIdentity) {

    public HeartbeatRequest {
        if (workerId == null || workerId.isBlank()) {
            throw new IllegalArgumentException("workerId must not be blank");
        }
        if (memoryPressure == null || memoryPressure.isBlank()) {
            memoryPressure = "NONE";
        }
        java.util.Objects.requireNonNull(workerExecutionIdentity, "workerExecutionIdentity must not be null");
        cachedFilePaths = cachedFilePaths == null ? List.of() : List.copyOf(cachedFilePaths);
        tenantMemoryBytes = tenantMemoryBytes == null ? Map.of() : Map.copyOf(tenantMemoryBytes);
        tenantActiveQueries = tenantActiveQueries == null ? Map.of() : Map.copyOf(tenantActiveQueries);
    }

    public HeartbeatRequest(String workerId, String memoryPressure,
                            double cpuUtilization, int runningTaskCount,
                            long availableMemoryBytes,
                            org.pangrova.protocol.execution.identity.WorkerExecutionIdentity workerExecutionIdentity) {
        this(workerId, memoryPressure, cpuUtilization, runningTaskCount,
                availableMemoryBytes, List.of(), 0, 0, 0, 0, Map.of(), Map.of(), workerExecutionIdentity);
    }
}
