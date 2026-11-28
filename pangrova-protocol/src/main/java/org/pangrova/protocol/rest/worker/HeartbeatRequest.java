package org.pangrova.protocol.rest.worker;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.pangrova.protocol.execution.identity.WorkerExecutionIdentity;

import java.util.List;
import java.util.Objects;

/** Current Worker placement and resource usage, sampled once per heartbeat. */
public record HeartbeatRequest(
        @JsonProperty("worker_id") String workerId,
        @JsonProperty("memory_pressure") String memoryPressure,
        @JsonProperty("cpu_utilization") double cpuUtilization,
        @JsonProperty("in_flight_task_count") int inFlightTaskCount,
        @JsonProperty("available_memory_bytes") long availableMemoryBytes,
        @JsonProperty("cached_file_paths") List<String> cachedFilePaths,
        @JsonProperty("tenant_usage") List<TenantUsageSnapshot> tenantUsage,
        @JsonProperty("worker_execution_identity") WorkerExecutionIdentity workerExecutionIdentity) {

    public HeartbeatRequest {
        if (workerId == null || workerId.isBlank()) {
            throw new IllegalArgumentException("workerId must not be blank");
        }
        if (memoryPressure == null || memoryPressure.isBlank()) {
            memoryPressure = "NONE";
        }
        if (inFlightTaskCount < 0) {
            throw new IllegalArgumentException("inFlightTaskCount must not be negative");
        }
        Objects.requireNonNull(workerExecutionIdentity, "workerExecutionIdentity must not be null");
        cachedFilePaths = cachedFilePaths == null ? List.of() : List.copyOf(cachedFilePaths);
        tenantUsage = tenantUsage == null ? List.of() : List.copyOf(tenantUsage);
    }

    public HeartbeatRequest(String workerId, String memoryPressure, double cpuUtilization,
            int inFlightTaskCount, long availableMemoryBytes, WorkerExecutionIdentity workerExecutionIdentity) {
        this(workerId, memoryPressure, cpuUtilization, inFlightTaskCount, availableMemoryBytes, List.of(), List.of(),
                workerExecutionIdentity);
    }
}
