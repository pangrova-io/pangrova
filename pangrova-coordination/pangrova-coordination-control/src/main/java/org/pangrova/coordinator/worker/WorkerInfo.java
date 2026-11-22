package org.pangrova.coordinator.worker;

import org.pangrova.protocol.descriptor.CanonicalProtocolIdentity;

import java.time.Instant;
import java.util.List;

import org.pangrova.protocol.execution.identity.WorkerExecutionIdentity;
import org.pangrova.protocol.capability.inventory.CapabilityInventoryCodec;
import org.pangrova.protocol.v2.CapabilityInventory;
import org.pangrova.common.enums.MemoryPressure;
import org.pangrova.scheduler.api.enums.HealthState;
import org.pangrova.scheduler.api.placement.PlacementWorker;
import org.pangrova.coordination.api.heartbeat.CacheCatalogSummary;

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
        AdmittedInventory admittedInventory,
        int copyTaskTlsPort,
        WorkerExecutionIdentity workerExecutionIdentity,
        org.pangrova.coordination.api.membership.WorkerShuffleDeployment shuffleDeployment) implements PlacementWorker {
    private static final int MAX_TCP_PORT = 65535;

    /** Retains the one verified catalogue across health and heartbeat snapshots. */
    public record AdmittedInventory(CapabilityInventory inventory) {
        public AdmittedInventory {
            CapabilityInventoryCodec.validate(checkNotNull(inventory, "capabilityInventory must not be null"));
        }
    }

    /** Admits an immutable catalogue for a new Worker snapshot. */
    public WorkerInfo(String workerId, String host, int httpPort, int transportPort,
                      int resultServerPort, HealthState healthState, MemoryPressure memoryPressure,
                      double cpuUtilization, int runningTaskCount, long availableMemoryBytes,
                      Instant lastHeartbeat, double performanceScore, String resourcePoolId,
                      int protocolVersion, String rackId, List<String> dataLocality,
                      CacheCatalogSummary cacheSummary, CapabilityInventory capabilityInventory,
                      int copyTaskTlsPort, WorkerExecutionIdentity workerExecutionIdentity,
                      org.pangrova.coordination.api.membership.WorkerShuffleDeployment shuffleDeployment) {
        this(workerId, host, httpPort, transportPort, resultServerPort, healthState,
                memoryPressure, cpuUtilization, runningTaskCount, availableMemoryBytes, lastHeartbeat,
                performanceScore, resourcePoolId, protocolVersion, rackId, dataLocality, cacheSummary,
                new AdmittedInventory(capabilityInventory), copyTaskTlsPort, workerExecutionIdentity,
                shuffleDeployment);
    }

    /** Validates the ports and health fields of a Worker view. */
    public WorkerInfo(
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
        CapabilityInventory capabilityInventory,
        int copyTaskTlsPort,
        WorkerExecutionIdentity workerExecutionIdentity) {
        this(workerId, host, httpPort, transportPort, resultServerPort, healthState,
                memoryPressure, cpuUtilization, runningTaskCount, availableMemoryBytes, lastHeartbeat, performanceScore,
                resourcePoolId, protocolVersion, rackId, dataLocality, cacheSummary, capabilityInventory,
                        copyTaskTlsPort, workerExecutionIdentity, null);
    }

    /** Compatibility constructor for snapshots without a dedicated COPY task listener. */
    @SuppressWarnings("checkstyle:ParameterNumber")
    public WorkerInfo(String workerId, String host, int httpPort, int transportPort,
                      int resultServerPort, HealthState healthState, MemoryPressure memoryPressure,
                      double cpuUtilization, int runningTaskCount, long availableMemoryBytes,
                      Instant lastHeartbeat, double performanceScore, String resourcePoolId,
                      int protocolVersion, String rackId, List<String> dataLocality,
                      CacheCatalogSummary cacheSummary, CapabilityInventory capabilityInventory,
                      WorkerExecutionIdentity workerExecutionIdentity) {
        this(workerId, host, httpPort, transportPort, resultServerPort, healthState,
                memoryPressure, cpuUtilization, runningTaskCount, availableMemoryBytes,
                lastHeartbeat, performanceScore, resourcePoolId, protocolVersion, rackId,
                dataLocality, cacheSummary, capabilityInventory, 0, workerExecutionIdentity);
    }

    public WorkerInfo {
        checkNotNull(workerExecutionIdentity, "workerExecutionIdentity must not be null");
        checkNotNull(workerId, "workerId must not be null");
        checkNotNull(host, "host must not be null");
        checkNotNull(healthState, "healthState must not be null");
        checkNotNull(memoryPressure, "memoryPressure must not be null");
        checkNotNull(lastHeartbeat, "lastHeartbeat must not be null");
        if (copyTaskTlsPort < 0 || copyTaskTlsPort > MAX_TCP_PORT) {
            throw new IllegalArgumentException("copyTaskTlsPort must be a valid optional TCP port");
        }
        if (protocolVersion != CanonicalProtocolIdentity.PROTOCOL_MAJOR) {
            throw new IllegalArgumentException(
                    "protocolVersion must equal canonical v2");
        }
        if (dataLocality == null) {
            dataLocality = List.of();
        } else {
            dataLocality = List.copyOf(dataLocality);
        }
        checkNotNull(admittedInventory, "admittedInventory must not be null");
    }

    /** Returns the actual Native capability message retained by the Worker view. */
    public CapabilityInventory capabilityInventory() { return admittedInventory.inventory(); }

    /** Returns a Worker snapshot with the updated health state. */
    public WorkerInfo withHealthState(HealthState newState) {
        return new WorkerInfo(workerId, host, httpPort, transportPort, resultServerPort,
                newState, memoryPressure, cpuUtilization, runningTaskCount,
                availableMemoryBytes, lastHeartbeat, performanceScore,
                resourcePoolId, protocolVersion,
                rackId, dataLocality, cacheSummary, admittedInventory, copyTaskTlsPort, workerExecutionIdentity,
                        shuffleDeployment);
    }

    /** Returns a Worker snapshot with the updated cache summary. */
    public WorkerInfo withCacheSummary(CacheCatalogSummary summary) {
        return new WorkerInfo(workerId, host, httpPort, transportPort, resultServerPort,
                healthState, memoryPressure, cpuUtilization, runningTaskCount,
                availableMemoryBytes, lastHeartbeat, performanceScore,
                resourcePoolId, protocolVersion,
                rackId, dataLocality, summary, admittedInventory, copyTaskTlsPort, workerExecutionIdentity,
                        shuffleDeployment);
    }

    WorkerInfo withCapabilityInventory(AdmittedInventory inventory) {
        return new WorkerInfo(workerId, host, httpPort, transportPort, resultServerPort,
                healthState, memoryPressure, cpuUtilization, runningTaskCount,
                availableMemoryBytes, lastHeartbeat, performanceScore, resourcePoolId,
                protocolVersion, rackId, dataLocality, cacheSummary, inventory, copyTaskTlsPort,
                workerExecutionIdentity, shuffleDeployment);
    }
}
