package org.pangrova.coordinator.worker;

import java.time.Instant;
import java.util.List;

import org.pangrova.protocol.CanonicalTaskRequests;
import org.pangrova.protocol.v2.CapabilityInventory;
import org.pangrova.resource.MemoryPressure;

/**
 * Test factory for WorkerInfo instances using the canonical constructor.
 * All helpers construct the exact canonical v2 worker contract.
 */
public final class TestWorkerInfoBuilder {

    private TestWorkerInfoBuilder() {}

    /**
     * Creates an ACTIVE worker with default metrics and no resource pool.
     */
    public static WorkerInfo active(String workerId, String host, int httpPort, int transportPort) {
        return new WorkerInfo(workerId, host, httpPort, transportPort, 0,
                HealthState.ACTIVE, MemoryPressure.NONE,
                0.0, 0, 1024L * 1024 * 1024, Instant.now(), 1.0, null, 2,
                null, List.of(), null, canonicalInventory());
    }

    /**
     * Creates a worker with the specified health state and default metrics.
     */
    public static WorkerInfo withState(String workerId, String host, int httpPort, int transportPort,
                                        HealthState state) {
        return new WorkerInfo(workerId, host, httpPort, transportPort, 0,
                state, MemoryPressure.NONE,
                0.0, 0, 1024L * 1024 * 1024, Instant.now(), 1.0, null, 2,
                null, List.of(), null, canonicalInventory());
    }

    /**
     * Creates an ACTIVE worker with the specified performance metrics.
     */
    public static WorkerInfo withMetrics(String workerId, String host, int httpPort, int transportPort,
                                          double cpuUtilization, int runningTaskCount,
                                          long availableMemoryBytes, double performanceScore) {
        return new WorkerInfo(workerId, host, httpPort, transportPort, 0,
                HealthState.ACTIVE, MemoryPressure.NONE,
                cpuUtilization, runningTaskCount, availableMemoryBytes,
                Instant.now(), performanceScore, null, 2,
                null, List.of(), null, canonicalInventory());
    }

    /**
     * Creates an ACTIVE worker with the specified last heartbeat timestamp.
     */
    public static WorkerInfo withHeartbeat(String workerId, String host, int httpPort, int transportPort,
                                            Instant lastHeartbeat) {
        return new WorkerInfo(workerId, host, httpPort, transportPort, 0,
                HealthState.ACTIVE, MemoryPressure.NONE,
                0.5, 0, 1024L * 1024 * 1024,
                lastHeartbeat, 1.0, null, 2,
                null, List.of(), null, canonicalInventory());
    }

    /**
     * Creates an ACTIVE worker with the specified running task count (load).
     */
    public static WorkerInfo withLoad(String workerId, String host, int httpPort, int transportPort,
                                       int runningTaskCount) {
        return new WorkerInfo(workerId, host, httpPort, transportPort, 0,
                HealthState.ACTIVE, MemoryPressure.NONE,
                0.0, runningTaskCount, 1024L * 1024 * 1024,
                Instant.now(), 1.0, null, 2,
                null, List.of(), null, canonicalInventory());
    }

    /**
     * Creates an ACTIVE worker with topology info (rackId + dataLocality).
     */
    public static WorkerInfo withTopology(String workerId, String host, int httpPort, int transportPort,
                                           String rackId, List<String> dataLocality) {
        return new WorkerInfo(workerId, host, httpPort, transportPort, 0,
                HealthState.ACTIVE, MemoryPressure.NONE,
                0.0, 0, 1024L * 1024 * 1024,
                Instant.now(), 1.0, null, 2,
                rackId, dataLocality, null, canonicalInventory());
    }

    /**
     * Creates an ACTIVE worker with rackId only (no dataLocality).
     */
    public static WorkerInfo withRack(String workerId, String host, int httpPort, int transportPort,
                                       String rackId) {
        return new WorkerInfo(workerId, host, httpPort, transportPort, 0,
                HealthState.ACTIVE, MemoryPressure.NONE,
                0.0, 0, 1024L * 1024 * 1024,
                Instant.now(), 1.0, null, 2,
                rackId, List.of(), null, canonicalInventory());
    }

    /**
     * Creates an ACTIVE worker with the specified memory pressure level.
     */
    public static WorkerInfo withPressure(String workerId, String host, int httpPort, int transportPort,
                                           MemoryPressure pressure) {
        return new WorkerInfo(workerId, host, httpPort, transportPort, 0,
                HealthState.ACTIVE, pressure,
                0.0, 0, 1024L * 1024 * 1024, Instant.now(), 1.0, null, 2,
                null, List.of(), null, canonicalInventory());
    }

    /**
     * Creates an ACTIVE worker with a native result server port.
     */
    public static WorkerInfo withResultServerPort(String workerId, String host, int httpPort,
                                                   int transportPort, int resultServerPort) {
        return new WorkerInfo(workerId, host, httpPort, transportPort, resultServerPort,
                HealthState.ACTIVE, MemoryPressure.NONE,
                0.0, 0, 1024L * 1024 * 1024,
                Instant.now(), 1.0, null, 2,
                null, List.of(), null, canonicalInventory());
    }

    /** Returns an empty canonical v2 inventory for non-capability scheduling tests. */
    public static CapabilityInventory canonicalInventory() {
        return CapabilityInventory.newBuilder()
                .setProtocol(CanonicalTaskRequests.protocolVersion())
                .setResultEnvelopeVersion(1)
                .setNativeEngineVersion("test-native")
                .build();
    }
}
