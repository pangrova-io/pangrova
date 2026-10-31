package org.pangrova.coordinator.worker;

import org.pangrova.scheduler.api.placement.HealthState;

import java.time.Instant;
import java.util.List;

import org.pangrova.common.value.MemoryPressure;
import org.pangrova.coordination.inmemory.InMemoryWorkerRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class WorkerManagerPoolIdPreservationTest {

    private WorkerManager manager;

    @BeforeEach
    void setUp() {
        manager = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
    }

    @Test
    void heartbeatPreservesResourcePoolId() {
        WorkerInfo registered = new WorkerInfo(
                "w-pool", "host-1", 8080, 9090, 0,
                HealthState.ACTIVE, MemoryPressure.NONE,
                0.1, 0, 1024L * 1024 * 1024,
                Instant.now(), 0.0, "gpu-pool",
                2, null, List.of(), null,
                TestWorkerInfoBuilder.canonicalInventory(),
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.executionIdentity("w-pool"));
        manager.registerWorker(registered);

        manager.onHeartbeat("w-pool", new WorkerManager.HeartbeatData(
                MemoryPressure.LOW, 0.5, 3, 512L * 1024 * 1024));

        WorkerInfo updated = manager.getActiveWorkers().stream()
                .filter(w -> "w-pool".equals(w.workerId()))
                .findFirst().orElseThrow();
        assertThat(updated.resourcePoolId()).isEqualTo("gpu-pool");
    }

    @Test
    void heartbeatPreservesNullPoolId() {
        WorkerInfo registered = new WorkerInfo(
                "w-no-pool", "host-2", 8080, 9090, 0,
                HealthState.ACTIVE, MemoryPressure.NONE,
                0.1, 0, 1024L * 1024 * 1024,
                Instant.now(), 0.0, null,
                2, null, List.of(), null,
                TestWorkerInfoBuilder.canonicalInventory(),
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.executionIdentity("w-no-pool"));
        manager.registerWorker(registered);

        manager.onHeartbeat("w-no-pool", new WorkerManager.HeartbeatData(
                MemoryPressure.NONE, 0.2, 1, 768L * 1024 * 1024));

        WorkerInfo updated = manager.getActiveWorkers().stream()
                .filter(w -> "w-no-pool".equals(w.workerId()))
                .findFirst().orElseThrow();
        assertThat(updated.resourcePoolId()).isNull();
    }
}
