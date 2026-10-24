package org.pangrova.coordinator.query.execution;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;

import org.pangrova.coordinator.query.execution.split.CacheAffinityAssigner;
import org.pangrova.coordinator.worker.HealthState;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.resource.MemoryPressure;
import org.junit.jupiter.api.Test;

/**
 * Tests for CacheAffinityAssigner topology wiring via WorkerManager events.
 * Verifies that the hash ring is automatically rebuilt when workers join or leave.
 */
class CacheAffinityTopologyWiringTest {

    @Test
    void workerRegistrationTriggersRingRebuild() {
        CacheAffinityAssigner assigner = new CacheAffinityAssigner();
        WorkerManager wm = new WorkerManager(20_000, 60_000, new org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry());
        wm.addTopologyListener(assigner);

        assertThat(assigner.hasWorkers()).isFalse();

        wm.registerWorker(buildWorker("w-1"));
        assertThat(assigner.hasWorkers()).isTrue();
        assertThat(assigner.preferredWorker("s3://bucket/file.parquet")).isEqualTo("w-1");
    }

    @Test
    void workerRemovalTriggersRingRebuild() {
        CacheAffinityAssigner assigner = new CacheAffinityAssigner();
        WorkerManager wm = new WorkerManager(20_000, 60_000, new org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry());
        wm.addTopologyListener(assigner);

        wm.registerWorker(buildWorker("w-1"));
        wm.registerWorker(buildWorker("w-2"));
        assertThat(assigner.hasWorkers()).isTrue();

        wm.removeWorker("w-1");
        wm.removeWorker("w-2");
        assertThat(assigner.hasWorkers()).isFalse();
    }

    @Test
    void heartbeatTimeoutToDeadTriggersRingRebuild() {
        CacheAffinityAssigner assigner = new CacheAffinityAssigner();
        WorkerManager wm = new WorkerManager(5_000, 15_000, new org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry());
        wm.addTopologyListener(assigner);

        wm.registerWorker(buildWorker("w-1"));
        wm.registerWorker(buildWorker("w-2"));

        // Both workers in ring
        String preferred = assigner.preferredWorker("s3://file.parquet");
        assertThat(preferred).isIn("w-1", "w-2");

        // Simulate heartbeat timeout — evaluate health far in the future
        Instant farFuture = Instant.now().plusSeconds(120);
        wm.evaluateHealth("w-1", farFuture);

        // w-1 is now DEAD, ring should only have w-2
        assertThat(assigner.preferredWorker("s3://file.parquet")).isEqualTo("w-2");
    }

    @Test
    void removeDeadWorkersTriggersRingRebuild() {
        CacheAffinityAssigner assigner = new CacheAffinityAssigner();
        WorkerManager wm = new WorkerManager(5_000, 15_000, new org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry());
        wm.addTopologyListener(assigner);

        wm.registerWorker(buildWorker("w-1"));
        assertThat(assigner.hasWorkers()).isTrue();

        // Mark as DEAD via health evaluation
        Instant farFuture = Instant.now().plusSeconds(120);
        wm.evaluateHealth("w-1", farFuture);

        // Remove dead workers
        wm.removeDeadWorkers();
        assertThat(assigner.hasWorkers()).isFalse();
    }

    @Test
    void multipleListenersAllNotified() {
        WorkerManager wm = new WorkerManager(20_000, 60_000, new org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry());
        CacheAffinityAssigner assigner1 = new CacheAffinityAssigner();
        CacheAffinityAssigner assigner2 = new CacheAffinityAssigner();
        wm.addTopologyListener(assigner1);
        wm.addTopologyListener(assigner2);

        wm.registerWorker(buildWorker("w-1"));
        assertThat(assigner1.hasWorkers()).isTrue();
        assertThat(assigner2.hasWorkers()).isTrue();
    }

    private static WorkerInfo buildWorker(String workerId) {
        return new WorkerInfo(
                workerId, "localhost", 8080, 9090, 0,
                HealthState.ACTIVE, MemoryPressure.NONE,
                0.1, 0, 1_000_000_000L, Instant.now(),
                1.0, null, 2, null, List.of(), null,
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.canonicalInventory(),
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.executionIdentity(workerId));
    }
}
