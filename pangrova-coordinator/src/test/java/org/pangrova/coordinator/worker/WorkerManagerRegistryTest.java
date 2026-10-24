package org.pangrova.coordinator.worker;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import org.pangrova.resource.MemoryPressure;
import org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry;
import org.pangrova.spi.coordination.WorkerSnapshot;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for WorkerManager registry integration (write-through and loadFromRegistry).
 *
 * <p>Uses {@link InMemoryWorkerRegistry} — no mocks, no external dependencies.
 *
 * <p>Validates: Requirements 5.1-5.10
 */
class WorkerManagerRegistryTest {

    private static final long SUSPECT_MS = 20_000;
    private static final long DEAD_MS = 60_000;

    private InMemoryWorkerRegistry registry;
    private WorkerManager manager;

    @BeforeEach
    void setUp() {
        registry = new InMemoryWorkerRegistry();
        manager = new WorkerManager(SUSPECT_MS, DEAD_MS, registry);
    }

    // --- helpers ---

    private static WorkerInfo workerInfo(String id) {
        return new WorkerInfo(
                id, "host-" + id, 8080, 9090, 0,
                HealthState.ACTIVE, MemoryPressure.NONE,
                0.1, 0, 1024L * 1024 * 1024,
                Instant.now(), 0.0, null,
                2, null, List.of(), null,
                TestWorkerInfoBuilder.canonicalInventory(), 8443,
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.executionIdentity(id));
    }

    private static WorkerSnapshot workerSnapshot(String id, long lastHeartbeatMs) {
        return new WorkerSnapshot(
                id, "host-" + id, 8080, 9090,
                2, null, List.of(),
                "NONE", 0.1, 0, 1024L * 1024 * 1024,
                lastHeartbeatMs, lastHeartbeatMs, 0, null,
                org.pangrova.protocol.CapabilityInventoryCodec.encode(
                        TestWorkerInfoBuilder.canonicalInventory()), 8443,
                "00000000-0000-0000-0000-0000000000d1", "00000000-0000-0000-0000-0000000000e1");
    }

    // --- test cases ---

    /**
     * Req 5.3: registerWorker() writes through to registry.
     */
    @Test
    void registerWorkerWritesThroughToRegistry() {
        WorkerInfo info = workerInfo("w-1");

        manager.registerWorker(info);

        assertThat(registry.getWorker("w-1")).isPresent();
        WorkerSnapshot snapshot = registry.getWorker("w-1").get();
        assertThat(snapshot.workerId()).isEqualTo("w-1");
        assertThat(snapshot.host()).isEqualTo("host-w-1");
        assertThat(snapshot.httpPort()).isEqualTo(8080);
        assertThat(snapshot.transportPort()).isEqualTo(9090);
        assertThat(snapshot.copyTaskTlsPort()).isEqualTo(8443);
        assertThat(snapshot.admissionDomainId()).isEqualTo(info.workerExecutionIdentity().admissionDomainId());
        assertThat(snapshot.incarnationId()).isEqualTo(info.workerExecutionIdentity().incarnationId());
    }

    /**
     * Req 5.4: onHeartbeat() writes through to registry, updating dynamic fields.
     */
    @Test
    void heartbeatWritesThroughToRegistry() {
        WorkerInfo info = workerInfo("w-2");
        manager.registerWorker(info);

        manager.onHeartbeat("w-2", new WorkerManager.HeartbeatData(
                MemoryPressure.LOW, 0.75, 5, 512L * 1024 * 1024));

        WorkerSnapshot snapshot = registry.getWorker("w-2").orElseThrow();
        assertThat(snapshot.memoryPressure()).isEqualTo("LOW");
        assertThat(snapshot.cpuUtilization()).isEqualTo(0.75);
        assertThat(snapshot.runningTaskCount()).isEqualTo(5);
        assertThat(snapshot.availableMemoryBytes()).isEqualTo(512L * 1024 * 1024);
        // static fields unchanged
        assertThat(snapshot.host()).isEqualTo("host-w-2");
        assertThat(snapshot.httpPort()).isEqualTo(8080);
        assertThat(snapshot.copyTaskTlsPort()).isEqualTo(8443);
        assertThat(snapshot.admissionDomainId()).isEqualTo(info.workerExecutionIdentity().admissionDomainId());
        assertThat(snapshot.incarnationId()).isEqualTo(info.workerExecutionIdentity().incarnationId());
    }

    /**
     * Req 5.5: removeWorker() writes through to registry.
     */
    @Test
    void removeWorkerWritesThroughToRegistry() {
        WorkerInfo info = workerInfo("w-3");
        manager.registerWorker(info);
        assertThat(registry.getWorker("w-3")).isPresent();

        manager.removeWorker("w-3");

        assertThat(registry.getWorker("w-3")).isEmpty();
    }

    /**
     * Req 5.6: loadFromRegistry() populates local cache from registry.
     */
    @Test
    void loadFromRegistryPopulatesLocalCache() {
        long now = System.currentTimeMillis();
        registry.register(workerSnapshot("w-10", now));
        registry.register(workerSnapshot("w-11", now));
        registry.register(workerSnapshot("w-12", now));

        assertThat(manager.getActiveWorkers()).isEmpty();

        manager.loadFromRegistry();

        List<WorkerInfo> active = manager.getActiveWorkers();
        assertThat(active).hasSize(3);
        assertThat(active).extracting(WorkerInfo::workerId)
                .containsExactlyInAnyOrder("w-10", "w-11", "w-12");
        assertThat(active).extracting(WorkerInfo::copyTaskTlsPort)
                .containsOnly(8443);
        assertThat(active).allSatisfy(worker -> {
            assertThat(worker.workerExecutionIdentity().admissionDomainId())
                    .isEqualTo("00000000-0000-0000-0000-0000000000d1");
            assertThat(worker.workerExecutionIdentity().incarnationId())
                    .isEqualTo("00000000-0000-0000-0000-0000000000e1");
        });
    }

    /**
     * Req 5.7: expireStaleFromRegistry() delegates to registry.expireStale().
     */
    @Test
    void expireStaleFromRegistryDelegatesToRegistry() {
        long now = System.currentTimeMillis();
        long staleTime = now - DEAD_MS - 1000;
        registry.register(workerSnapshot("w-fresh", now));
        registry.register(workerSnapshot("w-stale-1", staleTime));
        registry.register(workerSnapshot("w-stale-2", staleTime));

        int expired = manager.expireStaleFromRegistry(Duration.ofMillis(DEAD_MS));

        assertThat(expired).isEqualTo(2);
        assertThat(registry.getWorker("w-fresh")).isPresent();
        assertThat(registry.getWorker("w-stale-1")).isEmpty();
        assertThat(registry.getWorker("w-stale-2")).isEmpty();
    }

    /**
     * Req 5.8: getActiveWorkers() reads from local cache, not DB round-trip.
     */
    @Test
    void getActiveWorkersReadsFromLocalCache() {
        manager.registerWorker(workerInfo("w-cached-1"));
        manager.registerWorker(workerInfo("w-cached-2"));

        // Add a worker directly to registry (bypassing WorkerManager)
        long now = System.currentTimeMillis();
        registry.register(workerSnapshot("w-registry-only", now));

        // getActiveWorkers reads local cache — should NOT include registry-only worker
        List<WorkerInfo> active = manager.getActiveWorkers();
        assertThat(active).hasSize(2);
        assertThat(active).extracting(WorkerInfo::workerId)
                .containsExactlyInAnyOrder("w-cached-1", "w-cached-2");
    }
}
