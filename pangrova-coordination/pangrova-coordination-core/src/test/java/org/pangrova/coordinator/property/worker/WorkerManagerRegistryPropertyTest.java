package org.pangrova.coordinator.property.worker;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.pangrova.scheduler.api.placement.HealthState;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.common.value.MemoryPressure;
import org.pangrova.persistence.memory.coordination.InMemoryWorkerRegistry;
import org.pangrova.spi.coordination.WorkerSnapshot;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based tests for {@link WorkerManager} registry integration.
 *
 * <p>Validates: Requirements 5.1-5.10</p>
 *
 * <p>Verifies that after operation sequences, the local cache and the
 * underlying {@link InMemoryWorkerRegistry} remain consistent.</p>
 */
class WorkerManagerRegistryPropertyTest {

    private static final long SUSPECT_MS = 20_000;
    private static final long DEAD_MS = 60_000;

    // --- intermediate records for WorkerInfo (16 fields, exceeds jqwik 8-param limit) ---

    /** Static identity fields of a WorkerInfo. */
    private record IdentityPart(
            String workerId, String host, int httpPort, int transportPort,
            HealthState healthState, MemoryPressure memoryPressure,
            double cpuUtilization, int runningTaskCount) {}

    /** Dynamic/metric fields of a WorkerInfo. */
    private record MetricsPart(
            long availableMemoryBytes, Instant lastHeartbeat,
            double performanceScore, String resourcePoolId,
            String rackId, List<String> dataLocality) {}

    @Provide
    Arbitrary<WorkerInfo> workerInfos() {
        Arbitrary<IdentityPart> identity = Combinators.combine(
                Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(8).map(s -> "w-" + s),
                Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(8).map(s -> "host-" + s),
                Arbitraries.integers().between(1024, 65535),
                Arbitraries.integers().between(1024, 65535),
                Arbitraries.of(HealthState.ACTIVE),
                Arbitraries.of(MemoryPressure.values()),
                Arbitraries.doubles().between(0.0, 1.0),
                Arbitraries.integers().between(0, 50)
        ).as(IdentityPart::new);

        Arbitrary<MetricsPart> metrics = Combinators.combine(
                Arbitraries.longs().between(0L, 16_000_000_000L),
                Arbitraries.longs().between(
                        System.currentTimeMillis() - 10_000,
                        System.currentTimeMillis()).map(Instant::ofEpochMilli),
                Arbitraries.doubles().between(0.0, 1.0),
                Arbitraries.of((String) null),
                Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(6)
                        .map(s -> "rack-" + s).injectNull(0.3),
                Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(6)
                        .map(s -> "/data/" + s).list().ofMinSize(0).ofMaxSize(2)
        ).as(MetricsPart::new);

        return Combinators.combine(identity, metrics).as((id, m) -> new WorkerInfo(
                id.workerId(), id.host(), id.httpPort(), id.transportPort(),
                0,
                id.healthState(), id.memoryPressure(),
                id.cpuUtilization(), id.runningTaskCount(),
                m.availableMemoryBytes(), m.lastHeartbeat(),
                m.performanceScore(), m.resourcePoolId(),
                2, m.rackId(), m.dataLocality(), null,
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder
                        .canonicalInventory(),
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.executionIdentity(id.workerId())));
    }

    @Provide
    Arbitrary<WorkerManager.HeartbeatData> heartbeatData() {
        return Combinators.combine(
                Arbitraries.of(MemoryPressure.values()),
                Arbitraries.doubles().between(0.0, 1.0),
                Arbitraries.integers().between(0, 50),
                Arbitraries.longs().between(0L, 16_000_000_000L)
        ).as(WorkerManager.HeartbeatData::new);
    }

    /**
     * For any valid WorkerInfo, after registerWorker(), the registry contains
     * a snapshot with matching workerId, host, and httpPort.
     *
     * <p>Validates: Requirements 5.3</p>
     */
    @Property(tries = 10)
    void registerThenRegistryContainsWorker(
            @ForAll("workerInfos") WorkerInfo info) {
        var registry = new InMemoryWorkerRegistry();
        var manager = new WorkerManager(SUSPECT_MS, DEAD_MS, registry);

        manager.registerWorker(info);

        assertThat(registry.getWorker(info.workerId())).isPresent();
        WorkerSnapshot snapshot = registry.getWorker(info.workerId()).get();
        assertThat(snapshot.workerId()).isEqualTo(info.workerId());
        assertThat(snapshot.host()).isEqualTo(info.host());
        assertThat(snapshot.httpPort()).isEqualTo(info.httpPort());
    }

    /**
     * For any registered worker and any HeartbeatData, after onHeartbeat(),
     * the registry snapshot's dynamic fields match the heartbeat data.
     *
     * <p>Validates: Requirements 5.4</p>
     */
    @Property(tries = 10)
    void heartbeatUpdatesRegistryDynamicFields(
            @ForAll("workerInfos") WorkerInfo info,
            @ForAll("heartbeatData") WorkerManager.HeartbeatData heartbeat) {
        var registry = new InMemoryWorkerRegistry();
        var manager = new WorkerManager(SUSPECT_MS, DEAD_MS, registry);

        manager.registerWorker(info);
        manager.onHeartbeat(info.workerId(), heartbeat);

        WorkerSnapshot snapshot = registry.getWorker(info.workerId()).orElseThrow();
        assertThat(snapshot.memoryPressure()).isEqualTo(heartbeat.pressure().name());
        assertThat(snapshot.cpuUtilization()).isEqualTo(heartbeat.cpuUtilization());
        assertThat(snapshot.runningTaskCount()).isEqualTo(heartbeat.runningTaskCount());
        assertThat(snapshot.availableMemoryBytes()).isEqualTo(heartbeat.availableMemoryBytes());
        // static fields unchanged
        assertThat(snapshot.host()).isEqualTo(info.host());
        assertThat(snapshot.httpPort()).isEqualTo(info.httpPort());
    }

    /**
     * For any registered worker, after removeWorker(), the registry no longer
     * contains it.
     *
     * <p>Validates: Requirements 5.5</p>
     */
    @Property(tries = 10)
    void removeWorkerClearsRegistry(
            @ForAll("workerInfos") WorkerInfo info) {
        var registry = new InMemoryWorkerRegistry();
        var manager = new WorkerManager(SUSPECT_MS, DEAD_MS, registry);

        manager.registerWorker(info);
        assertThat(registry.getWorker(info.workerId())).isPresent();

        manager.removeWorker(info.workerId());

        assertThat(registry.getWorker(info.workerId())).isEmpty();
    }

    /**
     * Register N workers, then remove some. After all operations, every worker
     * in local cache (getActiveWorkers) also exists in registry, and vice versa
     * for registered workers.
     *
     * <p>Validates: Requirements 5.1-5.10</p>
     */
    @Property(tries = 10)
    void localCacheAndRegistryConsistentAfterOperations(
            @ForAll("workerInfos") WorkerInfo template) {
        var registry = new InMemoryWorkerRegistry();
        var manager = new WorkerManager(SUSPECT_MS, DEAD_MS, registry);

        // Register N workers with unique IDs derived from template
        List<String> registeredIds = new ArrayList<>();
        int workerCount = 5;
        for (int i = 0; i < workerCount; i++) {
            String uniqueId = template.workerId() + "-" + i;
            WorkerInfo worker = new WorkerInfo(
                    uniqueId, template.host(), template.httpPort(), template.transportPort(),
                    0,
                    HealthState.ACTIVE, template.memoryPressure(),
                    template.cpuUtilization(), template.runningTaskCount(),
                    template.availableMemoryBytes(), Instant.now(),
                    template.performanceScore(), null,
                    template.protocolVersion(), template.rackId(),
                    template.dataLocality(), null,
                    template.capabilityInventory(),
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.executionIdentity(uniqueId));
            manager.registerWorker(worker);
            registeredIds.add(uniqueId);
        }

        // Remove the first two workers
        for (int i = 0; i < 2; i++) {
            manager.removeWorker(registeredIds.get(i));
        }

        // Verify consistency: every active worker in local cache exists in registry
        List<WorkerInfo> activeWorkers = manager.getActiveWorkers();
        for (WorkerInfo active : activeWorkers) {
            assertThat(registry.getWorker(active.workerId()))
                    .as("Worker %s in local cache must exist in registry", active.workerId())
                    .isPresent();
        }

        // Verify consistency: every worker in registry exists in local cache
        List<String> remainingIds = registeredIds.subList(2, workerCount);
        for (String id : remainingIds) {
            assertThat(registry.getWorker(id)).isPresent();
            assertThat(activeWorkers).extracting(WorkerInfo::workerId).contains(id);
        }

        // Verify removed workers are gone from both
        for (int i = 0; i < 2; i++) {
            String removedId = registeredIds.get(i);
            assertThat(registry.getWorker(removedId)).isEmpty();
            assertThat(activeWorkers).extracting(WorkerInfo::workerId).doesNotContain(removedId);
        }
    }
}
