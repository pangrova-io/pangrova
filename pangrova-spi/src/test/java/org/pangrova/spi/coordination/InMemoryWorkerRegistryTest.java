package org.pangrova.spi.coordination;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;

import org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link InMemoryWorkerRegistry}.
 *
 * <p>Validates: Requirements 3.1</p>
 */
class InMemoryWorkerRegistryTest {

    private final InMemoryWorkerRegistry registry = new InMemoryWorkerRegistry();

    private WorkerSnapshot worker(String id, long lastHeartbeatMs) {
        return new WorkerSnapshot(
                id, "host-" + id, 8080, 9090, 2,
                null, List.of(),
                "NONE", 0.1, 0, 1024L,
                lastHeartbeatMs, lastHeartbeatMs - 10_000,
                0, null, "dGVzdA==",
                "00000000-0000-0000-0000-0000000000d1", "00000000-0000-0000-0000-0000000000e1");
    }

    @Test
    void workerSnapshotRequiresExactV2AndInventoryPayload() {
        long now = System.currentTimeMillis();
        assertThatThrownBy(() -> new WorkerSnapshot(
                "w1", "host-w1", 8080, 9090, 1,
                null, List.of(), "NONE", 0.1, 0, 1024L,
                now, now, 0, null, "dGVzdA==",
                "00000000-0000-0000-0000-0000000000d1", "00000000-0000-0000-0000-0000000000e1"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("canonical v2");
        assertThatThrownBy(() -> new WorkerSnapshot(
                "w1", "host-w1", 8080, 9090, 2,
                null, List.of(), "NONE", 0.1, 0, 1024L,
                now, now, 0, null, "",
                "00000000-0000-0000-0000-0000000000d1", "00000000-0000-0000-0000-0000000000e1"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("capabilityInventory");
    }

    @Test
    void registerAndGet() {
        WorkerSnapshot w = worker("w1", System.currentTimeMillis());
        registry.register(w);

        assertThat(registry.getWorker("w1")).isPresent().contains(w);
    }

    @Test
    void registerIsIdempotent() {
        long now = System.currentTimeMillis();
        WorkerSnapshot first = worker("w1", now - 5000);
        WorkerSnapshot second = worker("w1", now);

        registry.register(first);
        registry.register(second);

        assertThat(registry.workerCount()).isEqualTo(1);
        assertThat(registry.getWorker("w1")).isPresent().contains(second);
    }

    @Test
    void heartbeatUpdatesDynamicFields() {
        long now = System.currentTimeMillis();
        WorkerSnapshot w = new WorkerSnapshot(
                "w1", "host-a", 8080, 9090, 2,
                "rack-1", List.of("/data/1"),
                "NONE", 0.1, 0, 2048L,
                now - 5000, now - 60_000, 0, null, "dGVzdA==",
                "00000000-0000-0000-0000-0000000000d1", "00000000-0000-0000-0000-0000000000e1");
        registry.register(w);

        HeartbeatSnapshot hb = new HeartbeatSnapshot("HIGH", 0.95, 5, 512L);
        registry.heartbeat("w1", hb);

        WorkerSnapshot updated = registry.getWorker("w1").orElseThrow();
        // dynamic fields updated
        assertThat(updated.memoryPressure()).isEqualTo("HIGH");
        assertThat(updated.cpuUtilization()).isEqualTo(0.95);
        assertThat(updated.runningTaskCount()).isEqualTo(5);
        assertThat(updated.availableMemoryBytes()).isEqualTo(512L);
        assertThat(updated.lastHeartbeatMs()).isGreaterThanOrEqualTo(now);
        // static fields unchanged
        assertThat(updated.workerId()).isEqualTo("w1");
        assertThat(updated.host()).isEqualTo("host-a");
        assertThat(updated.httpPort()).isEqualTo(8080);
        assertThat(updated.transportPort()).isEqualTo(9090);
        assertThat(updated.protocolVersion()).isEqualTo(2);
        assertThat(updated.capabilityInventory()).isEqualTo("dGVzdA==");
        assertThat(updated.rackId()).isEqualTo("rack-1");
        assertThat(updated.dataLocality()).containsExactly("/data/1");
        assertThat(updated.registeredAtMs()).isEqualTo(now - 60_000);
    }

    @Test
    void heartbeatForUnregisteredWorkerIsNoOp() {
        HeartbeatSnapshot hb = new HeartbeatSnapshot("LOW", 0.5, 2, 1024L);
        registry.heartbeat("unknown", hb);

        assertThat(registry.getWorker("unknown")).isEmpty();
        assertThat(registry.workerCount()).isZero();
    }

    @Test
    void unregisterRemovesWorker() {
        registry.register(worker("w1", System.currentTimeMillis()));
        registry.unregister("w1");

        assertThat(registry.getWorker("w1")).isEmpty();
        assertThat(registry.workerCount()).isZero();
    }

    @Test
    void unregisterUnknownWorkerIsNoOp() {
        registry.unregister("ghost");

        assertThat(registry.workerCount()).isZero();
    }

    @Test
    void getActiveWorkersFiltersStale() {
        long now = System.currentTimeMillis();
        WorkerSnapshot fresh = worker("fresh", now);
        WorkerSnapshot stale = worker("stale", now - 60_000);

        registry.register(fresh);
        registry.register(stale);

        List<WorkerSnapshot> active = registry.getActiveWorkers(Duration.ofSeconds(30));
        assertThat(active).hasSize(1);
        assertThat(active.get(0).workerId()).isEqualTo("fresh");
    }

    @Test
    void getActiveWorkersOnEmptyRegistry() {
        List<WorkerSnapshot> active = registry.getActiveWorkers(Duration.ofSeconds(30));
        assertThat(active).isEmpty();
    }

    @Test
    void expireStaleRemovesOldEntries() {
        long now = System.currentTimeMillis();
        registry.register(worker("old1", now - 120_000));
        registry.register(worker("old2", now - 90_000));
        registry.register(worker("fresh", now));

        int expired = registry.expireStale(Duration.ofSeconds(60));

        assertThat(expired).isEqualTo(2);
        assertThat(registry.workerCount()).isEqualTo(1);
        assertThat(registry.getWorker("fresh")).isPresent();
        assertThat(registry.getWorker("old1")).isEmpty();
        assertThat(registry.getWorker("old2")).isEmpty();
    }

    @Test
    void expireStaleReturnsZeroWhenNoneStale() {
        long now = System.currentTimeMillis();
        registry.register(worker("w1", now));
        registry.register(worker("w2", now));

        int expired = registry.expireStale(Duration.ofSeconds(60));

        assertThat(expired).isZero();
        assertThat(registry.workerCount()).isEqualTo(2);
    }
}
