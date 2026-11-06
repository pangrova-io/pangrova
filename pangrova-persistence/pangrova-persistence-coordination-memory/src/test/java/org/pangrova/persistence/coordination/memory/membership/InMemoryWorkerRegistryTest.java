package org.pangrova.persistence.coordination.memory.membership;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;

import org.pangrova.coordination.api.membership.WorkerSnapshot;
import org.junit.jupiter.api.Test;
import org.pangrova.coordination.api.heartbeat.HeartbeatSnapshot;
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
        registry.heartbeat(registry.getWorker("w1").orElseThrow().registrationIdentity(), hb);

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
        registry.heartbeat(worker("unknown", System.currentTimeMillis()).registrationIdentity(), hb);

        assertThat(registry.getWorker("unknown")).isEmpty();
        assertThat(registry.workerCount()).isZero();
    }

    @Test
    void unregisterRemovesWorker() {
        registry.register(worker("w1", System.currentTimeMillis()));
        registry.unregister(registry.getWorker("w1").orElseThrow().registrationIdentity());

        assertThat(registry.getWorker("w1")).isEmpty();
        assertThat(registry.workerCount()).isZero();
    }

    @Test
    void unregisterUnknownWorkerIsNoOp() {
        registry.unregister(worker("ghost", System.currentTimeMillis()).registrationIdentity());

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

    @Test
    void staleWithdrawalCannotDeleteReplacementOrDifferentAdmissionDomain() {

        long now = System.currentTimeMillis();
        var original = removalWorker("00000000-0000-0000-0000-0000000000d1",
                "00000000-0000-0000-0000-0000000000e1", now);
        registry.register(original);
        var replacement = removalWorker(original.admissionDomainId(),
                "00000000-0000-0000-0000-0000000000e2", now);
        registry.register(replacement);
        assertThat(registry.unregister(original.registrationIdentity())).isFalse();
        assertThat(registry.getWorker(original.workerId())).contains(replacement);
        var moved = removalWorker("00000000-0000-0000-0000-0000000000d2",
                replacement.incarnationId(), now);
        registry.register(moved);
        assertThat(registry.unregister(replacement.registrationIdentity())).isFalse();
        assertThat(registry.getWorker(original.workerId())).contains(moved);
        assertThat(registry.unregister(moved.registrationIdentity())).isTrue();
        assertThat(registry.unregister(moved.registrationIdentity())).isFalse();
    }

    @Test
    void heartbeatMetricsDoNotChangeRegistrationIdentity() {

        var original = removalWorker("00000000-0000-0000-0000-0000000000d1",
                "00000000-0000-0000-0000-0000000000e1", System.currentTimeMillis());
        registry.register(original);
        registry.heartbeat(original.registrationIdentity(),
                new org.pangrova.coordination.api.heartbeat.HeartbeatSnapshot("HIGH", 0.9, 3, 123L));
        assertThat(registry.getWorker(original.workerId()).orElseThrow().runningTaskCount()).isEqualTo(3);
        assertThat(registry.unregister(original.registrationIdentity())).isTrue();
    }

    private static WorkerSnapshot removalWorker(String domain, String incarnation, long now) {
        return new WorkerSnapshot("withdrawal-worker", "host", 8080, 9090, 2,
                null, java.util.List.of(), "NONE", 0.0, 0, 1024L, now, now,
                9191, null, "dGVzdA==", domain, incarnation);
    }
    @Test
    void exactHeartbeatAndConditionalRepairCannotOverwriteAnotherProcess() throws Exception {

        long now = System.currentTimeMillis();
        var old = removalWorker("00000000-0000-0000-0000-0000000000d1",
                "00000000-0000-0000-0000-0000000000e1", now);
        var metrics = new org.pangrova.coordination.api.heartbeat.HeartbeatSnapshot("HIGH", 0.9, 7, 99L);
        assertThat(registry.heartbeat(old.registrationIdentity(), metrics)).isEmpty();
        assertThat(registry.refreshRegistration(old)).isTrue();
        assertThat(registry.refreshRegistration(old)).isTrue();
        assertThat(registry.heartbeat(old.registrationIdentity(), metrics).orElseThrow().runningTaskCount()).isEqualTo(7);
        var replacement = removalWorker(old.admissionDomainId(), "00000000-0000-0000-0000-0000000000e2", now);
        registry.register(replacement);
        assertThat(registry.heartbeat(old.registrationIdentity(), metrics)).isEmpty();
        assertThat(registry.refreshRegistration(old)).isFalse();
        assertThat(registry.getWorker(old.workerId())).contains(replacement);
        var moved = removalWorker("00000000-0000-0000-0000-0000000000d2", replacement.incarnationId(), now);
        registry.register(moved);
        assertThat(registry.heartbeat(replacement.registrationIdentity(), metrics)).isEmpty();
        assertThat(registry.refreshRegistration(replacement)).isFalse();
        assertThat(registry.getWorker(old.workerId())).contains(moved);
    }

}
