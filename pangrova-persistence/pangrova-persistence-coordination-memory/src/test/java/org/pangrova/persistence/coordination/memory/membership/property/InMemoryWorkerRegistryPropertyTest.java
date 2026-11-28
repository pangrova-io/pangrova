package org.pangrova.persistence.coordination.memory.membership.property;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

import org.pangrova.coordination.api.heartbeat.HeartbeatSnapshot;
import org.pangrova.persistence.coordination.memory.membership.InMemoryWorkerRegistry;
import org.pangrova.coordination.api.membership.WorkerSnapshot;

/**
 * Property-based tests for {@link InMemoryWorkerRegistry}.
 *
 * <p>Validates: Requirements 3.1</p>
 */
class InMemoryWorkerRegistryPropertyTest {

    /** Intermediate record to hold the first 8 fields of WorkerSnapshot. */
    private record IdentityPart(
            String workerId, String host, int httpPort, int transportPort,
            String rackId, List<String> dataLocality) {}

    /** Intermediate record to hold the remaining 6 fields of WorkerSnapshot. */
    private record MetricsPart(
            String memoryPressure, double cpuUtilization,
            int inFlightTaskCount, long availableMemoryBytes,
            long lastHeartbeatMs, long registeredAtMs) {}

    @Provide
    Arbitrary<WorkerSnapshot> workerSnapshots() {
        long now = System.currentTimeMillis();

        Arbitrary<IdentityPart> identity = Combinators.combine(
                Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(12).map(s -> "w-" + s),
                Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(10).map(s -> "host-" + s),
                Arbitraries.integers().between(1024, 65535),
                Arbitraries.integers().between(1024, 65535),
                Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(8)
                        .map(s -> "rack-" + s).injectNull(0.3),
                Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(8)
                        .map(s -> "/data/" + s).list().ofMinSize(0).ofMaxSize(3)
        ).as(IdentityPart::new);

        Arbitrary<MetricsPart> metrics = Combinators.combine(
                Arbitraries.of("NONE", "LOW", "HIGH"),
                Arbitraries.doubles().between(0.0, 1.0),
                Arbitraries.integers().between(0, 100),
                Arbitraries.longs().between(0L, 16_000_000_000L),
                Arbitraries.longs().between(now - 300_000, now),
                Arbitraries.longs().between(now - 600_000, now - 300_000)
        ).as(MetricsPart::new);

        return Combinators.combine(identity, metrics).as((id, m) -> new WorkerSnapshot(
                id.workerId(), id.host(), id.httpPort(), id.transportPort(),
                2, id.rackId(), id.dataLocality(),
                m.memoryPressure(), m.cpuUtilization(), m.inFlightTaskCount(),
                m.availableMemoryBytes(), m.lastHeartbeatMs(), m.registeredAtMs(),
                0, null, "dGVzdA==",
                "00000000-0000-0000-0000-0000000000d1", "00000000-0000-0000-0000-0000000000e1"));
    }

    @Provide
    Arbitrary<HeartbeatSnapshot> heartbeatSnapshots() {
        return Combinators.combine(
                Arbitraries.of("NONE", "LOW", "HIGH"),
                Arbitraries.doubles().between(0.0, 1.0),
                Arbitraries.integers().between(0, 100),
                Arbitraries.longs().between(0L, 16_000_000_000L)
        ).as((pressure, cpu, tasks, memory) -> new HeartbeatSnapshot(pressure, cpu, tasks, memory, null));
    }

    /**
     * For any valid WorkerSnapshot, after register(), getWorker() returns
     * the same snapshot.
     */
    @Property(tries = 10)
    void registerThenGetReturnsRegisteredWorker(
            @ForAll("workerSnapshots") WorkerSnapshot worker) {
        var registry = new InMemoryWorkerRegistry();
        registry.register(worker);

        assertThat(registry.getWorker(worker.workerId()))
                .isPresent()
                .contains(worker);
    }

    /**
     * For any registered worker and any HeartbeatSnapshot, after heartbeat(),
     * the static fields (workerId, host, httpPort, transportPort,
     * protocolVersion, capabilityInventory, rackId, dataLocality, registeredAtMs)
     * remain unchanged.
     */
    @Property(tries = 10)
    void heartbeatPreservesStaticFields(
            @ForAll("workerSnapshots") WorkerSnapshot worker,
            @ForAll("heartbeatSnapshots") HeartbeatSnapshot heartbeat) {
        var registry = new InMemoryWorkerRegistry();
        registry.register(worker);
        registry.heartbeat(worker.registrationIdentity(), heartbeat);

        var updated = registry.getWorker(worker.workerId()).orElseThrow();
        assertThat(updated.workerId()).isEqualTo(worker.workerId());
        assertThat(updated.host()).isEqualTo(worker.host());
        assertThat(updated.httpPort()).isEqualTo(worker.httpPort());
        assertThat(updated.transportPort()).isEqualTo(worker.transportPort());
        assertThat(updated.protocolVersion()).isEqualTo(worker.protocolVersion());
        assertThat(updated.capabilityInventory()).isEqualTo(worker.capabilityInventory());
        assertThat(updated.rackId()).isEqualTo(worker.rackId());
        assertThat(updated.dataLocality()).isEqualTo(worker.dataLocality());
        assertThat(updated.registeredAtMs()).isEqualTo(worker.registeredAtMs());
    }

    /**
     * For any registered worker and any HeartbeatSnapshot, after heartbeat(),
     * the dynamic fields (memoryPressure, cpuUtilization, inFlightTaskCount,
     * availableMemoryBytes) match the heartbeat snapshot.
     */
    @Property(tries = 10)
    void heartbeatUpdatesDynamicFields(
            @ForAll("workerSnapshots") WorkerSnapshot worker,
            @ForAll("heartbeatSnapshots") HeartbeatSnapshot heartbeat) {
        var registry = new InMemoryWorkerRegistry();
        registry.register(worker);

        long beforeHeartbeat = System.currentTimeMillis();
        registry.heartbeat(worker.registrationIdentity(), heartbeat);

        var updated = registry.getWorker(worker.workerId()).orElseThrow();
        assertThat(updated.memoryPressure()).isEqualTo(heartbeat.memoryPressure());
        assertThat(updated.cpuUtilization()).isEqualTo(heartbeat.cpuUtilization());
        assertThat(updated.inFlightTaskCount()).isEqualTo(heartbeat.inFlightTaskCount());
        assertThat(updated.availableMemoryBytes()).isEqualTo(heartbeat.availableMemoryBytes());
        assertThat(updated.lastHeartbeatMs()).isGreaterThanOrEqualTo(beforeHeartbeat);
    }

    /**
     * Register N workers with varying lastHeartbeatMs. After
     * expireStale(threshold), only workers with lastHeartbeatMs older than
     * threshold are removed.
     */
    @Property(tries = 10)
    void expireStaleOnlyRemovesOldEntries(
            @ForAll("workerSnapshots") WorkerSnapshot template) {
        var registry = new InMemoryWorkerRegistry();
        long now = System.currentTimeMillis();
        long thresholdMs = 60_000;

        var fresh = new WorkerSnapshot(
                "fresh-" + template.workerId(), template.host(),
                template.httpPort(), template.transportPort(),
                template.protocolVersion(),
                template.rackId(), template.dataLocality(),
                template.memoryPressure(), template.cpuUtilization(),
                template.inFlightTaskCount(), template.availableMemoryBytes(),
                now, template.registeredAtMs(), template.resultServerPort(),
                template.cacheSummary(), template.capabilityInventory(),
                "00000000-0000-0000-0000-0000000000d1", "00000000-0000-0000-0000-0000000000e1");

        var stale = new WorkerSnapshot(
                "stale-" + template.workerId(), template.host(),
                template.httpPort(), template.transportPort(),
                template.protocolVersion(),
                template.rackId(), template.dataLocality(),
                template.memoryPressure(), template.cpuUtilization(),
                template.inFlightTaskCount(), template.availableMemoryBytes(),
                now - thresholdMs - 10_000, template.registeredAtMs(),
                template.resultServerPort(), template.cacheSummary(),
                template.capabilityInventory(),
                "00000000-0000-0000-0000-0000000000d1", "00000000-0000-0000-0000-0000000000e1");

        registry.register(fresh);
        registry.register(stale);

        int expired = registry.expireStale(Duration.ofMillis(thresholdMs));

        assertThat(expired).isEqualTo(1);
        assertThat(registry.getWorker(fresh.workerId())).isPresent();
        assertThat(registry.getWorker(stale.workerId())).isEmpty();
    }
}
