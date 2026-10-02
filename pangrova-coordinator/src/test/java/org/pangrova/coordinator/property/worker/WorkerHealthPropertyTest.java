package org.pangrova.coordinator.property.worker;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tag;
import org.pangrova.coordinator.worker.HealthState;
import org.pangrova.coordinator.worker.TestWorkerInfoBuilder;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.resource.MemoryPressure;

import java.time.Instant;
import java.util.List;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property tests for Worker health state machine consistency.
 *
 * <p><b>Validates: Requirements 45.1-45.4, 46.1-46.3</b></p>
 */
class WorkerHealthPropertyTest {

    private static final long SUSPECT_THRESHOLD_MS = 20_000;
    private static final long DEAD_THRESHOLD_MS = 60_000;

    // -- Arbitraries --

    @Provide
    Arbitrary<Long> elapsedMillis() {
        return Arbitraries.longs().between(0, 120_000);
    }

    @Provide
    Arbitrary<String> workerIds() {
        return Arbitraries.strings().alpha().ofMinLength(3).ofMaxLength(10)
                .map(s -> "w-" + s);
    }

    @Provide
    Arbitrary<MemoryPressure> memoryPressures() {
        return Arbitraries.of(MemoryPressure.values());
    }

    @Provide
    Arbitrary<WorkerManager.HeartbeatData> heartbeatData() {
        return Combinators.combine(
                memoryPressures(),
                Arbitraries.doubles().between(0.0, 1.0),
                Arbitraries.integers().between(0, 50),
                Arbitraries.longs().between(0, 16L * 1024 * 1024 * 1024)
        ).as(WorkerManager.HeartbeatData::new);
    }

    @Provide
    Arbitrary<Integer> workerCounts() {
        return Arbitraries.integers().between(1, 20);
    }

    // -- Helpers --

    private static WorkerInfo createWorker(String workerId, Instant lastHeartbeat) {
        return TestWorkerInfoBuilder.withHeartbeat(workerId, "localhost", 8080, 9090, lastHeartbeat);
    }

    private static HealthState expectedState(long elapsedMs) {
        if (elapsedMs > DEAD_THRESHOLD_MS) {
            return HealthState.DEAD;
        }
        if (elapsedMs > SUSPECT_THRESHOLD_MS) {
            return HealthState.SUSPECT;
        }
        return HealthState.ACTIVE;
    }

    // -- Property 23a: Health classification monotonicity --

    /**
     * For any elapsed time since last heartbeat:
     * elapsed ≤ 20s → ACTIVE, 20s < elapsed ≤ 60s → SUSPECT, elapsed > 60s → DEAD.
     *
     * <p><b>Validates: Requirements 45.1-45.4</b></p>
     */
    @Property(tries = 10)
    @Tag("Feature: e2e-pipeline-integration, Property 23: Worker health state machine consistency")
    void healthClassificationMatchesElapsedThresholds(
            @ForAll("workerIds") String workerId,
            @ForAll("elapsedMillis") long elapsedMs) {

        WorkerManager manager = new WorkerManager(20_000, 60_000, new org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry());
        Instant now = Instant.now();
        Instant lastHeartbeat = now.minusMillis(elapsedMs);
        manager.registerWorker(createWorker(workerId, lastHeartbeat));

        HealthState actual = manager.evaluateHealth(workerId, now);
        HealthState expected = expectedState(elapsedMs);

        assertThat(actual)
                .as("elapsed=%dms → expected %s", elapsedMs, expected)
                .isEqualTo(expected);
    }

    // -- Property 23b: Heartbeat resets to ACTIVE --

    /**
     * For any Worker with any elapsed heartbeat time, receiving a heartbeat
     * always resets health to ACTIVE.
     *
     * <p><b>Validates: Requirements 45.1, 46.1-46.3</b></p>
     */
    @Property(tries = 10)
    @Tag("Feature: e2e-pipeline-integration, Property 23: Worker health state machine consistency")
    void heartbeatResetsHealthToActive(
            @ForAll("workerIds") String workerId,
            @ForAll("elapsedMillis") long elapsedMs,
            @ForAll("heartbeatData") WorkerManager.HeartbeatData data) {

        WorkerManager manager = new WorkerManager(20_000, 60_000, new org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry());
        Instant now = Instant.now();
        Instant staleHeartbeat = now.minusMillis(elapsedMs);
        manager.registerWorker(createWorker(workerId, staleHeartbeat));

        // Send heartbeat — should reset to ACTIVE regardless of prior state
        manager.onHeartbeat(workerId, data);

        HealthState afterHeartbeat = manager.evaluateHealth(workerId, Instant.now());
        assertThat(afterHeartbeat)
                .as("After heartbeat, worker must be ACTIVE (was %dms stale)", elapsedMs)
                .isEqualTo(HealthState.ACTIVE);
    }

    // -- Property 23c: Dead worker removal completeness --

    /**
     * For any mix of N workers (some ACTIVE, some DEAD based on lastHeartbeat),
     * removeDeadWorkers() removes exactly the DEAD ones and preserves ACTIVE ones.
     *
     * <p><b>Validates: Requirements 45.3, 45.4</b></p>
     */
    @Property(tries = 10)
    @Tag("Feature: e2e-pipeline-integration, Property 23: Worker health state machine consistency")
    void removeDeadWorkersPreservesActiveOnly(
            @ForAll("workerCounts") int activeCount,
            @ForAll("workerCounts") int deadCount) {

        WorkerManager manager = new WorkerManager(20_000, 60_000, new org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry());
        Instant now = Instant.now();

        List<String> activeIds = IntStream.range(0, activeCount)
                .mapToObj(i -> "active-" + i).toList();
        List<String> deadIds = IntStream.range(0, deadCount)
                .mapToObj(i -> "dead-" + i).toList();

        // Register active workers (heartbeat just now)
        activeIds.forEach(id -> manager.registerWorker(createWorker(id, now)));
        // Register dead workers (heartbeat 120s ago → well past DEAD_THRESHOLD)
        deadIds.forEach(id -> manager.registerWorker(
                createWorker(id, now.minusSeconds(120))));

        // Evaluate health to update internal state before removal
        activeIds.forEach(id -> manager.evaluateHealth(id, now));
        deadIds.forEach(id -> {
            HealthState state = manager.evaluateHealth(id, now);
            // Re-register with DEAD state so removeDeadWorkers can find them
            manager.registerWorker(new WorkerInfo(
                    id, "localhost", 8080, 9090, 0,
                    state, MemoryPressure.NONE,
                    0.5, 0, 1024L * 1024 * 1024,
                    now.minusSeconds(120), 1.0, null, 2,
                    null, List.of(), null,
                    org.pangrova.coordinator.worker.TestWorkerInfoBuilder.canonicalInventory()));
        });

        manager.removeDeadWorkers();

        assertThat(manager.getAllWorkers())
                .as("After removal, only active workers remain")
                .hasSize(activeCount);

        activeIds.forEach(id ->
                assertThat(manager.getWorker(id))
                        .as("Active worker %s must be preserved", id)
                        .isNotNull());

        deadIds.forEach(id ->
                assertThat(manager.getWorker(id))
                        .as("Dead worker %s must be removed", id)
                        .isNull());
    }
}
