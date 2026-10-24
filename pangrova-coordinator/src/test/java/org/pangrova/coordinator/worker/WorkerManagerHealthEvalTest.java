package org.pangrova.coordinator.worker;

import org.pangrova.resource.MemoryPressure;
import org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Validates BUG-C04: WorkerManager.evaluateHealth() TOCTOU fix.
 * Entire evaluation now runs inside computeIfPresent for atomicity.
 */
class WorkerManagerHealthEvalTest {

    private WorkerManager manager;

    @BeforeEach
    void setUp() {
        // 20s suspect, 60s dead
        manager = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
    }

    @Test
    @DisplayName("evaluateHealth returns ACTIVE within suspect threshold")
    void activeWithinThreshold() {
        Instant now = Instant.now();
        manager.registerWorker(createWorker("w1", now));

        HealthState state = manager.evaluateHealth("w1", now.plusMillis(10_000));
        assertThat(state).isEqualTo(HealthState.ACTIVE);
        assertThat(manager.getWorker("w1").healthState()).isEqualTo(HealthState.ACTIVE);
    }

    @Test
    @DisplayName("evaluateHealth returns SUSPECT after suspect threshold")
    void suspectAfterThreshold() {
        Instant now = Instant.now();
        manager.registerWorker(createWorker("w1", now));

        HealthState state = manager.evaluateHealth("w1", now.plusMillis(25_000));
        assertThat(state).isEqualTo(HealthState.SUSPECT);
        assertThat(manager.getWorker("w1").healthState()).isEqualTo(HealthState.SUSPECT);
    }

    @Test
    @DisplayName("evaluateHealth returns DEAD after dead threshold")
    void deadAfterThreshold() {
        Instant now = Instant.now();
        manager.registerWorker(createWorker("w1", now));

        HealthState state = manager.evaluateHealth("w1", now.plusMillis(65_000));
        assertThat(state).isEqualTo(HealthState.DEAD);
        assertThat(manager.getWorker("w1").healthState()).isEqualTo(HealthState.DEAD);
    }

    @Test
    @DisplayName("evaluateHealth returns UNKNOWN for unknown worker")
    void deadForUnknownWorker() {
        HealthState state = manager.evaluateHealth("unknown", Instant.now());
        assertThat(state).isEqualTo(HealthState.UNKNOWN);
    }

    @Test
    @DisplayName("evaluateHealth atomically updates stored WorkerInfo")
    void atomicUpdateOfStoredWorkerInfo() {
        Instant now = Instant.now();
        manager.registerWorker(createWorker("w1", now));

        // First eval: ACTIVE
        manager.evaluateHealth("w1", now.plusMillis(5_000));
        assertThat(manager.getWorker("w1").healthState()).isEqualTo(HealthState.ACTIVE);

        // Second eval: SUSPECT — stored state must reflect this
        manager.evaluateHealth("w1", now.plusMillis(25_000));
        assertThat(manager.getWorker("w1").healthState()).isEqualTo(HealthState.SUSPECT);

        // Third eval: DEAD — stored state must reflect this
        manager.evaluateHealth("w1", now.plusMillis(65_000));
        assertThat(manager.getWorker("w1").healthState()).isEqualTo(HealthState.DEAD);
    }

    private static WorkerInfo createWorker(String id, Instant heartbeat) {
        return new WorkerInfo(id, "localhost", 8080, 9090, 0,
                HealthState.ACTIVE, MemoryPressure.NONE,
                0.5, 0, 1024 * 1024 * 1024L, heartbeat,
                1.0, null, 2, null, List.of(), null,
                TestWorkerInfoBuilder.canonicalInventory(),
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.executionIdentity(id));
    }
}
