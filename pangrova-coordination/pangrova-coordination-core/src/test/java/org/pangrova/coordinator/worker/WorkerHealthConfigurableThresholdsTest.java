package org.pangrova.coordinator.worker;

import org.pangrova.scheduler.api.placement.HealthState;

import java.time.Instant;

import org.pangrova.coordinator.bootstrap.CoordinatorBootstrapConfig;
import org.pangrova.persistence.memory.coordination.InMemoryWorkerRegistry;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Verifies that WorkerManager health thresholds are configurable
 * and override the default 20s/60s values.
 */
class WorkerHealthConfigurableThresholdsTest {

    @Test
    void customThresholdsOverrideDefaults() {
        WorkerManager manager = new WorkerManager(5_000, 15_000, new InMemoryWorkerRegistry());
        manager.registerWorker(workerAt("w1", Instant.now().minusMillis(6_000)));

        HealthState state = manager.evaluateHealth("w1", Instant.now());

        assertEquals(HealthState.SUSPECT, state,
                "6s elapsed with 5s suspect threshold should be SUSPECT");
    }

    @Test
    void customDeadThresholdTriggersDeadState() {
        WorkerManager manager = new WorkerManager(5_000, 15_000, new InMemoryWorkerRegistry());
        manager.registerWorker(workerAt("w1", Instant.now().minusMillis(16_000)));

        HealthState state = manager.evaluateHealth("w1", Instant.now());

        assertEquals(HealthState.DEAD, state,
                "16s elapsed with 15s dead threshold should be DEAD");
    }

    @Test
    void withinCustomSuspectThresholdRemainsActive() {
        WorkerManager manager = new WorkerManager(5_000, 15_000, new InMemoryWorkerRegistry());
        manager.registerWorker(workerAt("w1", Instant.now().minusMillis(3_000)));

        HealthState state = manager.evaluateHealth("w1", Instant.now());

        assertEquals(HealthState.ACTIVE, state,
                "3s elapsed with 5s suspect threshold should be ACTIVE");
    }

    @Test
    void defaultConstructorUsesStandardThresholds() {
        WorkerManager manager = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
        manager.registerWorker(workerAt("w1", Instant.now().minusMillis(25_000)));

        HealthState state = manager.evaluateHealth("w1", Instant.now());

        assertEquals(HealthState.SUSPECT, state,
                "25s elapsed with default 20s suspect / 60s dead should be SUSPECT");
    }

    @Test
    void suspectThresholdMustBePositive() {
        assertThrows(IllegalArgumentException.class,
                () -> new WorkerManager(0, 10_000, new InMemoryWorkerRegistry()));
    }

    @Test
    void deadThresholdMustExceedSuspectThreshold() {
        assertThrows(IllegalArgumentException.class,
                () -> new WorkerManager(10_000, 10_000, new InMemoryWorkerRegistry()));
    }

    @Test
    void configValuesFlowFromCoordinatorBootstrapConfig() {
        var config = new CoordinatorBootstrapConfig(
                new CoordinatorBootstrapConfig.CoreConfig("coord-1", 8080, 30_000, org.pangrova.coordinator.bootstrap.CoordinatorRole.COMBINED, "0.0.0.0"),
                CoordinatorBootstrapConfig.FeatureFlags.allDisabled(),
                new CoordinatorBootstrapConfig.QueryConfig(300, 10, 0),
                CoordinatorBootstrapConfig.TransportConfig.defaults(),
                new CoordinatorBootstrapConfig.WorkerHealthConfig(5_000, 15_000, 2_000),
                null, null, null);

        assertEquals(5_000, config.workerSuspectThresholdMs());
        assertEquals(15_000, config.workerDeadThresholdMs());
        assertEquals(2_000, config.workerHealthCheckIntervalMs());
    }

    private static WorkerInfo workerAt(String id, Instant lastHeartbeat) {
        return TestWorkerInfoBuilder.withHeartbeat(id, "localhost", 8080, 9090, lastHeartbeat);
    }
}
