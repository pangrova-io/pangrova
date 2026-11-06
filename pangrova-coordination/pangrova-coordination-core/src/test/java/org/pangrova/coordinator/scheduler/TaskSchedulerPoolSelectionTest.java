package org.pangrova.coordinator.scheduler;

import org.pangrova.scheduler.core.placement.TaskScheduler;
import org.pangrova.scheduler.api.placement.HealthState;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.scheduler.builtin.placement.LoadBalancedStrategy;
import org.pangrova.common.value.MemoryPressure;
import org.junit.jupiter.api.Test;
import java.time.Instant;
import java.util.List;
import java.util.Collections;
import java.util.stream.IntStream;
import static org.assertj.core.api.Assertions.assertThat;

/** Pool, pressure and tie-rotation behavior through the actual built-in Provider. */
class TaskSchedulerPoolSelectionTest {
    private final TaskScheduler scheduler = new TaskScheduler(new LoadBalancedStrategy(), ignoredTenantForPlacement -> java.util.Optional.empty());

    @Test
    void matchingPoolSelectsItsWorker() {
        assertThat(scheduler.selectWorker(0, List.of(worker("w", "pool-a", MemoryPressure.NONE)), "pool-a")
                .orElseThrow().workerId()).isEqualTo("w");
    }

    @Test
    void equalScoreWorkersShareConcurrentSelections() {
        var workers = List.of(worker("w2", "pool-a", MemoryPressure.NONE), worker("w1", "pool-a", MemoryPressure.NONE));
        var placements = IntStream.range(0, 1000).parallel()
                .mapToObj(i -> scheduler.selectWorker(0, workers, "pool-a").orElseThrow().workerId()).toList();
        assertThat(Collections.frequency(placements, "w1")).isEqualTo(500);
        assertThat(Collections.frequency(placements, "w2")).isEqualTo(500);
    }

    @Test
    void tiedWorkersRotateInIdentityOrder() {
        var workers = List.of(worker("w2", "pool-a", MemoryPressure.NONE), worker("w1", "pool-a", MemoryPressure.NONE));
        assertThat(scheduler.selectWorker(0, workers, "pool-a").orElseThrow().workerId()).isEqualTo("w1");
        assertThat(scheduler.selectWorker(0, workers, "pool-a").orElseThrow().workerId()).isEqualTo("w2");
    }

    @Test
    void missingPoolCannotFallBackToAnotherPool() {
        var workers = List.of(worker("w", "pool-b", MemoryPressure.NONE));
        assertThat(scheduler.eligibleWorkers(0, workers, "pool-a")).isEmpty();
        assertThat(scheduler.selectWorker(0, workers, "pool-a")).isEmpty();
        assertThat(scheduler.selectWorkerWithLocality(0, workers, "pool-a", List.of())).isEmpty();
    }

    @Test
    void nullPoolAllowsAllPools() {
        assertThat(scheduler.selectWorker(0, List.of(worker("w", "pool-a", MemoryPressure.NONE)), null)
                .orElseThrow().workerId()).isEqualTo("w");
    }

    @Test
    void unhealthyPoolCannotFallBackToHealthyOtherPool() {
        var workers = List.of(worker("bad", "pool-a", MemoryPressure.CRITICAL), worker("good", "pool-b", MemoryPressure.NONE));
        assertThat(scheduler.selectWorker(0, workers, "pool-a")).isEmpty();
    }

    @Test
    void oversizedTaskIsRejectedBeforeRanking() {
        assertThat(scheduler.selectWorker(Long.MAX_VALUE, List.of(worker("w", "pool-a", MemoryPressure.NONE)), "pool-a")).isEmpty();
    }

    @Test
    void criticalPressureIsRejected() {
        assertThat(scheduler.selectWorker(0, List.of(worker("w", "pool-a", MemoryPressure.CRITICAL)), "pool-a")).isEmpty();
    }

    @Test
    void unknownTaskMemoryDoesNotPlaceOnHighPressureWorker() {
        assertThat(scheduler.selectWorker(0, List.of(worker("w", "pool-a", MemoryPressure.HIGH)), "pool-a")).isEmpty();
    }

    private static WorkerInfo worker(String id, String resourcePoolId, MemoryPressure pressure) {
        return new WorkerInfo(
                id, "host-" + id, 8080, 9090, 0,
                HealthState.ACTIVE, pressure,
                0.1, 0, 8L * 1024 * 1024 * 1024,
                Instant.now(), 1.0, resourcePoolId, 2,
                null, List.of(), null,
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.canonicalInventory(),
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.executionIdentity(id));
    }
}
