package org.pangrova.coordinator.scheduler;

import org.pangrova.coordinator.worker.HealthState;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.coordinator.worker.WorkerPerformanceScorer;
import org.pangrova.resource.MemoryPressure;
import org.pangrova.resource.QueueType;
import org.pangrova.resource.scheduler.WeightedFairScheduler;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Collections;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that {@link TaskScheduler} integrates {@link WeightedFairScheduler}
 * for tenant-aware resource allocation (R10.10).
 */
class TaskSchedulerFairSchedulingTest {

    @Test
    void selectWorkerWithoutFairSchedulerUsesDefaultScoring() {
        TaskScheduler scheduler = new TaskScheduler(new WorkerPerformanceScorer());
        List<WorkerInfo> candidates = List.of(
                worker("w1", "pool-a", MemoryPressure.NONE),
                worker("w2", "pool-a", MemoryPressure.NONE));

        Optional<WorkerInfo> selected = scheduler.selectWorker(0L, candidates, "pool-a");

        assertThat(selected).isPresent();
    }

    @Test
    void equalScoreWorkersShareConcurrentSelections() {
        TaskScheduler scheduler = new TaskScheduler(new WorkerPerformanceScorer());
        List<WorkerInfo> candidates = List.of(
                worker("w2", "pool-a", MemoryPressure.NONE),
                worker("w1", "pool-a", MemoryPressure.NONE));

        List<String> placements = IntStream.range(0, 1_000).parallel()
                .mapToObj(index -> scheduler.selectWorker(0L, candidates, "pool-a")
                        .orElseThrow().workerId())
                .toList();

        assertThat(Collections.frequency(placements, "w1")).isEqualTo(500);
        assertThat(Collections.frequency(placements, "w2")).isEqualTo(500);
    }

    @Test
    void equalScoreFairShareWorkersAlsoRotate() {
        TaskScheduler scheduler = new TaskScheduler(
                new WorkerPerformanceScorer(), new WeightedFairScheduler());
        List<WorkerInfo> candidates = List.of(
                worker("w2", "pool-a", MemoryPressure.NONE),
                worker("w1", "pool-a", MemoryPressure.NONE));

        assertThat(scheduler.selectWorker(0L, candidates, "pool-a")
                .orElseThrow().workerId()).isEqualTo("w1");
        assertThat(scheduler.selectWorker(0L, candidates, "pool-a")
                .orElseThrow().workerId()).isEqualTo("w2");
    }

    @Test
    void selectWorkerWithFairSchedulerUsesWeightedShares() {
        WeightedFairScheduler fairScheduler = new WeightedFairScheduler();
        fairScheduler.submitTask(() -> {}, QueueType.INTERACTIVE);
        fairScheduler.submitTask(() -> {}, QueueType.BATCH);

        TaskScheduler scheduler = new TaskScheduler(new WorkerPerformanceScorer(), fairScheduler);
        List<WorkerInfo> candidates = List.of(
                worker("w1", "interactive-pool", MemoryPressure.NONE),
                worker("w2", "interactive-pool", MemoryPressure.NONE));

        Optional<WorkerInfo> selected = scheduler.selectWorker(
                0L, candidates, "interactive-pool");

        assertThat(selected).isPresent();
    }

    @Test
    void selectWorkerWithNullResourcePoolFallsToDefault() {
        WeightedFairScheduler fairScheduler = new WeightedFairScheduler();
        TaskScheduler scheduler = new TaskScheduler(new WorkerPerformanceScorer(), fairScheduler);
        List<WorkerInfo> candidates = List.of(worker("w1", null, MemoryPressure.NONE));

        Optional<WorkerInfo> selected = scheduler.selectWorker(0L, candidates, null);

        assertThat(selected).isPresent();
    }

    @Test
    void selectWorkerWithFairSchedulerAndNoActiveQueuesStillSelects() {
        WeightedFairScheduler fairScheduler = new WeightedFairScheduler();
        TaskScheduler scheduler = new TaskScheduler(new WorkerPerformanceScorer(), fairScheduler);
        List<WorkerInfo> candidates = List.of(worker("w1", "batch-pool", MemoryPressure.NONE));

        Optional<WorkerInfo> selected = scheduler.selectWorker(0L, candidates, "batch-pool");

        assertThat(selected).isPresent();
    }

    @Test
    void nullFairSchedulerIsAccepted() {
        TaskScheduler scheduler = new TaskScheduler(new WorkerPerformanceScorer(), null);
        List<WorkerInfo> candidates = List.of(worker("w1", "pool-a", MemoryPressure.NONE));

        Optional<WorkerInfo> selected = scheduler.selectWorker(0L, candidates, "pool-a");

        assertThat(selected).isPresent();
    }

    @Test
    void fairSchedulerRespectsMemoryPressureFilter() {
        WeightedFairScheduler fairScheduler = new WeightedFairScheduler();
        fairScheduler.submitTask(() -> {}, QueueType.BATCH);
        TaskScheduler scheduler = new TaskScheduler(new WorkerPerformanceScorer(), fairScheduler);

        List<WorkerInfo> candidates = List.of(
                worker("w-critical", "batch-pool", MemoryPressure.CRITICAL));

        Optional<WorkerInfo> selected = scheduler.selectWorker(0L, candidates, "batch-pool");

        assertThat(selected).isEmpty();
    }

    @Test
    void unknownTaskMemoryDoesNotPlaceOnHighPressureWorker() {
        TaskScheduler scheduler = new TaskScheduler(new WorkerPerformanceScorer());
        List<WorkerInfo> candidates = List.of(
                worker("w-high", "batch-pool", MemoryPressure.HIGH));

        assertThat(scheduler.selectWorker(0L, candidates, "batch-pool")).isEmpty();
    }

    private static WorkerInfo worker(String id, String resourcePoolId, MemoryPressure pressure) {
        return new WorkerInfo(
                id, "host-" + id, 8080, 9090, 0,
                HealthState.ACTIVE, pressure,
                0.1, 0, 8L * 1024 * 1024 * 1024,
                Instant.now(), 1.0, resourcePoolId, 2,
                null, List.of(), null,
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.canonicalInventory());
    }
}
