package org.pangrova.coordinator.scheduler;

import org.pangrova.scheduler.core.placement.TaskScheduler;
import org.pangrova.scheduler.api.placement.HealthState;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.scheduler.builtin.placement.LoadBalancedStrategy;
import org.pangrova.common.value.MemoryPressure;
import org.junit.jupiter.api.Test;
import java.time.Instant;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;

/** Ranking is independent of queue fairness; pool filtering is a Core constraint. */
class TaskSchedulerRankingTest {
    private final TaskScheduler scheduler = new TaskScheduler(new LoadBalancedStrategy(), ignoredTenantForPlacement -> java.util.Optional.empty());

    @Test
    void lowerCpuWinsWithinPool() {
        var workers = List.of(worker("better", "interactive-pool", .1, .2), worker("worse", "interactive-pool", .5, .6));
        assertThat(scheduler.selectWorker(0, workers, "interactive-pool").orElseThrow().workerId()).isEqualTo("better");
    }

    @Test
    void higherScoringOtherPoolCannotBypassPoolConstraint() {
        var workers = List.of(worker("in", "interactive-pool", .5, .6), worker("out", "batch-pool", .1, .1));
        assertThat(scheduler.selectWorker(0, workers, "interactive-pool").orElseThrow().workerId()).isEqualTo("in");
        assertThat(scheduler.selectWorker(0, workers).orElseThrow().workerId()).isEqualTo("out");
    }

    @Test
    void currentRequestReplacesPreviousCandidates() {
        assertThat(scheduler.selectWorker(0, List.of(worker("old", "a", .1, .1))).orElseThrow().workerId()).isEqualTo("old");
        assertThat(scheduler.selectWorker(0, List.of(worker("new", "a", .9, .9))).orElseThrow().workerId()).isEqualTo("new");
    }

    @Test
    void nullPoolUsesAllCandidates() {
        var workers = List.of(worker("better", null, .1, .1), worker("worse", null, .9, .9));
        assertThat(scheduler.selectWorker(0, workers, null).orElseThrow().workerId()).isEqualTo("better");
    }

    private static WorkerInfo worker(String id, String resourcePoolId,
                                     double cpuUtilization, double performanceScore) {
        return new WorkerInfo(
                id, "host-" + id, 8080, 9090, 0,
                HealthState.ACTIVE, MemoryPressure.NONE,
                cpuUtilization, 0, 8L * 1024 * 1024 * 1024,
                Instant.now(), performanceScore, resourcePoolId, 2,
                null, List.of(), null,
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.canonicalInventory(),
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.executionIdentity(id));
    }
}
