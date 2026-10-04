package org.pangrova.coordinator.speculation;

import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.task.TaskDuration;
import org.pangrova.coordinator.task.TaskCancelRetrier;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.observability.noop.NoOpTracingProvider;
import org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry;

import java.time.Instant;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Pure duration/configuration inputs; these tests do not assert Worker or Native execution. */
class SpeculativeExecutionAlgorithmTest {
    @Test
    void disabled_noDetection() {
        var tracker = new CoordinatorTaskTracker();
        registerDurationInput(tracker, "t1");
        var launcher = new SpeculativeTaskLauncher();
        var handler = new SpeculativeStageHandler(SpeculationConfig.DISABLED,
                new StragglerDetector(), launcher, tracker);

        handler.evaluateStragglers("duration-query", "1");

        assertThat(launcher.getOriginalTasksWithCopies()).isEmpty();
    }

    @Test
    void durationSnapshots() {
        var tracker = new CoordinatorTaskTracker();
        registerDurationInput(tracker, "t1");
        registerDurationInput(tracker, "t2");
        tracker.updateTaskState("duration-query", "t2", "FINISHED");

        var snapshots = tracker.getTaskDurationSnapshots("duration-query", "1", Set.of(), Instant.now());

        assertThat(snapshots).hasSize(2);
        assertThat(snapshots.stream().filter(TaskDuration::completed).count()).isEqualTo(1);
    }

    @Test
    void configDefaults() {
        assertThat(SpeculationConfig.DISABLED.enabled()).isFalse();
        assertThat(SpeculationConfig.DISABLED.detectionIntervalMs()).isEqualTo(5_000L);
    }

    @Test
    void launchSpeculative_noOriginalRequest_doesNotInventCopy() {
        var tracker = new CoordinatorTaskTracker();
        registerDurationInput(tracker, "to");
        tracker.assignWorker("to", "unissued-worker");
        var workers = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
        try (var dispatcher = new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), new NoOpTracingProvider())) {
            var launcher = new SpeculativeTaskLauncher();
            launcher.setDispatchDependencies(dispatcher, workers, tracker);
            var retrier = new TaskCancelRetrier(dispatcher, tracker);
            launcher.setCancelRetrier(retrier);

            assertThat(launcher.launchSpeculative("to", "unissued-worker", "duration-query")).isNull();
            assertThat(launcher.getOriginalTasksWithCopies()).isEmpty();
            assertThat(tracker.hasFailedTask("duration-query")).isFalse();
            assertThat(tracker.getTaskIds("duration-query")).containsExactly("to");
            assertThat(retrier.pendingCount()).isZero();
        }
    }

    private static void registerDurationInput(CoordinatorTaskTracker tracker, String taskId) {
        tracker.registerTask("duration-query", taskId);
        tracker.markScheduled("duration-query", taskId);
        tracker.updateTaskState("duration-query", taskId, "RUNNING");
        tracker.recordTaskStart(taskId, "1");
    }
}
