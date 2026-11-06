package org.pangrova.coordinator.speculation;

import org.pangrova.coordinator.query.adapter.scheduler.CoordinatorSpeculativeWorkerInventory;
import org.pangrova.coordinator.query.adapter.scheduler.CoordinatorSpeculativeTaskRegistry;
import org.pangrova.coordinator.query.adapter.scheduler.CoordinatorSpeculativeAttemptCanceller;
import org.pangrova.coordinator.query.adapter.scheduler.CoordinatorSpeculativeAttemptDispatcher;
import org.pangrova.scheduler.core.speculation.SpeculationConfig;
import org.pangrova.scheduler.core.speculation.SpeculativeStageHandler;
import org.pangrova.scheduler.core.speculation.SpeculativeTaskLauncher;
import org.pangrova.scheduler.core.speculation.StragglerDetector;

import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.scheduler.api.placement.TaskDuration;
import org.pangrova.coordinator.task.TaskCancelRetrier;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.observability.noop.NoOpTracingProvider;
import org.pangrova.persistence.coordination.memory.membership.InMemoryWorkerRegistry;

import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/** Pure duration/configuration inputs; these tests do not assert Worker or Native execution. */
class SpeculativeExecutionAlgorithmTest {
    @Test
    void disabled_noDetection() {
        var tracker = new CoordinatorTaskTracker();
        registerDurationInput(tracker, "t1");
        var launcher = new SpeculativeTaskLauncher();
        var handler = new SpeculativeStageHandler(SpeculationConfig.DISABLED,
                new StragglerDetector(), launcher, new CoordinatorSpeculativeTaskRegistry(tracker));

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
            launcher.setDispatchDependencies(
                    new CoordinatorSpeculativeAttemptDispatcher(dispatcher),
                    new CoordinatorSpeculativeWorkerInventory(workers),
                    new CoordinatorSpeculativeTaskRegistry(tracker));
            var retrier = new TaskCancelRetrier(dispatcher, tracker);
            launcher.setCancelRetrier(new CoordinatorSpeculativeAttemptCanceller(retrier));

            assertThat(launcher.launchSpeculative("to", "unissued-worker", "duration-query")).isNull();
            assertThat(launcher.getOriginalTasksWithCopies()).isEmpty();
            assertThat(tracker.hasFailedTask("duration-query")).isFalse();
            assertThat(tracker.getTaskIds("duration-query")).containsExactly("to");
            assertThat(retrier.pendingCount()).isZero();
        }
    }

    @Test
    void speculativeCopyIsNotAStagePeerOrANewStraggler() {
        var tracker = new CoordinatorTaskTracker();
        for (String peer : List.of("peer-1", "peer-2", "peer-3")) {
            registerDurationInput(tracker, peer, Instant.now().minusSeconds(1));
            tracker.updateTaskState("duration-query", peer, "FINISHED");
        }
        registerDurationInput(tracker, "original", Instant.now().minusSeconds(40));
        registerDurationInput(tracker, "spec-copy", Instant.now().minusSeconds(40));
        var considered = new AtomicReference<List<TaskDuration>>();
        var launched = new AtomicReference<String>();
        var detector = new StragglerDetector() {
            @Override public List<String> detectStragglers(List<TaskDuration> snapshots) {
                considered.set(List.copyOf(snapshots));
                return super.detectStragglers(snapshots);
            }
        };
        var launcher = new SpeculativeTaskLauncher() {
            @Override public boolean isSpeculative(String taskId) { return taskId.equals("spec-copy"); }
            @Override public String launchSpeculative(String taskId, String workerId, String queryId) {
                launched.set(taskId);
                return "spec-new";
            }
        };
        var handler = new SpeculativeStageHandler(new SpeculationConfig(true,
                SpeculationConfig.DEFAULT_DETECTION_INTERVAL_MS),
                detector, launcher, new CoordinatorSpeculativeTaskRegistry(tracker));

        handler.evaluateStragglers("duration-query", "1");

        assertThat(considered.get()).hasSize(4).noneMatch(snapshot -> snapshot.taskId().equals("spec-copy"));
        assertThat(launched.get()).isEqualTo("original");
    }

    @Test
    void launcherRejectsCopyOfCopyBeforeDispatch() {
        var launcher = new SpeculativeTaskLauncher() {
            @Override public boolean isSpeculative(String taskId) { return taskId.equals("spec-copy"); }
        };

        assertThat(launcher.launchSpeculative("spec-copy", "worker-1", "duration-query")).isNull();
    }

    private static void registerDurationInput(CoordinatorTaskTracker tracker, String taskId) {
        tracker.registerTask("duration-query", taskId);
        tracker.markScheduled("duration-query", taskId);
        tracker.updateTaskState("duration-query", taskId, "RUNNING");
        tracker.recordTaskStart(taskId, "1");
    }

    private static void registerDurationInput(CoordinatorTaskTracker tracker, String taskId, Instant start) {
        tracker.registerTask("duration-query", taskId);
        tracker.markScheduled("duration-query", taskId);
        tracker.updateTaskState("duration-query", taskId, "RUNNING");
        tracker.recordTaskStart(taskId, "1", start);
    }
}
