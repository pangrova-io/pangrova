package org.pangrova.coordinator.speculation;
import org.pangrova.coordinator.task.TaskDuration;
import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.coordinator.query.QueryManagerContext;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.query.execution.dispatch.DispatchResult;
import org.pangrova.coordinator.query.task.TaskStatusReceiver;
import org.pangrova.coordinator.query.task.TaskStatusReceiverContext;
import org.pangrova.coordinator.scheduler.SchedulerCancelResult;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.coordinator.scheduler.TaskDispatcher;
import org.pangrova.coordinator.scheduler.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.task.TaskStateMachine;
import org.pangrova.coordinator.worker.HealthState;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.coordinator.worker.WorkerPerformanceScorer;
import org.pangrova.observability.noop.NoOpTracingProvider;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.protocol.rest.query.ResultEndpoint;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.resource.MemoryPressure;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import static org.assertj.core.api.Assertions.assertThat;
/** Validates: Requirements 1.45, 1.46, 2.45, 2.46 */
class SpeculativeExecutionWiringTest {
    private static final String QID = "q-spec-1";
    private static final String SID = "1";
    private static final String WA = "worker-a";
    private static final String WB = "worker-b";
    private CoordinatorTaskTracker tracker;
    private SpeculativeTaskLauncher launcher;
    private DuplicateTaskResolver resolver;
    private SpeculativeStageHandler handler;
    private TaskStatusReceiver receiver;
    private Rec rec;
    private WorkerManager wm;
    @BeforeEach void setUp() {
        tracker = new CoordinatorTaskTracker();
        launcher = new SpeculativeTaskLauncher();
        resolver = new DuplicateTaskResolver();
        rec = new Rec();
        handler = new SpeculativeStageHandler(new SpeculationConfig(true, 5_000L), new StragglerDetector(), launcher, tracker);
        wm = new WorkerManager(20_000, 60_000, new org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry());
        wm.registerWorker(w(WA, "ha"));
        wm.registerWorker(w(WB, "hb"));
        receiver = new TaskStatusReceiver(new TaskStatusReceiverContext(            qm(), tracker, wm, launcher, resolver, rec, null, null, null, null, null, null));
    }
    @Test void slowTask_specLaunched_winnerKept_loserCancelled() {
        // t1-t3 start 1s ago (fast tasks); ts starts 60s ago (straggler: duration >> median*1.5)
        Instant fastStart = Instant.now().minusSeconds(1);
        Instant slowStart = Instant.now().minusSeconds(60);
        for (String id : List.of("t1", "t2", "t3")) runAt(id, fastStart);
        runAt("ts", slowStart);
        tracker.assignWorker("ts", WA);
        tracker.setDeadline("ts", Instant.now().plusSeconds(300));
        for (String id : List.of("t1", "t2", "t3")) tracker.updateTaskState(QID, id, "FINISHED");
        handler.evaluateStragglers(QID, SID);
        assertThat(launcher.getOriginalTasksWithCopies()).contains("ts");
        String si = launcher.getSpeculativeTaskId("ts").orElseThrow();
        tracker.registerTask(QID, si);
        tracker.markScheduled(QID, si);
        tracker.updateTaskState(QID, si, "RUNNING");
        tracker.assignWorker(si, WB);
        receiver.onTaskStatus(fin(si, WB));
        assertThat(resolver.getWinner(SID, 0)).hasValue(si);
        assertThat(tracker.getTaskState(QID, "ts")).isEqualTo(TaskStateMachine.State.FAILED);
        assertThat(rec.last).isEqualTo("ts");
    }
    @Test void disabled_noDetection() {
        var d = new SpeculativeStageHandler(SpeculationConfig.DISABLED, new StragglerDetector(), launcher, tracker);
        run("t1");
        d.evaluateStragglers(QID, SID);
        assertThat(launcher.getOriginalTasksWithCopies()).isEmpty();
    }
    @Test void originalWins_specCancelled() {
        run("to");
        tracker.assignWorker("to", WA);
        String si = launcher.launchSpeculative("to", WA);
        tracker.registerTask(QID, si);
        tracker.markScheduled(QID, si);
        tracker.updateTaskState(QID, si, "RUNNING");
        tracker.assignWorker(si, WB);
        receiver.onTaskStatus(fin("to", WA));
        assertThat(resolver.getWinner("1", 0)).hasValue("to");
        assertThat(rec.last).isEqualTo(si);
    }
    @Test void launchSpeculative_withDispatchDeps_actuallyDispatchesToDifferentWorker() {
        // Wire dispatch dependencies so the launcher actually dispatches
        launcher.setDispatchDependencies(rec, wm, tracker);

        // Register the original task and store its request in the tracker
        Instant fastStart = Instant.now().minusSeconds(1);
        Instant slowStart = Instant.now().minusSeconds(60);
        for (String id : List.of("t1", "t2", "t3")) runAt(id, fastStart);
        runAt("ts", slowStart);
        tracker.assignWorker("ts", WA);
        tracker.setDeadline("ts", Instant.now().plusSeconds(300));
        // Store the original task request in the tracker (as the dispatch path would do)
        SubmitTaskRequest originalReq = new SubmitTaskRequest(
                "ts", QID, 1, 0, "COMPUTE", "planBase64==", List.of(), List.of(),
                null, java.util.Map.of(), null, null,
                null, null, null, null, null, null, null);
        tracker.registerTaskRequest("ts", originalReq);

        for (String id : List.of("t1", "t2", "t3")) tracker.updateTaskState(QID, id, "FINISHED");

        // Evaluate stragglers -- this should trigger actual dispatch
        handler.evaluateStragglers(QID, SID);

        // Verify the speculative task was tracked
        assertThat(launcher.getOriginalTasksWithCopies()).contains("ts");
        String specTaskId = launcher.getSpeculativeTaskId("ts").orElseThrow();

        // Verify the speculative task was registered in the tracker
        assertThat(tracker.getTaskState(QID, specTaskId))
                .as("Speculative task should be in RUNNING state after dispatch")
                .isEqualTo(TaskStateMachine.State.RUNNING);

        // Verify the speculative task was assigned to a different worker
        String assignedWorker = tracker.getWorkerForTask(specTaskId);
        assertThat(assignedWorker)
                .as("Speculative task should be assigned to a different worker than the original")
                .isNotEqualTo(WA);
        assertThat(assignedWorker).isEqualTo(WB);
    }

    @Test void launchSpeculative_noOriginalRequest_gracefullyDegrades() {
        // Wire dispatch dependencies but don't store any task request
        launcher.setDispatchDependencies(rec, wm, tracker);
        run("to");
        tracker.assignWorker("to", WA);
        // Do NOT store a task request -- launcher should handle gracefully
        String specId = launcher.launchSpeculative("to", WA, QID);

        // Bookkeeping should still work
        assertThat(launcher.isSpeculative(specId)).isTrue();
        assertThat(launcher.getOriginalTaskId(specId)).hasValue("to");
    }

    @Test void durationSnapshots() {
        run("t1");
        run("t2");
        tracker.updateTaskState(QID, "t2", "FINISHED");
        var s = tracker.getTaskDurationSnapshots(QID, SID, Set.of(), Instant.now());
        assertThat(s).hasSize(2);
        assertThat(s.stream().filter(TaskDuration::completed).count()).isEqualTo(1);
    }
    @Test void configDefaults() {
        assertThat(SpeculationConfig.DISABLED.enabled()).isFalse();
        assertThat(SpeculationConfig.DISABLED.detectionIntervalMs()).isEqualTo(5_000L);
    }
    @Test void resourceCleanup() {
        run("to");
        tracker.assignWorker("to", WA);
        tracker.setDeadline("to", Instant.now().plusSeconds(300));
        String si = launcher.launchSpeculative("to", WA);
        tracker.registerTask(QID, si);
        tracker.markScheduled(QID, si);
        tracker.updateTaskState(QID, si, "RUNNING");
        tracker.assignWorker(si, WB);
        receiver.onTaskStatus(fin(si, WB));
        assertThat(tracker.getTimedOutTasks(Instant.now().plusSeconds(600))).doesNotContainKey("to");
    }
    private void run(String id) {
        tracker.registerTask(QID, id);
        tracker.markScheduled(QID, id);
        tracker.updateTaskState(QID, id, "RUNNING");
        tracker.recordTaskStart(id, SID);
    }
    private void runAt(String id, Instant startTime) {
        tracker.registerTask(QID, id);
        tracker.markScheduled(QID, id);
        tracker.updateTaskState(QID, id, "RUNNING");
        tracker.recordTaskStart(id, SID, startTime);
    }
    private static TaskStatusReport fin(String tid, String wid) {
        return new TaskStatusReport(tid, QID, 1, 0, "COMPUTE", wid, "FINISHED", null, null, null, 1L, System.currentTimeMillis());
    }
    private QueryManager qm() {
        var s = new TaskScheduler(new WorkerPerformanceScorer());
        var h = new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), new NoOpTracingProvider());
        var d = QueryDispatcherContext.withDefaults(new SqlCompiler(), null, null, null, h, s, wm, tracker, null, null, null, null, 0L);
        return new QueryManager(QueryManagerContext.minimal(new QueryDispatcher(d, null, null), new NoOpTracingProvider()));
    }
    private static WorkerInfo w(String id, String host) {
        return new WorkerInfo(id, host, 8080, 8081, 0, HealthState.ACTIVE, MemoryPressure.NONE, 0.5, 0, 1024L * 1024 * 1024, Instant.now(), 1.0, null, 1, List.of(), null, List.of());
    }
    static class Rec implements TaskDispatcher {
        volatile String last;
        @Override public DispatchResult dispatch(SubmitTaskRequest r, WorkerInfo w) { return new DispatchResult(w.host(), r.taskId(), new ResultEndpoint(w.host(), w.httpPort(), "t", "grpc")); }
        @Override public SchedulerCancelResult cancelTask(String tid, String h, int p) { last = tid; return SchedulerCancelResult.cancelled(); }
    }
}
