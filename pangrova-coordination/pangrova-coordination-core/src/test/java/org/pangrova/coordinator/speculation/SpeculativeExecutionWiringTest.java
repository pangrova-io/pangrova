package org.pangrova.coordinator.speculation;

import org.pangrova.scheduler.core.speculation.SpeculationConfig;
import org.pangrova.scheduler.core.speculation.SpeculativeStageHandler;
import org.pangrova.scheduler.core.speculation.SpeculativeTaskLauncher;
import org.pangrova.scheduler.core.speculation.StragglerDetector;
import org.pangrova.scheduler.core.speculation.DuplicateTaskResolver;
import org.pangrova.coordinator.adapter.scheduler.SpeculationAdapters;
import org.pangrova.scheduler.api.placement.TaskDuration;
import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.coordinator.query.QueryManagerContext;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.query.execution.dispatch.DispatchResult;
import org.pangrova.coordinator.query.task.TaskStatusReceiver;
import org.pangrova.coordinator.query.task.TaskStatusReceiverContext;
import org.pangrova.coordinator.query.task.write.NativeWriteCommitTracker;
import org.pangrova.coordinator.scheduler.SchedulerCancelResult;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.coordinator.scheduler.TaskDispatcher;
import org.pangrova.scheduler.core.placement.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.task.TaskStateMachine;
import org.pangrova.scheduler.api.placement.HealthState;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.scheduler.core.scoring.WorkerPerformanceScorer;
import org.pangrova.observability.sdk.noop.NoOpTracingProvider;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.client.core.protocol.ResultEndpoint;
import org.pangrova.protocol.rest.data.WriteResultDescriptor;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.common.value.MemoryPressure;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicInteger;
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
        handler = new SpeculativeStageHandler(new SpeculationConfig(true, 5_000L),
                new StragglerDetector(), launcher, SpeculationAdapters.taskRegistry(tracker));
        wm = new WorkerManager(20_000, 60_000, new org.pangrova.persistence.memory.coordination.InMemoryWorkerRegistry());
        wm.registerWorker(w(WA, "ha"));
        wm.registerWorker(w(WB, "hb"));
        launcher.setDispatchDependencies(
                SpeculationAdapters.dispatcher(rec),
                SpeculationAdapters.workerInventory(wm),
                SpeculationAdapters.taskRegistry(tracker));
        launcher.setCancelRetrier(SpeculationAdapters.canceller(
                new org.pangrova.coordinator.task.TaskCancelRetrier(rec, tracker)));
        tracker.addExecutionDrainListener(launcher::executionDrained);
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
        receiver.onTaskStatus(fin(si, WB));
        assertThat(tracker.getTaskRequest("ts")).isNotNull();
        receiver.onTaskStatus(fin("ts", WA));
        assertThat(resolver.getWinner(QID, SID, 0))
                .as("winner stays fenced until query retirement").hasValue(si);
        assertThat(tracker.getTaskIds(QID)).doesNotContain("ts");
        assertThat(tracker.hasFailedTask(QID)).isFalse();
        assertThat(rec.last).isEqualTo("ts");
    }
    @Test void disabled_noDetection() {
        var d = new SpeculativeStageHandler(SpeculationConfig.DISABLED,
                new StragglerDetector(), launcher, SpeculationAdapters.taskRegistry(tracker));
        run("t1");
        d.evaluateStragglers(QID, SID);
        assertThat(launcher.getOriginalTasksWithCopies()).isEmpty();
    }
    @Test void originalWins_specCancelled() {
        run("to");
        tracker.assignWorker("to", WA);
        String si = launcher.launchSpeculative("to", WA, QID);
        receiver.onTaskStatus(fin("to", WA));
        assertThat(tracker.getTaskRequest(si)).isNotNull();
        receiver.onTaskStatus(fin(si, WB));
        assertThat(resolver.getWinner(QID, "1", 0))
                .as("winner stays fenced until query retirement").hasValue("to");
        assertThat(rec.last).isEqualTo(si);
        assertThat(tracker.getTaskIds(QID)).doesNotContain(si);
        assertThat(tracker.hasFailedTask(QID)).isFalse();
    }
    @Test void speculativeWinnerCompletesOriginalDagSlotWithoutFailingQuery() {
        AtomicReference<String> completedTask = new AtomicReference<>();
        AtomicReference<String> failedTask = new AtomicReference<>();
        tracker.setTaskFailureCallback((queryId, taskId) -> failedTask.set(taskId));
        var stageCallback = new org.pangrova.coordinator.query.execution.dispatch.dag.StageCompletionCallback() {
            @Override public void onTaskTerminal(
                    String queryId, String taskId, String state,
                    org.pangrova.client.core.error.ErrorDetail error) {
                completedTask.set(taskId);
            }

            @Override public void onQueryTerminal(
                    String queryId, java.util.function.Consumer<Boolean> listener) {
                listener.accept(true);
            }
        };
        receiver = new TaskStatusReceiver(new TaskStatusReceiverContext(
                qm(), tracker, wm, launcher, resolver, rec,
                null, null, null, stageCallback, null, null));
        run("to");
        tracker.assignWorker("to", WA);
        String copy = launcher.launchSpeculative("to", WA, QID);

        assertThat(receiver.onTaskStatus(fin(copy, WB))).isTrue();
        assertThat(completedTask.get()).isEqualTo("to");
        assertThat(failedTask.get()).isNull();
        assertThat(tracker.getTaskRequest("to")).isNotNull();
        receiver.onTaskStatus(fin("to", WA));
        assertThat(tracker.getTaskIds(QID)).doesNotContain("to");
        assertThat(tracker.getTaskIds(QID)).isEmpty();
    }
    @Test void failedCopyDoesNotFailOriginalLogicalTask() {
        String copy = runningPair();
        AtomicInteger failures = new AtomicInteger();
        tracker.setTaskFailureCallback((queryId, taskId) -> failures.incrementAndGet());

        assertThat(receiver.onTaskStatus(failed(copy, WB))).isTrue();
        assertThat(tracker.hasFailedTask(QID)).isFalse();
        assertThat(receiver.onTaskStatus(fin("to", WA))).isTrue();
        assertThat(tracker.allTasksFinished(QID)).isTrue();
        assertThat(failures.get()).isZero();
    }
    @Test void failedOriginalCanBeReplacedBySuccessfulCopy() {
        String copy = runningPair();
        AtomicInteger failures = new AtomicInteger();
        tracker.setTaskFailureCallback((queryId, taskId) -> failures.incrementAndGet());

        assertThat(receiver.onTaskStatus(failed("to", WA))).isTrue();
        assertThat(tracker.hasFailedTask(QID)).isFalse();
        assertThat(receiver.onTaskStatus(fin(copy, WB))).isTrue();
        assertThat(tracker.getTaskIds(QID)).isEmpty();
        assertThat(failures.get()).isZero();
    }
    @Test void bothFailedAttemptsFailTheirLogicalTaskOnce() {
        String copy = runningPair();
        AtomicInteger failures = new AtomicInteger();
        tracker.setTaskFailureCallback((queryId, taskId) -> failures.incrementAndGet());

        assertThat(receiver.onTaskStatus(failed(copy, WB))).isTrue();
        assertThat(receiver.onTaskStatus(failed("to", WA))).isTrue();
        assertThat(tracker.hasFailedTask(QID)).isTrue();
        assertThat(tracker.getTaskIds(QID)).doesNotContain(copy);
        assertThat(failures.get()).isEqualTo(1);
    }
    @RepeatedTest(20) void concurrentSpeculativeLoserDoesNotContributeWriteFragment() throws Exception {
        var fragments = new NativeWriteCommitTracker();
        fragments.registerWriteContext(
                QID,
                "catalog",
                new org.pangrova.spi.connector.metadata.ConnectorOutputTableHandle() { },
                new org.pangrova.coordinator.query.statement.StatementConnectorSession(
                        "catalog", "default"),
                new byte[]{42},
                org.pangrova.coordinator.testinfra.CoordinatorWriteReceiptFixtures.receipt(),
                new org.pangrova.coordinator.query.task.write.WriteAllocationIntent("INSERT", java.util.Map.of()), null);
        CountDownLatch bothAtElection = new CountDownLatch(2);
        var concurrentResolver = new DuplicateTaskResolver() {
            @Override public boolean onTaskComplete(
                    String queryId, String taskId, String stageId, int partitionId) {
                bothAtElection.countDown();
                try {
                    if (!bothAtElection.await(5, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("both attempts did not reach election");
                    }
                } catch (InterruptedException failure) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("election interrupted", failure);
                }
                return super.onTaskComplete(queryId, taskId, stageId, partitionId);
            }
        };
        receiver = new TaskStatusReceiver(new TaskStatusReceiverContext(
                qm(), tracker, wm, launcher, concurrentResolver, rec,
                null, null, fragments, null, null, null));
        run("to");
        tracker.assignWorker("to", WA);
        String copy = launcher.launchSpeculative("to", WA, QID);

        var executor = Executors.newFixedThreadPool(2);
        try {
            var original = CompletableFuture.supplyAsync(
                    () -> receiver.onTaskStatus(finWithFragment("to", WA, (byte) 1)), executor);
            var duplicate = CompletableFuture.supplyAsync(
                    () -> receiver.onTaskStatus(finWithFragment(copy, WB, (byte) 2)), executor);
            boolean originalAccepted = original.get(10, TimeUnit.SECONDS);
            boolean duplicateAccepted = duplicate.get(10, TimeUnit.SECONDS);
            assertThat(originalAccepted || duplicateAccepted).isTrue();
            assertThat(fragments.peekWriteState(QID).fragmentCount()).isEqualTo(1);
        } finally {
            executor.shutdownNow();
        }
    }
    @Test void launchSpeculative_withDispatchDeps_actuallyDispatchesToDifferentWorker() {
        // Wire dispatch dependencies so the launcher actually dispatches
        launcher.setDispatchDependencies(
                SpeculationAdapters.dispatcher(rec),
                SpeculationAdapters.workerInventory(wm),
                SpeculationAdapters.taskRegistry(tracker));

        // Register the original task and store its request in the tracker
        Instant fastStart = Instant.now().minusSeconds(1);
        Instant slowStart = Instant.now().minusSeconds(60);
        for (String id : List.of("t1", "t2", "t3")) runAt(id, fastStart);
        tracker.registerTask(QID, "ts");
        // Store the original task request in the tracker (as the dispatch path would do)
        SubmitTaskRequest originalReq =
                org.pangrova.coordinator.CanonicalTaskRequestTestFixture.request(
                        "ts", QID, 1, 0,
                        org.pangrova.protocol.v2.TaskKind.TASK_KIND_READ,
                        "plan".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        originalReq = rec.bindWorkerIdentity(originalReq, wm.getWorker(WA));
        tracker.registerTaskRequest("ts", originalReq);
        tracker.markScheduled(QID, "ts");
        tracker.updateTaskState(QID, "ts", "RUNNING");
        tracker.recordTaskStart("ts", SID, slowStart);
        tracker.assignWorker("ts", WA);
        tracker.setDeadline("ts", Instant.now().plusSeconds(300));

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
        assertThat(tracker.getTaskRequest(specTaskId).taskRequest().getAttempt().getWorkerExecutionIdentity())
                .isEqualTo(wm.getWorker(WB).workerExecutionIdentity().toProto());
    }

    @Test void launchSpeculative_noOriginalRequest_doesNotInventCopy() {
        launcher.setDispatchDependencies(
                SpeculationAdapters.dispatcher(rec),
                SpeculationAdapters.workerInventory(wm),
                SpeculationAdapters.taskRegistry(tracker));
        tracker.registerTask(QID, "to");
        tracker.markScheduled(QID, "to");
        tracker.updateTaskState(QID, "to", "RUNNING");
        tracker.assignWorker("to", WA);
        String specId = launcher.launchSpeculative("to", WA, QID);
        assertThat(specId).isNull();
        assertThat(launcher.getOriginalTasksWithCopies()).isEmpty();
        assertThat(tracker.hasFailedTask(QID)).isFalse();
    }

    @Test void failedSpeculativeDispatchDoesNotFailOriginalQuery() {
        var rejectingDispatcher = new Rec() {
            @Override public DispatchResult dispatch(SubmitTaskRequest request, WorkerInfo worker) {
                throw new IllegalStateException("dispatch unavailable");
            }
        };
        launcher.setDispatchDependencies(
                SpeculationAdapters.dispatcher(rejectingDispatcher),
                SpeculationAdapters.workerInventory(wm),
                SpeculationAdapters.taskRegistry(tracker));
        launcher.setCancelRetrier(SpeculationAdapters.canceller(
                new org.pangrova.coordinator.task.TaskCancelRetrier(rejectingDispatcher, tracker)));
        run("to");
        tracker.assignWorker("to", WA);
        AtomicInteger failures = new AtomicInteger();
        tracker.setTaskFailureCallback((queryId, taskId) -> failures.incrementAndGet());

        assertThat(launcher.launchSpeculative("to", WA, QID)).isNull();
        assertThat(launcher.getOriginalTasksWithCopies()).contains("to");
        assertThat(tracker.getTaskIds(QID)).contains("to");
        assertThat(tracker.getTaskIds(QID).stream().filter(id -> id.startsWith("spec-")))
                .as("ambiguous dispatch remains tracked until actual Worker drain")
                .hasSize(1);
        String copy = tracker.getTaskIds(QID).stream()
                .filter(id -> id.startsWith("spec-")).findFirst().orElseThrow();
        assertThat(receiver.onTaskStatus(fin(copy, WB))).isTrue();
        assertThat(launcher.getOriginalTasksWithCopies()).isEmpty();
        assertThat(tracker.hasFailedTask(QID)).isFalse();
        assertThat(failures.get()).isZero();
        assertThat(rejectingDispatcher.last).startsWith("spec-");
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
        String si = launcher.launchSpeculative("to", WA, QID);
        receiver.onTaskStatus(fin(si, WB));
        assertThat(tracker.getTimedOutTasks(Instant.now().plusSeconds(600))).containsKey("to");
        receiver.onTaskStatus(fin("to", WA));
        assertThat(tracker.getTimedOutTasks(Instant.now().plusSeconds(600))).doesNotContainKey("to");
    }
    private void run(String id) {
        tracker.registerTask(QID, id);
        registerRequest(id, org.pangrova.protocol.rest.task.TaskAttemptDescriptor.of(1, 0));
        tracker.markScheduled(QID, id);
        tracker.updateTaskState(QID, id, "RUNNING");
        tracker.recordTaskStart(id, SID);
    }
    private void runAt(String id, Instant startTime) {
        tracker.registerTask(QID, id);
        registerRequest(id, org.pangrova.protocol.rest.task.TaskAttemptDescriptor.of(1, 0));
        tracker.markScheduled(QID, id);
        tracker.updateTaskState(QID, id, "RUNNING");
        tracker.recordTaskStart(id, SID, startTime);
    }
    private String runningPair() {
        run("to");
        tracker.assignWorker("to", WA);
        String copy = launcher.launchSpeculative("to", WA, QID);
        return copy;
    }
    private void registerRequest(
            String taskId,
            org.pangrova.protocol.rest.task.TaskAttemptDescriptor attempt) {
        var builder = org.pangrova.coordinator.CanonicalTaskRequestTestFixture.builder(
                taskId, QID, 1, 0,
                org.pangrova.protocol.v2.TaskKind.TASK_KIND_READ,
                "plan".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        var canonical = builder
                .setAttempt(builder.getAttempt().toBuilder()
                        .setPlanEpoch(attempt.planEpoch())
                        .setAttemptId(attempt.attemptId())
                        .setFencingToken(attempt.fencingToken()))
                .build();
        tracker.registerTaskRequest(taskId, SubmitTaskRequest.from(canonical));
    }

    private TaskStatusReport fin(String tid, String wid) {
        SubmitTaskRequest request = tracker.getTaskRequest(tid);
        var status = org.pangrova.protocol.v2.TaskReport.newBuilder()
                .setReportOrigin(org.pangrova.protocol.v2.TaskReportOrigin
                        .TASK_REPORT_ORIGIN_NATIVE)
                .setAttempt(request.taskRequest().getAttempt())
                .setExecutionDrainState(org.pangrova.protocol.v2.ExecutionDrainState
                        .EXECUTION_DRAIN_STATE_NATIVE_OWNER_RELEASED)
                .setState(org.pangrova.protocol.v2.TaskState.TASK_STATE_FINISHED)
                .setCoverage(org.pangrova.protocol.v2.CoverageReport.newBuilder()
                        .setCompletionState(
                                org.pangrova.protocol.v2.CompletionState
                                        .COMPLETION_STATE_COMPLETE)
                        .setExpectedWorkUnits(1)
                        .setCompletedWorkUnits(1)
                        .setCoverageRatio(1.0))
                .build();
        return new TaskStatusReport(
                tid, QID, 1, 0, "COMPUTE", wid, "FINISHED",
                null, null, null, 1L, System.currentTimeMillis(), null, null,
                org.pangrova.protocol.rest.task.TaskAttemptDescriptor.from(
                        request.taskRequest().getAttempt()), status.toByteArray());
    }
    private TaskStatusReport finWithFragment(String taskId, String workerId, byte fragment) {
        TaskStatusReport status = fin(taskId, workerId);
        return new TaskStatusReport(
                status.taskId(), status.queryId(), status.stageId(), status.partitionId(),
                status.taskType(), status.workerId(), status.state(), status.stats(),
                status.error(), new WriteResultDescriptor(List.of(), 1, 1,
                        new byte[]{fragment}), status.reportSequence(), status.timestampMs(),
                status.errorCode(), status.errorMetadata(), status.attempt(),
                status.canonicalTaskReport());
    }
    private TaskStatusReport failed(String taskId, String workerId) {
        SubmitTaskRequest request = tracker.getTaskRequest(taskId);
        var status = org.pangrova.protocol.v2.TaskReport.newBuilder()
                .setReportOrigin(org.pangrova.protocol.v2.TaskReportOrigin
                        .TASK_REPORT_ORIGIN_NATIVE)
                .setAttempt(request.taskRequest().getAttempt())
                .setExecutionDrainState(org.pangrova.protocol.v2.ExecutionDrainState
                        .EXECUTION_DRAIN_STATE_NATIVE_OWNER_RELEASED)
                .setState(org.pangrova.protocol.v2.TaskState.TASK_STATE_FAILED)
                .setCoverage(org.pangrova.protocol.v2.CoverageReport.newBuilder()
                        .setCompletionState(org.pangrova.protocol.v2.CompletionState
                                .COMPLETION_STATE_PARTIAL)
                        .setExpectedWorkUnits(1)
                        .addFailedWorkUnitIds("task-1"))
                .setError(org.pangrova.protocol.v2.ExecutionError.newBuilder()
                        .setErrorClass(org.pangrova.protocol.v2.ErrorClass
                                .ERROR_CLASS_INTERNAL)
                        .setCode("NATIVE_TASK_FAILED")
                        .setFailedComponent("native-execution")
                        .setMessage("attempt failed"))
                .build();
        return new TaskStatusReport(
                taskId, QID, 1, 0, "COMPUTE", workerId, "FAILED",
                null, new org.pangrova.client.core.error.ErrorDetail(
                        "NATIVE_TASK_FAILED", "NativeTaskFailure", "attempt failed"),
                null, 1L, System.currentTimeMillis(), null, null,
                org.pangrova.protocol.rest.task.TaskAttemptDescriptor.from(
                        request.taskRequest().getAttempt()), status.toByteArray());
    }
    private QueryManager qm() {
        var s = new TaskScheduler(new WorkerPerformanceScorer());
        var h = new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), new NoOpTracingProvider());
        var d = QueryDispatcherContext.withDefaults(new SqlCompiler(), null, null, null, h, s, wm, tracker, null, null, null, 0L);
        return new QueryManager(QueryManagerContext.minimal(new QueryDispatcher(d, null, null), new NoOpTracingProvider()));
    }
    private static WorkerInfo w(String id, String host) {
        return new WorkerInfo(id, host, 8080, 8081, 0, HealthState.ACTIVE,
                MemoryPressure.NONE, 0.5, 0, 1024L * 1024 * 1024,
                Instant.now(), 1.0, null, 2, null, List.of(), null,
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.canonicalInventory(),
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.executionIdentity(id));
    }
    static class Rec implements TaskDispatcher {
        volatile String last;
        @Override public DispatchResult dispatch(SubmitTaskRequest r, WorkerInfo w) { return new DispatchResult(w.host(), r.taskRequest().getAttempt().getTaskId(), new ResultEndpoint(w.host(), w.httpPort(), "t", "grpc")); }
        @Override public SchedulerCancelResult cancelTask(SubmitTaskRequest request, String h, int p) {
            last = request.taskRequest().getAttempt().getTaskId();
            return SchedulerCancelResult.failed("Recording fixture has no actual execution-drain owner", false);
        }
    }
}
