package org.pangrova.coordinator.property.query;

import net.jqwik.api.*;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.query.task.TaskStatusReceiver;
import org.pangrova.coordinator.query.task.TaskStatusReceiverContext;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.scheduler.core.placement.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.TestWorkerInfoBuilder;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.scheduler.core.scoring.WorkerPerformanceScorer;
import org.pangrova.observability.api.noop.NoOpTracingProvider;
import org.pangrova.client.core.error.ErrorDetail;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.rest.task.TaskAttemptDescriptor;
import org.pangrova.protocol.v2.CompletionState;
import org.pangrova.protocol.v2.CoverageReport;
import org.pangrova.protocol.v2.ErrorClass;
import org.pangrova.protocol.v2.ExecutionError;
import org.pangrova.protocol.v2.TaskKind;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskState;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.coordinator.query.QueryManagerContext;
import org.pangrova.coordinator.query.QueryStateMachine;
import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.coordinator.query.task.UnregisteredWorkerException;
import org.pangrova.planner.SqlCompiler;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property 23: Task status report propagation consistency.
 *
 * <p><b>Validates: Requirements 10.5</b></p>
 *
 * <p>For any valid TaskStatusReport, after processing by TaskStatusReceiver,
 * the CoordinatorTaskTracker state must be consistent with the reported state.</p>
 */
@Tag("Feature: full-pipeline-audit-fix")
@Tag("Property 23: Task status report propagation consistency")
class TaskStatusPropagationPropertyTest {

    private static final String WORKER_ID = "w-prop-test";

    @Property(tries = 10)
    void taskStatusReportUpdatesTrackerState(
            @ForAll("queryIds") String queryId,
            @ForAll("taskIds") String taskId,
            @ForAll("terminalStates") String state) {

        CoordinatorTaskTracker tracker = new CoordinatorTaskTracker();
        WorkerManager workerManager = new WorkerManager(20_000, 60_000, new org.pangrova.coordination.inmemory.InMemoryWorkerRegistry());
        workerManager.registerWorker(
                TestWorkerInfoBuilder.active(WORKER_ID, "localhost", 8080, 9090));

        QueryManager qm = buildMinimalQueryManager(tracker, workerManager);
        TaskStatusReceiver receiver = new TaskStatusReceiver(
                TaskStatusReceiverContext.withoutSpeculation(qm, tracker, workerManager));

        // Pre-register a QueryStateMachine so onAllTasksFinished/onTaskFailed can find it
        QueryStateMachine sm = new QueryStateMachine();
        qm.registerStateMachine(queryId, sm);
        sm.tryTransition(QueryStateMachine.State.PLANNING);
        sm.tryTransition(QueryStateMachine.State.SCHEDULING);
        sm.tryTransition(QueryStateMachine.State.RUNNING);

        // Register and schedule the task in the tracker
        tracker.registerTask(queryId, taskId);
        SubmitTaskRequest request = org.pangrova.coordinator.CanonicalTaskRequestTestFixture.request(
                taskId, queryId, 0, 0, TaskKind.TASK_KIND_READ, new byte[]{1});
        tracker.registerTaskRequest(taskId, request);
        tracker.registerTaskAttempt(taskId, TaskAttemptDescriptor.from(
                request.taskRequest().getAttempt()));
        tracker.markScheduled(queryId, taskId);

        // Transition through RUNNING before terminal state
        assertThat(receiver.onTaskStatus(buildReport(
                taskId, queryId, WORKER_ID, "RUNNING", null, request))).isTrue();

        // Send terminal state
        ErrorDetail error = "FAILED".equals(state)
                ? new ErrorDetail("INTERNAL", "INTERNAL", "simulated failure")
                : null;
        assertThat(receiver.onTaskStatus(buildReport(
                taskId, queryId, WORKER_ID, state, error, request))).isTrue();

        if ("FINISHED".equals(state)) {
            assertThat(tracker.allTasksFinished(queryId))
                    .as("All tasks should be finished for queryId=%s", queryId)
                    .isTrue();
            assertThat(sm.getState())
                    .as("QueryStateMachine should reach FINISHED")
                    .isEqualTo(QueryStateMachine.State.FINISHED);
        } else if ("FAILED".equals(state)) {
            assertThat(tracker.hasFailedTask(queryId))
                    .as("Should have failed task for queryId=%s", queryId)
                    .isTrue();
            assertThat(sm.getState())
                    .as("QueryStateMachine should reach FAILED")
                    .isEqualTo(QueryStateMachine.State.FAILED);
        }
    }

    @Property(tries = 10)
    void unregisteredWorkerReportIsRejected(
            @ForAll("queryIds") String queryId,
            @ForAll("taskIds") String taskId) {

        CoordinatorTaskTracker tracker = new CoordinatorTaskTracker();
        WorkerManager workerManager = new WorkerManager(20_000, 60_000, new org.pangrova.coordination.inmemory.InMemoryWorkerRegistry());

        QueryManager qm = buildMinimalQueryManager(tracker, workerManager);
        TaskStatusReceiver receiver = new TaskStatusReceiver(
                TaskStatusReceiverContext.withoutSpeculation(qm, tracker, workerManager));

        TaskStatusReport report = buildReport(
                taskId, queryId, "unknown-worker", "RUNNING", null, null);

        try {
            receiver.onTaskStatus(report);
            assertThat(false).as("Should throw UnregisteredWorkerException").isTrue();
        } catch (UnregisteredWorkerException e) {
            assertThat(e.getMessage()).contains("unknown-worker");
        }
    }

    @Provide
    Arbitrary<String> queryIds() {
        return Arbitraries.strings().alpha().ofMinLength(3).ofMaxLength(8)
                .map(s -> "q-" + s);
    }

    @Provide
    Arbitrary<String> taskIds() {
        return Arbitraries.strings().alpha().ofMinLength(3).ofMaxLength(8)
                .map(s -> "t-" + s);
    }

    @Provide
    Arbitrary<String> terminalStates() {
        return Arbitraries.of("FINISHED", "FAILED");
    }

    private static final java.util.concurrent.atomic.AtomicLong REPORT_SEQ = new java.util.concurrent.atomic.AtomicLong(0);

    private static TaskStatusReport buildReport(String taskId, String queryId,
                                                 String workerId, String state,
                                                 ErrorDetail error,
                                                 SubmitTaskRequest request) {
        TaskAttemptDescriptor attempt = request == null
                ? TaskAttemptDescriptor.of(1, 0)
                : TaskAttemptDescriptor.from(request.taskRequest().getAttempt());
        byte[] canonicalReport = request == null || "RUNNING".equals(state)
                ? null : terminalReport(request, taskId, state);
        return new TaskStatusReport(
                taskId, queryId, 0, 0, "COMPUTE",
                workerId, state, null, error, null,
                REPORT_SEQ.incrementAndGet(), System.currentTimeMillis(), null, null,
                attempt, canonicalReport);
    }

    private static byte[] terminalReport(SubmitTaskRequest request, String taskId, String state) {
        CoverageReport.Builder coverage = CoverageReport.newBuilder()
                .setExpectedWorkUnits(1);
        TaskReport.Builder report = TaskReport.newBuilder()
                .setReportOrigin(org.pangrova.protocol.v2.TaskReportOrigin
                        .TASK_REPORT_ORIGIN_NATIVE)
                .setAttempt(request.taskRequest().getAttempt());
        if ("FINISHED".equals(state)) {
            coverage.setCompletedWorkUnits(1)
                    .setCoverageRatio(1.0)
                    .setCompletionState(CompletionState.COMPLETION_STATE_COMPLETE);
            report.setState(TaskState.TASK_STATE_FINISHED);
        } else {
            coverage.addFailedWorkUnitIds(taskId)
                    .setCompletionState(CompletionState.COMPLETION_STATE_PARTIAL);
            report.setState(TaskState.TASK_STATE_FAILED)
                    .setError(ExecutionError.newBuilder()
                            .setErrorClass(ErrorClass.ERROR_CLASS_INTERNAL)
                            .setCode("TEST_FAILURE")
                            .setMessage("simulated failure"));
        }
        return report.setCoverage(coverage).build().toByteArray();
    }

    private static QueryManager buildMinimalQueryManager(
            CoordinatorTaskTracker tracker, WorkerManager workerManager) {
        var dispatcher = new RemoteTaskDispatcher(
                CircuitBreakerPolicy.noOp(), new NoOpTracingProvider());
        var scheduler = new TaskScheduler(new WorkerPerformanceScorer());
        var deps = QueryDispatcherContext.withDefaults(
                new SqlCompiler(), null, null, null, dispatcher, scheduler,
                workerManager, tracker, null, null, null, 0L);
        var queryDispatcher = new QueryDispatcher(
                deps, null, null);
        return new QueryManager(
                QueryManagerContext.minimal(queryDispatcher, new NoOpTracingProvider()));
    }
}
