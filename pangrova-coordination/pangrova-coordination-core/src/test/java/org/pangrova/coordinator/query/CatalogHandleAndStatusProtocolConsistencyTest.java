package org.pangrova.coordinator.query;

import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.CanonicalTaskRequestTestFixture;
import org.pangrova.coordinator.query.task.TaskStatusReceiver;
import org.pangrova.coordinator.query.task.TaskStatusReceiverContext;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.task.TaskStateMachine;
import org.pangrova.coordinator.worker.TestWorkerInfoBuilder;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.metadata.api.catalog.CatalogHandle;
import org.pangrova.metadata.api.catalog.CatalogType;
import org.pangrova.metadata.core.catalog.handle.connector.ConnectorCatalogHandle;
import org.pangrova.metadata.core.catalog.handle.federation.FederationCatalogHandle;
import org.pangrova.metadata.core.catalog.handle.internal.InternalCatalogHandle;
import org.pangrova.client.core.error.ErrorDetail;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.protocol.rest.task.TaskStatusReportResponse;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.coordinator.query.task.UnregisteredWorkerException;
import org.pangrova.coordination.inmemory.InMemoryWorkerRegistry;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Modifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies CatalogHandle sealed hierarchy consistency and
 * TaskStatusReporter (Worker) / TaskStatusReceiver (Coordinator) protocol consistency.
 *
 * Validates: Requirements 5.6, 5.7
 */
class CatalogHandleAndStatusProtocolConsistencyTest {

    // ── CatalogHandle open contract and provider implementations ──

    @Test
    void catalogHandleIsAnOpenContract() {
        assertThat(CatalogHandle.class.isSealed()).isFalse();
        assertThat(CatalogHandle.class.getPermittedSubclasses()).isNull();
    }

    @Test
    void catalogHandleImplementationsAreFinalAndPerType() {
        assertThat(Modifier.isFinal(ConnectorCatalogHandle.class.getModifiers())).isTrue();
        assertThat(Modifier.isFinal(FederationCatalogHandle.class.getModifiers())).isTrue();
        assertThat(Modifier.isFinal(InternalCatalogHandle.class.getModifiers())).isTrue();
    }

    @Test
    void catalogTypeEnumAlignedWithHandleImplementations() {
        CatalogType[] types = CatalogType.values();
        assertThat(types).hasSize(3);
        assertThat(types).containsExactlyInAnyOrder(
                CatalogType.INTERNAL, CatalogType.CONNECTOR, CatalogType.FEDERATION);
    }

    // ── TaskStatus protocol: Worker state strings accepted by Coordinator ──

    @Test
    void allWorkerStateStringsAcceptedByCoordinatorTracker() {
        String[] workerStates = {"RUNNING", "FLUSHING", "FINISHED", "FAILED", "CANCELLED"};

        CoordinatorTaskTracker tracker = new CoordinatorTaskTracker();
        for (String state : workerStates) {
            String queryId = "q-" + state;
            String taskId = "t-" + state;
            tracker.registerTask(queryId, taskId);
            tracker.markScheduled(queryId, taskId);
            tracker.updateTaskState(queryId, taskId, state);
        }
    }

    @Test
    void runningStateMapsToRunning() {
        CoordinatorTaskTracker tracker = scheduledTask("q1", "t1");
        tracker.updateTaskState("q1", "t1", "RUNNING");
        assertEquals(TaskStateMachine.State.RUNNING, tracker.getTaskState("q1", "t1"));
    }

    @Test
    void flushingStateMapsToRunning() {
        CoordinatorTaskTracker tracker = scheduledTask("q1", "t1");
        tracker.updateTaskState("q1", "t1", "FLUSHING");
        assertEquals(TaskStateMachine.State.RUNNING, tracker.getTaskState("q1", "t1"));
    }

    @Test
    void finishedStateMapsToFinished() {
        CoordinatorTaskTracker tracker = scheduledTask("q1", "t1");
        tracker.updateTaskState("q1", "t1", "RUNNING");
        tracker.updateTaskState("q1", "t1", "FINISHED");
        assertEquals(TaskStateMachine.State.FINISHED, tracker.getTaskState("q1", "t1"));
    }

    @Test
    void failedStateMapsToFailed() {
        CoordinatorTaskTracker tracker = scheduledTask("q1", "t1");
        tracker.updateTaskState("q1", "t1", "RUNNING");
        tracker.updateTaskState("q1", "t1", "FAILED");
        assertEquals(TaskStateMachine.State.FAILED, tracker.getTaskState("q1", "t1"));
    }

    @Test
    void cancelledStateMapsToFailed() {
        CoordinatorTaskTracker tracker = scheduledTask("q1", "t1");
        tracker.updateTaskState("q1", "t1", "RUNNING");
        tracker.updateTaskState("q1", "t1", "CANCELLED");
        assertEquals(TaskStateMachine.State.FAILED, tracker.getTaskState("q1", "t1"));
    }

    @Test
    void unknownWorkerStateRejected() {
        CoordinatorTaskTracker tracker = scheduledTask("q1", "t1");
        assertThatThrownBy(() -> tracker.updateTaskState("q1", "t1", "INVALID"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown worker state");
    }

    // ── TaskStatusReport field preservation ──

    @Test
    void taskStatusReportIdentityFieldsPreserved() {
        TaskStatusReport report = new TaskStatusReport(
                "task-42", "query-7", 3, 2, "COMPUTE", "worker-1",
                "RUNNING", null, null, null, 5, 1700000000000L,
                null, null, org.pangrova.protocol.rest.task.TaskAttemptDescriptor.of(1, 0));

        assertEquals("task-42", report.taskId());
        assertEquals("query-7", report.queryId());
        assertEquals(3, report.stageId());
        assertEquals(2, report.partitionId());
        assertEquals("COMPUTE", report.taskType());
        assertEquals("worker-1", report.workerId());
        assertEquals("RUNNING", report.state());
        assertEquals(5, report.reportSequence());
        assertEquals(1700000000000L, report.timestampMs());
    }

    @Test
    void taskStatusReportResponseFieldsPreserved() {
        TaskStatusReportResponse resp = new TaskStatusReportResponse("task-42", "CONTINUE");
        assertEquals("task-42", resp.taskId());
        assertEquals("CONTINUE", resp.command());
    }

    // ── TaskStatusReceiver integration ──

    @Test
    void unregisteredWorkerRejectedByReceiver() {
        WorkerManager wm = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
        CoordinatorTaskTracker tracker = new CoordinatorTaskTracker();
        QueryManager qm = buildMinimalQueryManager();

        TaskStatusReceiver receiver = new TaskStatusReceiver(
                TaskStatusReceiverContext.withoutSpeculation(qm, tracker, wm));

        TaskStatusReport report = new TaskStatusReport(
                "t1", "q1", 0, 0, "COMPUTE", "ghost",
                "RUNNING", null, null, null, 1, System.currentTimeMillis(),
                null, null, org.pangrova.protocol.rest.task.TaskAttemptDescriptor.of(1, 0));

        assertThatThrownBy(() -> receiver.onTaskStatus(report))
                .isInstanceOf(UnregisteredWorkerException.class);
    }

    @Test
    void registeredWorkerReportAcceptedAndStateUpdated() {
        WorkerManager wm = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
        wm.registerWorker(TestWorkerInfoBuilder.active("w1", "localhost", 8080, 9090));

        CoordinatorTaskTracker tracker = new CoordinatorTaskTracker();
        tracker.registerTask("q1", "t1");
        registerCanonicalRequest(tracker, "q1", "t1");
        tracker.markScheduled("q1", "t1");

        QueryManager qm = buildMinimalQueryManager();
        TaskStatusReceiver receiver = new TaskStatusReceiver(
                TaskStatusReceiverContext.withoutSpeculation(qm, tracker, wm));

        TaskStatusReport report = new TaskStatusReport(
                "t1", "q1", 0, 0, "COMPUTE", "w1",
                "RUNNING", null, null, null, 1, System.currentTimeMillis(),
                null, null, org.pangrova.protocol.rest.task.TaskAttemptDescriptor.from(
                        tracker.getTaskRequest("t1").taskRequest().getAttempt()));

        receiver.onTaskStatus(report);

        assertEquals(TaskStateMachine.State.RUNNING, tracker.getTaskState("q1", "t1"));
    }

    @Test
    void finishedReportTriggersQueryFinishedTransition() {
        WorkerManager wm = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
        wm.registerWorker(TestWorkerInfoBuilder.active("w1", "localhost", 8080, 9090));

        CoordinatorTaskTracker tracker = new CoordinatorTaskTracker();
        tracker.registerTask("q1", "t1");
        registerCanonicalRequest(tracker, "q1", "t1");
        tracker.markScheduled("q1", "t1");

        QueryManager qm = buildMinimalQueryManager();
        qm.registerStateMachine("q1", new QueryStateMachine());
        TaskStatusReceiver receiver = new TaskStatusReceiver(
                TaskStatusReceiverContext.withoutSpeculation(qm, tracker, wm));

        receiver.onTaskStatus(report(tracker, "t1", "q1", "w1", "RUNNING", 1));
        receiver.onTaskStatus(report(tracker, "t1", "q1", "w1", "FINISHED", 2));

        assertTrue(tracker.allTasksFinished("q1"),
                "All tasks should be finished after FINISHED report");
    }

    @Test
    void failedReportTriggersQueryFailedTransition() {
        WorkerManager wm = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
        wm.registerWorker(TestWorkerInfoBuilder.active("w1", "localhost", 8080, 9090));

        CoordinatorTaskTracker tracker = new CoordinatorTaskTracker();
        tracker.registerTask("q1", "t1");
        registerCanonicalRequest(tracker, "q1", "t1");
        tracker.markScheduled("q1", "t1");

        QueryManager qm = buildMinimalQueryManager();
        qm.registerStateMachine("q1", new QueryStateMachine());
        TaskStatusReceiver receiver = new TaskStatusReceiver(
                TaskStatusReceiverContext.withoutSpeculation(qm, tracker, wm));

        receiver.onTaskStatus(report(tracker, "t1", "q1", "w1", "RUNNING", 1));

        ErrorDetail error = new ErrorDetail("INTERNAL", "TestError", "something broke");
        TaskStatusReport failReport = new TaskStatusReport(
                "t1", "q1", 0, 0, "COMPUTE", "w1",
                "FAILED", null, error, null, 2, System.currentTimeMillis(),
                null, null, org.pangrova.protocol.rest.task.TaskAttemptDescriptor.from(
                        tracker.getTaskRequest("t1").taskRequest().getAttempt()),
                canonicalReport(tracker.getTaskRequest("t1"), "FAILED", "t1"));
        receiver.onTaskStatus(failReport);

        assertTrue(tracker.hasFailedTask("q1"),
                "Task should be in FAILED state after FAILED report");
    }

    // ── Helpers ──

    private static CoordinatorTaskTracker scheduledTask(String queryId, String taskId) {
        CoordinatorTaskTracker tracker = new CoordinatorTaskTracker();
        tracker.registerTask(queryId, taskId);
        tracker.registerTaskAttempt(taskId,
                org.pangrova.protocol.rest.task.TaskAttemptDescriptor.of(1, 0));
        tracker.markScheduled(queryId, taskId);
        return tracker;
    }

    private static void registerCanonicalRequest(
            CoordinatorTaskTracker tracker, String queryId, String taskId) {
        SubmitTaskRequest request = CanonicalTaskRequestTestFixture.request(
                taskId, queryId, 0, 0,
                org.pangrova.protocol.v2.TaskKind.TASK_KIND_READ, new byte[]{1});
        tracker.registerTaskRequest(taskId, request);
        tracker.registerTaskAttempt(taskId,
                org.pangrova.protocol.rest.task.TaskAttemptDescriptor.from(
                        request.taskRequest().getAttempt()));
    }

    private static TaskStatusReport report(CoordinatorTaskTracker tracker,
                                           String taskId, String queryId,
                                           String workerId, String state, long seq) {
        SubmitTaskRequest request = tracker.getTaskRequest(taskId);
        return new TaskStatusReport(
                taskId, queryId, 0, 0, "COMPUTE", workerId,
                state, null, null, null, seq, System.currentTimeMillis(),
                null, null, org.pangrova.protocol.rest.task.TaskAttemptDescriptor.from(
                        request.taskRequest().getAttempt()),
                "RUNNING".equals(state) ? null : canonicalReport(request, state, taskId));
    }

    private static byte[] canonicalReport(
            SubmitTaskRequest request, String state, String taskId) {
        var coverage = org.pangrova.protocol.v2.CoverageReport.newBuilder()
                .setExpectedWorkUnits(1);
        var report = org.pangrova.protocol.v2.TaskReport.newBuilder()
                .setReportOrigin(org.pangrova.protocol.v2.TaskReportOrigin
                        .TASK_REPORT_ORIGIN_NATIVE)
                .setAttempt(request.taskRequest().getAttempt());
        if ("FINISHED".equals(state)) {
            report.setState(org.pangrova.protocol.v2.TaskState.TASK_STATE_FINISHED);
            coverage.setCompletedWorkUnits(1).setCoverageRatio(1.0)
                    .setCompletionState(org.pangrova.protocol.v2.CompletionState
                            .COMPLETION_STATE_COMPLETE);
        } else if ("FAILED".equals(state)) {
            report.setState(org.pangrova.protocol.v2.TaskState.TASK_STATE_FAILED)
                    .setError(org.pangrova.protocol.v2.ExecutionError.newBuilder()
                            .setErrorClass(org.pangrova.protocol.v2.ErrorClass.ERROR_CLASS_INTERNAL)
                            .setCode("TEST_FAILURE").setMessage("something broke"));
            coverage.addFailedWorkUnitIds(taskId)
                    .setCompletionState(org.pangrova.protocol.v2.CompletionState
                            .COMPLETION_STATE_PARTIAL);
        } else {
            throw new IllegalArgumentException("Not a terminal state: " + state);
        }
        return report.setCoverage(coverage).build().toByteArray();
    }

    private static QueryManager buildMinimalQueryManager() {
        org.pangrova.planner.SqlCompiler compiler = new org.pangrova.planner.SqlCompiler();
        org.pangrova.scheduler.core.placement.TaskScheduler scheduler =
                new org.pangrova.scheduler.core.placement.TaskScheduler(
                        new org.pangrova.scheduler.core.scoring.WorkerPerformanceScorer());
        QueryDispatcher dispatcher = new QueryDispatcher(
                QueryDispatcherContext.withDefaults(
                        compiler, null, null, null,
                        new org.pangrova.coordinator.scheduler.RemoteTaskDispatcher(
                                org.pangrova.common.resilience.CircuitBreakerPolicy.noOp(),
                                new org.pangrova.observability.sdk.noop.NoOpTracingProvider()),
                        scheduler,
                        new org.pangrova.coordinator.worker.WorkerManager(20_000, 60_000, new org.pangrova.coordination.inmemory.InMemoryWorkerRegistry()),
                        new org.pangrova.coordinator.task.CoordinatorTaskTracker(),
                        null, null, null, 0L),
                null, null);
        return new QueryManager(QueryManagerContext.minimal(dispatcher,
                new org.pangrova.observability.sdk.noop.NoOpTracingProvider()));
    }
}
