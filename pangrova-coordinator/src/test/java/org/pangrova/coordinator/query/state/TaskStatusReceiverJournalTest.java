package org.pangrova.coordinator.query.state;

import java.util.List;

import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.coordinator.query.QueryManagerContext;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.query.task.TaskStatusReceiver;
import org.pangrova.coordinator.query.task.TaskStatusReceiverContext;
import org.pangrova.coordinator.scheduler.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.HealthState;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.coordinator.worker.WorkerPerformanceScorer;
import org.pangrova.observability.noop.NoOpTracingProvider;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.resource.MemoryPressure;
import org.pangrova.spi.coordination.QueryStateJournal;
import org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests verifying TaskStatusReceiver journals task state transitions.
 *
 * <p>Two tasks are registered per query so that a single task status report
 * does not trigger query-level transitions (which would call into
 * QueryManager.onAllTasksFinished and require a fully wired dispatcher).
 */
class TaskStatusReceiverJournalTest {

    private QueryManagerJournalTest.RecordingJournal journal;
    private TaskStatusReceiver receiver;
    private CoordinatorTaskTracker tracker;

    @BeforeEach
    void setUp() {
        journal = new QueryManagerJournalTest.RecordingJournal();
        QueryManager qm = buildQueryManager(journal);
        tracker = new CoordinatorTaskTracker();
        WorkerManager wm = new WorkerManager(5000, 15000, new org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry());
        wm.registerWorker(workerInfo("w-1"));

        TaskStatusReceiverContext deps = new TaskStatusReceiverContext(qm, tracker, wm, null, null, null, journal, null, null, null, null, null);
        receiver = new TaskStatusReceiver(deps);

        // Register TWO tasks so a single report does not trigger query-level
        // transitions (allTasksFinished / hasFailedTask remain false).
        tracker.registerTask("q-1", "t-1");
        registerCanonicalRequest("t-1");
        tracker.markScheduled("q-1", "t-1");
        tracker.registerTask("q-1", "t-2");
        registerCanonicalRequest("t-2");
        tracker.markScheduled("q-1", "t-2");
    }

    @Test
    void terminalStateJournaledSynchronously() {
        TaskStatusReport report = new TaskStatusReport(
                "t-1", "q-1", 0, 0, "COMPUTE", "w-1", "FINISHED",
                null, null, null, 1L, 0L, null, null,
                attempt(), canonicalReport("FINISHED"));
        receiver.onTaskStatus(report);

        assertEquals(1, journal.syncTaskEvents.size());
        assertEquals("FINISHED", journal.syncTaskEvents.get(0).state());
        assertEquals("t-1", journal.syncTaskEvents.get(0).taskId());
        assertTrue(journal.asyncTaskEvents.isEmpty(),
                "Terminal state should use sync, not async");
    }

    @Test
    void nonTerminalStateJournaledAsynchronously() {
        TaskStatusReport report = new TaskStatusReport(
                "t-1", "q-1", 0, 0, "COMPUTE", "w-1", "RUNNING",
                null, null, null, 1L, 0L, null, null,
                attempt(), canonicalReport("RUNNING"));
        receiver.onTaskStatus(report);

        assertEquals(1, journal.asyncTaskEvents.size());
        assertEquals("RUNNING", journal.asyncTaskEvents.get(0).state());
        assertTrue(journal.syncTaskEvents.isEmpty(),
                "Non-terminal state should use async, not sync");
    }

    @Test
    void failedStateJournaledSynchronously() {
        TaskStatusReport report = new TaskStatusReport(
                "t-1", "q-1", 0, 0, "COMPUTE", "w-1", "FAILED",
                null, null, null, 1L, 0L, null, null,
                attempt(), canonicalReport("FAILED"));
        receiver.onTaskStatus(report);

        assertEquals(1, journal.syncTaskEvents.size());
        assertEquals("FAILED", journal.syncTaskEvents.get(0).state());
    }

    private void registerCanonicalRequest(String taskId) {
        var request = org.pangrova.coordinator.CanonicalTaskRequestTestFixture.request(
                taskId, "q-1", 0, 0,
                org.pangrova.protocol.v2.TaskKind.TASK_KIND_READ, new byte[]{1});
        tracker.registerTaskRequest(taskId, request);
        tracker.registerTaskAttempt(taskId,
                org.pangrova.protocol.rest.task.TaskAttemptDescriptor.from(
                        request.taskRequest().getAttempt()));
    }

    private org.pangrova.protocol.rest.task.TaskAttemptDescriptor attempt() {
        return org.pangrova.protocol.rest.task.TaskAttemptDescriptor.from(
                tracker.getTaskRequest("t-1").taskRequest().getAttempt());
    }

    private byte[] canonicalReport(String state) {
        var report = org.pangrova.protocol.v2.TaskReport.newBuilder()
                .setReportOrigin(org.pangrova.protocol.v2.TaskReportOrigin
                        .TASK_REPORT_ORIGIN_NATIVE)
                .setAttempt(tracker.getTaskRequest("t-1").taskRequest().getAttempt());
        var coverage = org.pangrova.protocol.v2.CoverageReport.newBuilder()
                .setExpectedWorkUnits(1);
        switch (state) {
            case "FINISHED" -> {
                report.setState(org.pangrova.protocol.v2.TaskState.TASK_STATE_FINISHED);
                coverage.setCompletedWorkUnits(1).setCoverageRatio(1.0)
                        .setCompletionState(org.pangrova.protocol.v2.CompletionState
                                .COMPLETION_STATE_COMPLETE);
            }
            case "FAILED" -> {
                report.setState(org.pangrova.protocol.v2.TaskState.TASK_STATE_FAILED)
                        .setError(org.pangrova.protocol.v2.ExecutionError.newBuilder()
                                .setErrorClass(org.pangrova.protocol.v2.ErrorClass.ERROR_CLASS_INTERNAL)
                                .setCode("TEST_FAILURE").setMessage("failed"));
                coverage.addFailedWorkUnitIds("t-1")
                        .setCompletionState(org.pangrova.protocol.v2.CompletionState
                                .COMPLETION_STATE_PARTIAL);
            }
            case "RUNNING" -> report.setState(
                    org.pangrova.protocol.v2.TaskState.TASK_STATE_RUNNING);
            default -> throw new IllegalArgumentException("Unexpected state: " + state);
        }
        return report.setCoverage(coverage).build().toByteArray();
    }

    private static QueryManager buildQueryManager(QueryStateJournal journal) {
        QueryDispatcher dispatcher = buildMinimalDispatcher();
        QueryManagerContext deps = new QueryManagerContext(
                dispatcher, new NoOpTracingProvider(),
                null, null, null, null, null, null, null, null, journal, null);
        return new QueryManager(deps);
    }

    private static QueryDispatcher buildMinimalDispatcher() {
        CoordinatorTaskTracker tracker = new CoordinatorTaskTracker();
        WorkerManager workerManager = new WorkerManager(5000, 15000, new InMemoryWorkerRegistry());
        TaskScheduler scheduler = new TaskScheduler(new WorkerPerformanceScorer());
        QueryDispatcherContext qdDeps = new QueryDispatcherContext(
                new SqlCompiler(),
                org.pangrova.coordinator.query.execution.SecurityContextBundle.none(),
                new org.pangrova.coordinator.query.execution.SchedulingContextBundle(
                        (req, worker) -> null,
                        scheduler, workerManager, tracker,
                        null, null, null, null),
                null, null, 0, null, null, null, null, null, null, null, null, null);
        return new QueryDispatcher(qdDeps, null, null);
    }

    private static WorkerInfo workerInfo(String workerId) {
        return new WorkerInfo(workerId, "host-1", 8080, 8081, 0,
                HealthState.ACTIVE, MemoryPressure.NONE, 0.0, 0, 1024L * 1024 * 1024,
                java.time.Instant.now(), 1.0, null, 2, null, List.of(), null,
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.canonicalInventory());
    }
}
