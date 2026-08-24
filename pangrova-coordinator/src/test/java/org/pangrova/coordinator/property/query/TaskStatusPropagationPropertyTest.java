package org.pangrova.coordinator.property.query;

import net.jqwik.api.*;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.query.task.TaskStatusReceiver;
import org.pangrova.coordinator.query.task.TaskStatusReceiverContext;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.coordinator.scheduler.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.TestWorkerInfoBuilder;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.coordinator.worker.WorkerPerformanceScorer;
import org.pangrova.observability.noop.NoOpTracingProvider;
import org.pangrova.protocol.rest.error.ErrorDetail;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.coordinator.query.QueryManagerContext;
import org.pangrova.coordinator.query.QueryStateMachine;
import org.pangrova.coordinator.query.CoordinatorSessionContext;
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
        WorkerManager workerManager = new WorkerManager(20_000, 60_000, new org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry());
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
        tracker.markScheduled(queryId, taskId);

        // Transition through RUNNING before terminal state
        receiver.onTaskStatus(buildReport(taskId, queryId, WORKER_ID, "RUNNING", null));

        // Send terminal state
        ErrorDetail error = "FAILED".equals(state)
                ? new ErrorDetail("INTERNAL", "INTERNAL", "simulated failure")
                : null;
        receiver.onTaskStatus(buildReport(taskId, queryId, WORKER_ID, state, error));

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
        WorkerManager workerManager = new WorkerManager(20_000, 60_000, new org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry());

        QueryManager qm = buildMinimalQueryManager(tracker, workerManager);
        TaskStatusReceiver receiver = new TaskStatusReceiver(
                TaskStatusReceiverContext.withoutSpeculation(qm, tracker, workerManager));

        TaskStatusReport report = buildReport(
                taskId, queryId, "unknown-worker", "RUNNING", null);

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
                                                 ErrorDetail error) {
        return new TaskStatusReport(
                taskId, queryId, 0, 0, "SCAN",
                workerId, state, null, error, null,
                REPORT_SEQ.incrementAndGet(), System.currentTimeMillis());
    }

    private static QueryManager buildMinimalQueryManager(
            CoordinatorTaskTracker tracker, WorkerManager workerManager) {
        var dispatcher = new RemoteTaskDispatcher(
                CircuitBreakerPolicy.noOp(), new NoOpTracingProvider());
        var scheduler = new TaskScheduler(new WorkerPerformanceScorer());
        var deps = QueryDispatcherContext.withDefaults(
                new SqlCompiler(), null, null, null, dispatcher, scheduler,
                workerManager, tracker, null, null, null, null, 0L);
        var queryDispatcher = new QueryDispatcher(
                deps, null, null);
        return new QueryManager(
                QueryManagerContext.minimal(queryDispatcher, new NoOpTracingProvider()));
    }
}
