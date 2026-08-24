package org.pangrova.coordinator.query;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;

import org.pangrova.coordinator.http.CoordinatorHttpServer;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.task.TaskStatusReceiver;
import org.pangrova.coordinator.query.task.TaskStatusReceiverContext;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.coordinator.scheduler.NoAvailableWorkerException;
import org.pangrova.coordinator.scheduler.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.TestWorkerInfoBuilder;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.coordinator.worker.WorkerPerformanceScorer;
import org.pangrova.observability.tracing.TracingHttpFilter;
import org.pangrova.testing.TestObservability;
import org.pangrova.protocol.rest.error.ErrorDetail;
import org.pangrova.protocol.rest.query.SubmitQueryResponse;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.spi.observability.TracingProvider;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.testing.TestExchangeTransport;
import org.pangrova.worker.http.WorkerHttpServer;
import org.pangrova.worker.task.TaskManager;
import org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Full pipeline integration test — single JVM, real HTTP communication.
 * WorkerHttpServer + CoordinatorHttpServer on random ports.
 * Uses real SqlCompiler (Calcite + Substrait) — no stubs.
 * Validates: R14.1, R14.2, AC2, AC4, AC14, Requirement 17.4.
 */
class PipelineIntegrationTest {

    private static final String QUERY_ID = "q1";
    private static final String WORKER_ID = "worker-1";
    private static final int TRANSPORT_PORT = 0;
    private static final TracingProvider TRACING = TestObservability.TRACING;
    private static final TracingHttpFilter TRACING_FILTER = new TracingHttpFilter(TRACING);

    private TaskManager workerTaskManager;
    private QueryManager queryManager;
    private QueryDispatcher orchestrator;
    private CoordinatorTaskTracker taskTracker;
    private TaskStatusReceiver taskStatusReceiver;
    private WorkerManager workerManager;

    private WorkerHttpServer workerHttpServer;
    private CoordinatorHttpServer coordinatorHttpServer;

    @BeforeEach
    void setUp() throws Exception {
        workerTaskManager = TaskManager.forHttpWiringOnly();
        taskTracker = new CoordinatorTaskTracker();

        // 1. Start WorkerHttpServer on random port
        workerHttpServer = createWorkerHttpServer(workerTaskManager);
        workerHttpServer.start();

        // 2. Build orchestrator with RemoteTaskDispatcher + real worker port
        workerManager = createWorkerManagerWithPort(workerHttpServer.getPort());
        orchestrator = buildOrchestrator(workerManager);
        queryManager = new QueryManager(QueryManagerContext.minimal(orchestrator, TRACING));

        // 3. Start CoordinatorHttpServer on random port to receive status reports
        taskStatusReceiver = new TaskStatusReceiver(
                TaskStatusReceiverContext.withoutSpeculation(queryManager, taskTracker, workerManager));
        coordinatorHttpServer = new CoordinatorHttpServer(0, taskStatusReceiver, queryManager, TRACING_FILTER);
        coordinatorHttpServer.start();

        var dagEngine = orchestrator.dagEngine();
        workerTaskManager.setAutoFinishReporter(report ->
                dagEngine.onTaskTerminal(report.queryId(), report.taskId(),
                        report.state(), report.error()));
    }

    @AfterEach
    void tearDown() throws Exception {
        if (coordinatorHttpServer != null) {
            coordinatorHttpServer.stop();
        }
        if (workerHttpServer != null) {
            workerHttpServer.stop();
        }
    }

    @Test
    void constantQueryReachesRunning() {
        SubmitQueryResponse response = submitTestQuery();

        assertThat(response.queryId()).isEqualTo(QUERY_ID);
        // Unified path: all results go through resultEndpoint (transport-based retrieval)
        assertThat(response.resultEndpoint()).isNotNull();
        assertThat(queryState()).isEqualTo(QueryStateMachine.State.RUNNING);
    }

    @Test
    void dispatchCreatesWorkerTask() {
        String taskId = dispatchAndGetTaskId();

        assertThat(taskId).isNotNull();
        assertThat(queryState()).isEqualTo(QueryStateMachine.State.RUNNING);
    }

    @Test
    void fullPipelineReachesFinished() {
        String taskId = dispatchAndGetTaskId();

        simulateTaskCompletion(taskId);

        assertThat(queryState()).isEqualTo(QueryStateMachine.State.FINISHED);
        assertThat(taskTracker.allTasksFinished(QUERY_ID)).isTrue();
    }

    @Test
    void stateMachineReachesTerminalAfterCompletion() {
        String taskId = dispatchAndGetTaskId();
        QueryStateMachine sm = queryManager.getStateMachine(QUERY_ID);

        assertThat(sm.getState()).isEqualTo(QueryStateMachine.State.RUNNING);

        simulateTaskCompletion(taskId);

        assertThat(sm.getState()).isEqualTo(QueryStateMachine.State.FINISHED);
        assertThat(sm.isTerminal()).isTrue();
    }

    @Test
    void trackerShowsFinishedAndNoFailure() {
        String taskId = dispatchAndGetTaskId();

        simulateTaskCompletion(taskId);

        assertThat(taskTracker.allTasksFinished(QUERY_ID)).isTrue();
        assertThat(taskTracker.hasFailedTask(QUERY_ID)).isFalse();
    }

    /** Validates: R14.3 — compile failure transitions QueryStateMachine to FAILED. */
    @Test
    void compileFailureReachesFailed() throws Exception {
        QueryManager mgr = buildManagerWith(new SqlCompiler(), createWorkerManager());

        // Real SqlCompiler throws on unparseable SQL
        assertThatThrownBy(() -> mgr.submitQuery(QUERY_ID, "THIS IS NOT VALID SQL !!!", testSession()))
                .isInstanceOf(RuntimeException.class);

        assertThat(mgr.getQueryState(QUERY_ID)).hasValue(QueryStateMachine.State.FAILED);
    }

    /** Validates: R14.4 — no active worker transitions to FAILED + NoAvailableWorkerException. */
    /** Validates: R14.4 — no active worker transitions to FAILED + RuntimeException for non-constant queries. */
    @Test
    void noAvailableWorkerReachesFailed() throws Exception {
        WorkerManager emptyWm = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
        RemoteTaskDispatcher dispatcher = new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), TRACING);
        TaskScheduler scheduler = new TaskScheduler(new WorkerPerformanceScorer());
        var emptyDeps = QueryDispatcherContext.withDefaults(
                new SqlCompiler(), null, null, null, dispatcher, scheduler, emptyWm, taskTracker, null, null, null, null, 0L);
        QueryDispatcher orch = new QueryDispatcher(
                emptyDeps, null, null);

        QueryStateMachine sm = new QueryStateMachine();
        CoordinatorSessionContext session = new CoordinatorSessionContext(QUERY_ID, Map.of(), null, null);

        // Constant queries (SELECT 1) complete locally when no workers and no resultService.
        // Use a non-constant query to trigger worker selection path.
        assertThatThrownBy(() -> orch.execute(sm, "SELECT * FROM nonexistent_table", session))
                .isInstanceOf(RuntimeException.class);

        assertThat(sm.getState()).isEqualTo(QueryStateMachine.State.FAILED);
    }

    /** Validates: R14.5 — drain mode rejects new queries with QueryRejectedException. */
    @Test
    void drainRejectsNewQueries() {
        queryManager.setAcceptingQueries(false);

        assertThatThrownBy(() -> queryManager.submitQuery(QUERY_ID, "SELECT 1", testSession()))
                .isInstanceOf(QueryRejectedException.class)
                .hasMessageContaining("draining");
    }

    /** Validates: R14.2, R2.3, R11.2 — two tasks both FINISHED → query FINISHED. */
    @Test
    void multiTaskAllFinishedReachesQueryFinished() {
        String task1 = dispatchAndGetTaskId();
        String task2 = "extra-task-2";

        // task1 already registered by orchestrator dispatch
        taskTracker.registerTask(QUERY_ID, task2);
        taskTracker.markScheduled(QUERY_ID, task2);

        taskStatusReceiver.onTaskStatus(runningReport(task1));
        taskStatusReceiver.onTaskStatus(runningReport(task2));

        taskStatusReceiver.onTaskStatus(finishedReport(task1));
        assertThat(queryState()).isEqualTo(QueryStateMachine.State.RUNNING);

        taskStatusReceiver.onTaskStatus(finishedReport(task2));
        assertThat(queryState()).isEqualTo(QueryStateMachine.State.FINISHED);
        assertThat(taskTracker.allTasksFinished(QUERY_ID)).isTrue();
        assertThat(taskTracker.hasFailedTask(QUERY_ID)).isFalse();
    }

    /** Validates: R14.2, R2.3, R11.2 — two tasks, one FAILED → query FAILED. */
    @Test
    void multiTaskPartialFailureReachesQueryFailed() {
        String task1 = dispatchAndGetTaskId();
        String task2 = "extra-task-2";

        // task1 already registered by orchestrator dispatch
        taskTracker.registerTask(QUERY_ID, task2);
        taskTracker.markScheduled(QUERY_ID, task2);

        taskStatusReceiver.onTaskStatus(runningReport(task2));
        taskStatusReceiver.onTaskStatus(failedReport(task2));

        // failQuery keeps the SM in the map for client polling (FAILED state)
        assertThat(queryManager.getQueryState(QUERY_ID))
                .hasValue(QueryStateMachine.State.FAILED);
        assertThat(taskTracker.hasFailedTask(QUERY_ID)).isTrue();
        assertThat(taskTracker.allTasksFinished(QUERY_ID)).isFalse();
    }

    // --- wiring ---

    private QueryDispatcher buildOrchestrator(WorkerManager wm) {
        RemoteTaskDispatcher dispatcher = new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), TRACING);
        TaskScheduler scheduler = new TaskScheduler(new WorkerPerformanceScorer());
        return new QueryDispatcher(
                QueryDispatcherContext.withDefaults(
                        new SqlCompiler(), null, null, null, dispatcher, scheduler, wm, taskTracker, null, null, null, null, 0L),
                null, null);
    }

    private QueryManager buildManagerWith(SqlCompiler compiler, WorkerManager wm) {
        RemoteTaskDispatcher dispatcher = new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), TRACING);
        TaskScheduler scheduler = new TaskScheduler(new WorkerPerformanceScorer());
        QueryDispatcher orch = new QueryDispatcher(
                QueryDispatcherContext.withDefaults(
                        compiler, null, null, null, dispatcher, scheduler, wm, taskTracker, null, null, null, null, 0L),
                null, null);
        return new QueryManager(QueryManagerContext.minimal(orch, TRACING));
    }

    private static CoordinatorSessionContext testSession() {
        return new CoordinatorSessionContext(QUERY_ID, Map.of(), null, null);
    }

    // --- actions ---

    private SubmitQueryResponse submitTestQuery() {
        return queryManager.submitQuery(QUERY_ID, "SELECT 1", testSession());
    }

    /**
     * Dispatches a query to Worker via orchestrator.execute().
     * Registers the state machine in queryManager for status tracking.
     * Returns the dispatch result's task ticket.
     */
    private String dispatchAndGetTaskId() {
        QueryStateMachine sm = new QueryStateMachine();
        CoordinatorSessionContext session = testSession();
        var result = orchestrator.execute(sm, "SELECT 1", session);
        // Inject the state machine into queryManager so status receiver can find it
        queryManager.registerStateMachine(QUERY_ID, sm);
        return result.taskId();
    }

    /**
     * Simulates the worker completing a task and reporting back to coordinator.
     * Task is already registered and SCHEDULED by the orchestrator's dispatch.
     */
    private void simulateTaskCompletion(String taskId) {
        taskStatusReceiver.onTaskStatus(runningReport(taskId));
        taskStatusReceiver.onTaskStatus(finishedReport(taskId));
    }

    // --- queries ---

    private QueryStateMachine.State queryState() {
        return queryManager.getQueryState(QUERY_ID).orElseThrow();
    }

    // --- factories ---

    private static WorkerHttpServer createWorkerHttpServer(TaskManager taskManager) {
        var config = new WorkerHttpServer.WorkerHttpConfig(0, "localhost", TRANSPORT_PORT);
        return new WorkerHttpServer(config, taskManager, TestExchangeTransport.INSTANCE);
    }

    private static WorkerManager createWorkerManagerWithPort(int httpPort) {
        WorkerManager wm = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
        wm.registerWorker(workerWithPort(httpPort));
        return wm;
    }

    private static WorkerManager createWorkerManager() {
        WorkerManager wm = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
        wm.registerWorker(workerWithPort(8080));
        return wm;
    }

    private static WorkerInfo workerWithPort(int httpPort) {
        return TestWorkerInfoBuilder.active(WORKER_ID, "localhost", httpPort, TRANSPORT_PORT);
    }

    private static TaskStatusReport finishedReport(String taskId) {
        return new TaskStatusReport(
                taskId, QUERY_ID, 0, 0, "COMPUTE", WORKER_ID, "FINISHED",
                null, null, null, 2L, System.currentTimeMillis());
    }

    private static TaskStatusReport runningReport(String taskId) {
        return new TaskStatusReport(
                taskId, QUERY_ID, 0, 0, "COMPUTE", WORKER_ID, "RUNNING",
                null, null, null, 1L, System.currentTimeMillis());
    }

    private static TaskStatusReport failedReport(String taskId) {
        ErrorDetail error = new ErrorDetail("INTERNAL", "INTERNAL", "task execution failed");
        return new TaskStatusReport(
                taskId, QUERY_ID, 0, 0, "COMPUTE", WORKER_ID, "FAILED",
                null, error, null, 2L, System.currentTimeMillis());
    }
}
