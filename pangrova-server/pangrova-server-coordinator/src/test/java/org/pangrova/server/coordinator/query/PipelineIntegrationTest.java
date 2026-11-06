package org.pangrova.server.coordinator.query;

import org.pangrova.coordinator.testinfra.QueryManagerTestContexts;

import org.pangrova.query.api.lifecycle.QueryState;

import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.query.core.lifecycle.QueryStateMachine;

import org.pangrova.coordinator.query.admission.QueryRejectedException;
import org.pangrova.query.api.session.QuerySession;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.pangrova.coordinator.testinfra.SourceFreeQueryFixture.withPolicies;

import org.pangrova.server.coordinator.http.CoordinatorHttpServer;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.task.TaskStatusReceiver;
import org.pangrova.coordinator.query.task.TaskStatusReceiverContext;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.scheduler.api.error.NoAvailableWorkerException;
import org.pangrova.scheduler.core.placement.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.TestWorkerInfoBuilder;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.scheduler.builtin.placement.LoadBalancedStrategy;
import org.pangrova.observability.sdk.tracing.http.TracingHttpFilter;
import org.pangrova.testing.testkit.TestObservability;
import org.pangrova.client.error.ErrorDetail;
import org.pangrova.query.api.submission.QuerySubmissionResult;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.observability.spi.tracing.TracingProvider;
import org.pangrova.planner.core.analysis.SqlCompiler;
import org.pangrova.testing.testkit.TestExchangeTransport;
import org.pangrova.worker.http.WorkerHttpServer;
import org.pangrova.worker.task.TaskManager;
import org.pangrova.persistence.coordination.memory.membership.InMemoryWorkerRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * HTTP routing and task lifecycle integration in one JVM on dynamic ports.
 * Uses the real compiler and an HTTP-wiring-only Worker task fixture.
 * Does not prove Native execution, result correctness or distributed isolation.
 */
class PipelineIntegrationTest {

    private static final String QUERY_ID = "q1";
    private static final String WORKER_ID = "worker-1";
    private static final String LOOPBACK_HOST = "127.0.0.1";
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
        workerManager = createWorkerManagerWithPort(workerHttpServer.getPort(),
                workerTaskManager.workerExecutionIdentity());
        orchestrator = buildOrchestrator(workerManager);
        queryManager = new QueryManager(QueryManagerTestContexts.minimal(orchestrator, TRACING));

        // 3. Start CoordinatorHttpServer on random port to receive status reports
        taskStatusReceiver = new TaskStatusReceiver(
                TaskStatusReceiverContext.withoutSpeculation(queryManager, taskTracker, workerManager, new org.pangrova.persistence.query.memory.transaction.InMemoryWriteCommitJournal()));
        coordinatorHttpServer = new CoordinatorHttpServer(0, taskStatusReceiver, queryManager, TRACING_FILTER);
        coordinatorHttpServer.start();

        var dagEngine = orchestrator.dagEngine();
        workerTaskManager.setAutoFinishReporter(report ->
                dagEngine.onTaskTerminal(report.queryId(), report.taskId(),
                        report.state(), org.pangrova.coordinator.query.task.TaskReportQueryAdapter.failure(report)));
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
        QuerySubmissionResult response = submitTestQuery();

        assertThat(response.queryId()).isEqualTo(QUERY_ID);
        // Unified path: all results go through resultEndpoint (transport-based retrieval)
        assertThat(response.resultEndpoint()).isNotNull();
        assertThat(queryState()).isEqualTo(QueryState.RUNNING);
    }

    @Test
    void dispatchCreatesWorkerTask() {
        String taskId = dispatchAndGetTaskId();

        assertThat(taskId).isNotNull();
        assertThat(queryState()).isEqualTo(QueryState.RUNNING);
    }

    @Test
    void fullPipelineReachesFinished() {
        String taskId = dispatchAndGetTaskId();

        simulateTaskCompletion(taskId);

        assertThat(queryState()).isEqualTo(QueryState.FINISHED);
        assertThat(taskTracker.allTasksFinished(QUERY_ID)).isTrue();
    }

    @Test
    void rejectedTerminalReportDoesNotConsumeSequenceOrFinishQuery() {
        String taskId = dispatchAndGetTaskId();
        assertThat(taskStatusReceiver.onTaskStatus(runningReport(taskId))).isTrue();
        TaskStatusReport valid = finishedReport(taskId);
        TaskStatusReport missingEvidence = new TaskStatusReport(
                valid.taskId(), valid.queryId(), valid.stageId(), valid.partitionId(), valid.taskType(),
                valid.workerId(), valid.state(), valid.stats(), valid.error(), valid.writeResult(),
                valid.reportSequence(), valid.timestampMs(), valid.errorCode(), valid.errorMetadata(),
                valid.attempt(), null);

        assertThat(taskStatusReceiver.onTaskStatus(missingEvidence)).isFalse();
        assertThat(queryState()).isEqualTo(QueryState.RUNNING);
        assertThat(taskTracker.allTasksFinished(QUERY_ID)).isFalse();
        assertThat(taskStatusReceiver.onTaskStatus(valid)).isTrue();
        assertThat(queryState()).isEqualTo(QueryState.FINISHED);
    }

    @Test
    void stateMachineReachesTerminalAfterCompletion() {
        String taskId = dispatchAndGetTaskId();
        QueryStateMachine sm = queryManager.getStateMachine(QUERY_ID);

        assertThat(sm.getState()).isEqualTo(QueryState.RUNNING);

        simulateTaskCompletion(taskId);

        assertThat(sm.getState()).isEqualTo(QueryState.FINISHED);
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

        assertThat(mgr.getQueryState(QUERY_ID)).hasValue(QueryState.FAILED);
    }

    /** A valid protected query with no Worker fails admission, not SQL parsing. */
    @Test
    void noAvailableWorkerReachesFailed() throws Exception {
        WorkerManager emptyWm = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
        RemoteTaskDispatcher dispatcher = new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), TRACING);
        TaskScheduler scheduler = new TaskScheduler(new LoadBalancedStrategy(), ignoredTenantForPlacement -> java.util.Optional.empty());
        var emptyDeps = QueryDispatcherContext.withDefaults(
                new SqlCompiler(), null, null, null, dispatcher, scheduler, emptyWm, taskTracker, null, null, null, 0L);
        QueryManager manager = new QueryManager(QueryManagerTestContexts.minimal(
                new QueryDispatcher(withPolicies(emptyDeps), null, null), TRACING));
        assertThatThrownBy(() -> manager.submitQuery(QUERY_ID, "SELECT 1", testSession()))
                .isInstanceOf(NoAvailableWorkerException.class);
        assertThat(manager.getQueryState(QUERY_ID)).hasValue(QueryState.FAILED);
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

        registerAdditionalTask(task1, task2);

        assertThat(taskStatusReceiver.onTaskStatus(runningReport(task1))).isTrue();
        assertThat(taskStatusReceiver.onTaskStatus(runningReport(task2))).isTrue();

        assertThat(taskStatusReceiver.onTaskStatus(finishedReport(task1))).isTrue();
        assertThat(queryState()).isEqualTo(QueryState.RUNNING);

        assertThat(taskStatusReceiver.onTaskStatus(finishedReport(task2))).isTrue();
        assertThat(queryState()).isEqualTo(QueryState.FINISHED);
        assertThat(taskTracker.allTasksFinished(QUERY_ID)).isTrue();
        assertThat(taskTracker.hasFailedTask(QUERY_ID)).isFalse();
    }

    /** Validates: R14.2, R2.3, R11.2 — two tasks, one FAILED → query FAILED. */
    @Test
    void multiTaskPartialFailureReachesQueryFailed() {
        String task1 = dispatchAndGetTaskId();
        String task2 = "extra-task-2";

        registerAdditionalTask(task1, task2);

        assertThat(taskStatusReceiver.onTaskStatus(runningReport(task2))).isTrue();
        assertThat(taskStatusReceiver.onTaskStatus(failedReport(task2))).isTrue();

        // failQuery keeps the SM in the map for client polling (FAILED state)
        assertThat(queryManager.getQueryState(QUERY_ID))
                .hasValue(QueryState.FAILED);
        assertThat(taskTracker.hasFailedTask(QUERY_ID)).isTrue();
        assertThat(taskTracker.allTasksFinished(QUERY_ID)).isFalse();
    }

    // --- wiring ---

    private QueryDispatcher buildOrchestrator(WorkerManager wm) {
        RemoteTaskDispatcher dispatcher = fixtureDispatcher();
        TaskScheduler scheduler = new TaskScheduler(new LoadBalancedStrategy(), ignoredTenantForPlacement -> java.util.Optional.empty());
        return new QueryDispatcher(
                withPolicies(QueryDispatcherContext.withDefaults(
                        new SqlCompiler(), null, null, null, dispatcher, scheduler, wm, taskTracker, null, null, null, 0L)),
                null, null);
    }

    private QueryManager buildManagerWith(SqlCompiler compiler, WorkerManager wm) {
        RemoteTaskDispatcher dispatcher = fixtureDispatcher();
        TaskScheduler scheduler = new TaskScheduler(new LoadBalancedStrategy(), ignoredTenantForPlacement -> java.util.Optional.empty());
        QueryDispatcher orch = new QueryDispatcher(
                withPolicies(QueryDispatcherContext.withDefaults(
                        compiler, null, null, null, dispatcher, scheduler, wm, taskTracker, null, null, null, 0L)),
                null, null);
        return new QueryManager(QueryManagerTestContexts.minimal(orch, TRACING));
    }

    private static QuerySession testSession() {
        return org.pangrova.coordinator.testinfra.SourceFreeQueryFixture.session(QUERY_ID);
    }

    private static RemoteTaskDispatcher fixtureDispatcher() {
        var dispatcher = new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), TRACING);
        dispatcher.setControlEpochSupplier(() -> 1L);
        dispatcher.setExecutionOwnerResolver(ignored -> new org.pangrova.protocol.rest.task.ExecutionOwnerIdentity(
                "test-coordinator", "test-incarnation", 1L));
        return dispatcher;
    }

    // --- actions ---

    private QuerySubmissionResult submitTestQuery() {
        return queryManager.submitQuery(QUERY_ID, "SELECT 1", testSession());
    }

    /**
     * Dispatches a query to Worker via orchestrator.execute().
     * Registers the state machine in queryManager for status tracking.
     * Returns the dispatch result's task ticket.
     */
    private String dispatchAndGetTaskId() {
        QueryStateMachine sm = new QueryStateMachine();
        QuerySession session = testSession();
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
        assertThat(taskStatusReceiver.onTaskStatus(runningReport(taskId))).isTrue();
        assertThat(taskStatusReceiver.onTaskStatus(finishedReport(taskId))).isTrue();
    }

    private void registerAdditionalTask(String dispatchedTask, String additionalTask) {
        var original = taskTracker.getTaskRequest(dispatchedTask).taskRequest();
        var request = org.pangrova.protocol.rest.task.SubmitTaskRequest.from(original.toBuilder()
                .setAttempt(original.getAttempt().toBuilder().setTaskId(additionalTask)).build());
        taskTracker.registerTask(QUERY_ID, additionalTask);
        taskTracker.registerTaskRequest(additionalTask, request);
        taskTracker.registerTaskAttempt(additionalTask,
                org.pangrova.protocol.rest.task.TaskAttemptDescriptor.from(request.taskRequest().getAttempt()));
        taskTracker.assignWorker(additionalTask, WORKER_ID);
        taskTracker.markScheduled(QUERY_ID, additionalTask);
    }

    // --- queries ---

    private QueryState queryState() {
        return queryManager.getQueryState(QUERY_ID).orElseThrow();
    }

    // --- factories ---

    private static WorkerHttpServer createWorkerHttpServer(TaskManager taskManager) {
        var config = new WorkerHttpServer.WorkerHttpConfig(0, LOOPBACK_HOST, TRANSPORT_PORT);
        return new WorkerHttpServer(config, taskManager, TestExchangeTransport.INSTANCE,
                null, null, new org.pangrova.worker.result.ResultReadTickets(new byte[32]));
    }

    private static WorkerManager createWorkerManagerWithPort(int httpPort,
            org.pangrova.protocol.execution.identity.WorkerExecutionIdentity identity) {
        WorkerManager wm = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
        wm.registerWorker(TestWorkerInfoBuilder.active(
                WORKER_ID, LOOPBACK_HOST, httpPort, TRANSPORT_PORT, identity));
        return wm;
    }

    private static WorkerManager createWorkerManager() {
        WorkerManager wm = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
        wm.registerWorker(workerWithPort(8080));
        return wm;
    }

    private static WorkerInfo workerWithPort(int httpPort) {
        return TestWorkerInfoBuilder.active(WORKER_ID, LOOPBACK_HOST, httpPort, TRANSPORT_PORT);
    }

    private TaskStatusReport finishedReport(String taskId) {
        return report(taskId, "FINISHED");
    }

    private TaskStatusReport runningReport(String taskId) {
        return report(taskId, "RUNNING");
    }

    private TaskStatusReport failedReport(String taskId) {
        return report(taskId, "FAILED");
    }

    private TaskStatusReport report(String taskId, String state) {
        var request = taskTracker.getTaskRequest(taskId).taskRequest();
        var attempt = request.getAttempt();
        assertThat(request.getSplitsCount()).as("Source-free routing fixture must not simulate a source scan")
                .isZero();
        ErrorDetail error = "FAILED".equals(state)
                ? new ErrorDetail("INTERNAL", "INTERNAL", "task execution failed") : null;
        byte[] canonical = null;
        if ("FINISHED".equals(state)) {
            canonical = org.pangrova.protocol.v2.TaskReport.newBuilder()
                    .setAttempt(attempt)
                    .setReportOrigin(org.pangrova.protocol.v2.TaskReportOrigin.TASK_REPORT_ORIGIN_NATIVE)
                    .setState(org.pangrova.protocol.v2.TaskState.TASK_STATE_FINISHED)
                    .setCoverage(org.pangrova.protocol.v2.CoverageReport.newBuilder()
                            .setExpectedWorkUnits(1).setCompletedWorkUnits(1).setCoverageRatio(1.0)
                            .setCompletionState(org.pangrova.protocol.v2.CompletionState.COMPLETION_STATE_COMPLETE))
                    .build().toByteArray();
        } else if ("FAILED".equals(state)) {
            canonical = org.pangrova.protocol.task.WorkerTerminalTaskReports.unsuccessful(request,
                    org.pangrova.protocol.v2.TaskState.TASK_STATE_FAILED, error);
        }
        return new TaskStatusReport(
                taskId, QUERY_ID, attempt.getStageId(), attempt.getPartitionId(),
                request.getExecutionOptions().getDirectResultOutput() ? "OUTPUT" : "COMPUTE", WORKER_ID, state,
                null, error, null, "RUNNING".equals(state) ? 1L : 2L, System.currentTimeMillis(), null, null,
                org.pangrova.protocol.rest.task.TaskAttemptDescriptor.from(attempt), canonical);
    }
}
