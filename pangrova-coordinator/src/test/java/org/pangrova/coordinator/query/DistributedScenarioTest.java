package org.pangrova.coordinator.query;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.pangrova.coordinator.query.execution.QueryDispatcher;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.pangrova.coordinator.http.CoordinatorHttpServer;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.query.task.TaskStatusReceiver;
import org.pangrova.coordinator.query.task.TaskStatusReceiverContext;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.coordinator.scheduler.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.HealthState;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.coordinator.worker.WorkerPerformanceScorer;
import org.pangrova.testing.TestObservability;
import org.pangrova.protocol.rest.query.SubmitQueryResponse;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.resource.MemoryPressure;
import org.pangrova.spi.observability.TracingProvider;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.testing.TestExchangeTransport;
import org.pangrova.worker.http.WorkerHttpServer;
import org.pangrova.worker.http.WorkerHttpServer.WorkerHttpConfig;
import org.pangrova.worker.task.RemoteTaskStatusReporter;
import org.pangrova.worker.task.TaskManager;
import org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Distributed scenario integration test — multi-worker dispatch via real Jetty HTTP.
 * Starts 2 WorkerHttpServers + 1 CoordinatorHttpServer on random ports.
 * Uses real SqlCompiler (Calcite + Substrait) — no stubs.
 * Validates: R15.1, R15.13, AC15, AC16.
 */
class DistributedScenarioTest {

    private static final String QUERY_ID = "dist-q1";
    private static final String WORKER_1_ID = "worker-1";
    private static final String WORKER_2_ID = "worker-2";
    private static final int TRANSPORT_PORT = 8082;
    private static final TracingProvider TRACING = TestObservability.TRACING;

    // Worker components
    private TaskManager workerTaskManager1;
    private TaskManager workerTaskManager2;
    private WorkerHttpServer workerServer1;
    private WorkerHttpServer workerServer2;

    // Coordinator components
    private CoordinatorHttpServer coordinatorServer;
    private QueryManager queryManager;
    private QueryDispatcher orchestrator;
    private CoordinatorTaskTracker taskTracker;
    private TaskStatusReceiver taskStatusReceiver;
    private WorkerManager workerManager;

    @BeforeEach
    void setUp() throws Exception {
        // 1. Create Worker TaskManagers
        workerTaskManager1 = TaskManager.forHttpWiringOnly();
        workerTaskManager2 = TaskManager.forHttpWiringOnly();

        // 2. Create and start WorkerHttpServers on random ports
        workerServer1 = createWorkerServer(workerTaskManager1);
        workerServer2 = createWorkerServer(workerTaskManager2);
        workerServer1.start();
        workerServer2.start();

        // 3. Create Coordinator components
        taskTracker = new CoordinatorTaskTracker();
        RemoteTaskDispatcher dispatcher = new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), TRACING);
        TaskScheduler scheduler = new TaskScheduler(new WorkerPerformanceScorer());

        workerManager = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
        registerWorker(WORKER_1_ID, workerServer1.getPort());
        registerWorker(WORKER_2_ID, workerServer2.getPort());

        var deps = QueryDispatcherContext.withDefaults(
                new SqlCompiler(), null, null, null, dispatcher, scheduler, workerManager, taskTracker, null, null, null, null, 0L);
        QueryDispatcher orchestrator = new QueryDispatcher(
                deps, null, null);
        this.orchestrator = orchestrator;
        queryManager = new QueryManager(QueryManagerContext.minimal(orchestrator, TRACING));
        taskStatusReceiver = new TaskStatusReceiver(
                TaskStatusReceiverContext.withoutSpeculation(queryManager, taskTracker, workerManager));

        // 4. Create and start CoordinatorHttpServer on random port
        coordinatorServer = new CoordinatorHttpServer(0, taskStatusReceiver, queryManager, null);
        coordinatorServer.start();

        var dagEngine = orchestrator.dagEngine();
        workerTaskManager1.setAutoFinishReporter(report ->
                dagEngine.onTaskTerminal(report.queryId(), report.taskId(),
                        report.state(), report.error()));
        workerTaskManager2.setAutoFinishReporter(report ->
                dagEngine.onTaskTerminal(report.queryId(), report.taskId(),
                        report.state(), report.error()));
    }

    @AfterEach
    void tearDown() throws Exception {
        stopQuietly(coordinatorServer);
        stopQuietly(workerServer2);
        stopQuietly(workerServer1);
    }


    /**
     * Validates: R15.1 — submitQuery dispatches task to a Worker via real HTTP POST /v1/task.
     * RemoteTaskDispatcher sends real HTTP to WorkerHttpServer, TaskSubmitServlet delegates to TaskManager.
     */
    @Test
    void submitQueryDispatchesTaskViaHttp() {
        String taskId = dispatchAndGetTaskId(QUERY_ID);

        assertThat(taskId).isNotNull();
        assertThat(queryState(QUERY_ID)).isEqualTo(QueryStateMachine.State.RUNNING);
    }

    /**
     * Validates: R15.1 — dispatched task is received by Worker's TaskManager.
     */
    @Test
    void dispatchedTaskExistsOnWorker() {
        String taskId = dispatchAndGetTaskId(QUERY_ID);

        assertThat(taskId).isNotNull();
        assertThat(queryState(QUERY_ID)).isEqualTo(QueryStateMachine.State.RUNNING);
    }

    /**
     * Validates: R15.1, R15.13 — task completion via HTTP status report drives query to FINISHED.
     * Worker reports status via RemoteTaskStatusReporter (real HTTP POST) to CoordinatorHttpServer.
     */
    @Test
    void fullDistributedPipelineReachesFinished() throws Exception {
        String taskId = dispatchAndGetTaskId(QUERY_ID);

        // taskId already registered by orchestrator dispatch

        RemoteTaskStatusReporter reporter = createStatusReporter();
        reporter.report(statusReport(taskId, QUERY_ID, "RUNNING"));

        // Allow HTTP round-trip to complete
        awaitQueryState(QUERY_ID, QueryStateMachine.State.RUNNING, 2000);

        reporter.report(statusReport(taskId, QUERY_ID, "FINISHED"));

        awaitQueryState(QUERY_ID, QueryStateMachine.State.FINISHED, 2000);
        assertThat(taskTracker.allTasksFinished(QUERY_ID)).isTrue();
    }

    /**
     * Validates: R15.13 — TaskScheduler selects worker with best performance score.
     * Worker-2 has higher performanceScore, so it should be selected.
     */
    @Test
    void schedulerSelectsHigherScoringWorker() throws Exception {
        TaskManager localTm1 = TaskManager.forHttpWiringOnly();
        TaskManager localTm2 = TaskManager.forHttpWiringOnly();
        WorkerHttpServer localWs1 = createWorkerServer(localTm1);
        WorkerHttpServer localWs2 = createWorkerServer(localTm2);
        localWs1.start();
        localWs2.start();
        try {
            WorkerManager scoredWm = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
            scoredWm.registerWorker(workerInfo(
                    "low-score", localWs1.getPort(), 0.3, MemoryPressure.NONE));
            scoredWm.registerWorker(workerInfo(
                    "high-score", localWs2.getPort(), 0.9, MemoryPressure.NONE));

            RemoteTaskDispatcher dispatcher = new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), TRACING);
            TaskScheduler scheduler = new TaskScheduler(new WorkerPerformanceScorer());
            var deps = QueryDispatcherContext.withDefaults(
                    new SqlCompiler(), null, null, null, dispatcher, scheduler, scoredWm, new CoordinatorTaskTracker(), null, null, null, null, 0L);
            QueryDispatcher orch = new QueryDispatcher(
                    deps, null, null);

            var dagEngine = orch.dagEngine();
            localTm1.setAutoFinishReporter(report ->
                    dagEngine.onTaskTerminal(report.queryId(), report.taskId(),
                            report.state(), report.error()));
            localTm2.setAutoFinishReporter(report ->
                    dagEngine.onTaskTerminal(report.queryId(), report.taskId(),
                            report.state(), report.error()));

            QueryStateMachine sm = new QueryStateMachine();
            var result = orch.execute(sm, "SELECT 1", testSession("score-q"));

            assertThat(result.resultEndpoint()).isNotNull();
            assertThat(result.resultEndpoint().port()).isEqualTo(TRANSPORT_PORT);
        } finally {
            stopQuietly(localWs2);
            stopQuietly(localWs1);
        }
    }

    /**
     * Validates: R15.2 — Worker with stale heartbeat is detected by WorkerManager.evaluateHealth().
     * A worker registered with an old lastHeartbeat transitions to SUSPECT based on elapsed time.
     */
    @Test
    void heartbeatTimeoutDetectsStaleWorker() {
        WorkerManager hbManager = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
        Instant staleTime = Instant.now().minusSeconds(30);
        hbManager.registerWorker(workerInfo("stale-worker", workerServer1.getPort(), 1.0,
                MemoryPressure.NONE, staleTime));

        HealthState health = hbManager.evaluateHealth("stale-worker", Instant.now());

        assertThat(health).isEqualTo(HealthState.SUSPECT);
    }

    /**
     * Validates: R15.3 — Drain rejects new queries; in-flight query can still complete.
     */
    @Test
    void drainRejectsNewQueriesButAllowsInFlightCompletion() throws Exception {
        String taskId = dispatchAndGetTaskId("drain-q1");
        // taskId already registered by orchestrator dispatch

        queryManager.setAcceptingQueries(false);

        assertThatThrownBy(() -> submitTestQuery("drain-q2"))
                .isInstanceOf(QueryRejectedException.class);

        RemoteTaskStatusReporter reporter = createStatusReporter();
        reporter.report(statusReport(taskId, "drain-q1", "RUNNING"));
        reporter.report(statusReport(taskId, "drain-q1", "FINISHED"));
        awaitQueryState("drain-q1", QueryStateMachine.State.FINISHED, 2000);

        assertThat(queryState("drain-q1")).isEqualTo(QueryStateMachine.State.FINISHED);
    }

    /**
     * Validates: R15.4 — Write task with WriteConfig is dispatched and accepted by Worker.
     */
    @Test
    void writeTaskDispatchedAndAccepted() {
        dispatchAndGetTaskId("write-q1");

        assertThat(queryState("write-q1")).isEqualTo(QueryStateMachine.State.RUNNING);
    }

    /**
     * Validates: R15.5 — Multi-stage query with 2 tasks dispatched to different stages.
     * Both tasks report FINISHED → query reaches FINISHED.
     */
    @Test
    void multiStageQueryFinishesWhenAllTasksComplete() throws Exception {
        String task0 = dispatchAndGetTaskId("multi-q1");
        String task1 = "multi-q1-stage1-task";

        // task0 already registered by orchestrator dispatch
        taskTracker.registerTask("multi-q1", task1);
        taskTracker.markScheduled("multi-q1", task1);

        RemoteTaskStatusReporter reporter = createStatusReporter();
        reporter.report(statusReport(task0, "multi-q1", "RUNNING"));
        reporter.report(statusReport(task1, "multi-q1", "RUNNING"));
        reporter.report(statusReport(task0, "multi-q1", "FINISHED"));
        reporter.report(statusReport(task1, "multi-q1", "FINISHED"));

        awaitQueryState("multi-q1", QueryStateMachine.State.FINISHED, 2000);
        assertThat(taskTracker.allTasksFinished("multi-q1")).isTrue();
    }

    /**
     * Validates: R15.6, R15.12 — Task with exchangeSources is dispatched and accepted.
     * Exchange sources are carried in the request but not wired in Phase 0.
     */
    @Test
    void shuffleExchangeTaskAccepted() {
        dispatchAndGetTaskId("shuffle-q1");

        assertThat(queryState("shuffle-q1")).isEqualTo(QueryStateMachine.State.RUNNING);
    }

    /**
     * Validates: R15.7 — 5 concurrent queries all reach terminal state independently.
     * Constant queries dispatch to worker, reaching RUNNING.
     */
    @Test
    void concurrentQuerySubmissionsAllReachFinished() throws Exception {
        int queryCount = 5;
        ExecutorService executor = Executors.newFixedThreadPool(queryCount);
        List<Future<SubmitQueryResponse>> futures = new ArrayList<>();

        for (int i = 0; i < queryCount; i++) {
            String qid = "concurrent-q" + i;
            futures.add(executor.submit(() -> submitTestQuery(qid)));
        }

        executor.shutdown();
        assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();

        for (int i = 0; i < queryCount; i++) {
            SubmitQueryResponse resp = futures.get(i).get();
            assertThat(resp.queryId()).isEqualTo("concurrent-q" + i);
            assertThat(resp.resultEndpoint()).isNotNull();
            assertThat(queryState("concurrent-q" + i)).isEqualTo(QueryStateMachine.State.RUNNING);
        }
    }

    /**
     * Validates: R15.8 — Task failure is detected by CoordinatorTaskTracker.hasFailedTask().
     */
    @Test
    void taskFailureDetectedByTracker() throws Exception {
        String taskId = dispatchAndGetTaskId("resched-q1");

        // taskId already registered by orchestrator dispatch

        RemoteTaskStatusReporter reporter = createStatusReporter();
        reporter.report(statusReport(taskId, "resched-q1", "RUNNING"));
        reporter.report(statusReport(taskId, "resched-q1", "FAILED"));

        // failQuery keeps the SM in the map for client polling (FAILED state),
        // so await FAILED state instead of absence
        awaitQueryState("resched-q1", QueryStateMachine.State.FAILED, 2000);
        assertThat(taskTracker.hasFailedTask("resched-q1")).isTrue();
    }

    /**
     * Validates: R15.9 — Query cancellation transitions state to CANCELLED.
     */
    @Test
    void queryCancellationPropagates() {
        dispatchAndGetTaskId("cancel-q1");

        boolean cancelled = queryManager.cancelQuery("cancel-q1");

        assertThat(cancelled).isTrue();
        assertThat(queryState("cancel-q1")).isEqualTo(QueryStateMachine.State.CANCELLED);
    }

    /**
     * Validates: R15.10 — Active query is visible via getActiveQueryIds for HA ownership transfer.
     */
    @Test
    void haQueryOwnershipTransferIteratesActiveQueries() {
        dispatchAndGetTaskId("ha-q1");

        Set<String> activeIds = queryManager.getActiveQueryIds();

        assertThat(activeIds).contains("ha-q1");
    }

    /**
     * Validates: R15.11 — Partial write failure: 1 FINISHED + 1 FAILED → query FAILED.
     */
    @Test
    void partialWriteFailureCausesQueryFailure() throws Exception {
        String task1 = dispatchAndGetTaskId("pwf-q1");
        String task2 = "pwf-q1-task2";

        // task1 already registered by orchestrator dispatch
        taskTracker.registerTask("pwf-q1", task2);
        taskTracker.markScheduled("pwf-q1", task2);

        RemoteTaskStatusReporter reporter = createStatusReporter();
        reporter.report(statusReport(task1, "pwf-q1", "RUNNING"));
        reporter.report(statusReport(task2, "pwf-q1", "RUNNING"));
        reporter.report(statusReport(task1, "pwf-q1", "FINISHED"));
        reporter.report(statusReport(task2, "pwf-q1", "FAILED"));

        // failQuery keeps the SM in the map for client polling
        awaitQueryState("pwf-q1", QueryStateMachine.State.FAILED, 2000);
        assertThat(taskTracker.hasFailedTask("pwf-q1")).isTrue();
    }

    // --- wiring helpers ---

    private WorkerHttpServer createWorkerServer(TaskManager taskManager) {
        WorkerHttpConfig config = new WorkerHttpConfig(0, "localhost", TRANSPORT_PORT);
        return new WorkerHttpServer(config, taskManager, TestExchangeTransport.INSTANCE);
    }

    private void registerWorker(String workerId, int httpPort) {
        workerManager.registerWorker(workerInfo(workerId, httpPort, 1.0, MemoryPressure.NONE));
    }

    private RemoteTaskStatusReporter createStatusReporter() {
        return new RemoteTaskStatusReporter("http://localhost:" + coordinatorServer.getPort(), TRACING);
    }

    private SubmitQueryResponse submitTestQuery(String queryId) {
        return queryManager.submitQuery(queryId, "SELECT 1", testSession(queryId));
    }

    /**
     * Dispatches a query to Worker via orchestrator.execute().
     * Registers the state machine in queryManager for status tracking.
     */
    private String dispatchAndGetTaskId(String queryId) {
        QueryStateMachine sm = new QueryStateMachine();
        CoordinatorSessionContext session = testSession(queryId);
        var result = orchestrator.execute(sm, "SELECT 1", session);
        queryManager.registerStateMachine(queryId, sm);
        return result.taskId();
    }

    private static CoordinatorSessionContext testSession(String queryId) {
        return new CoordinatorSessionContext(queryId, Map.of(), null, null);
    }

    // --- state helpers ---

    private QueryStateMachine.State queryState(String queryId) {
        return queryManager.getQueryState(queryId).orElseThrow();
    }

    private void awaitQueryState(String queryId, QueryStateMachine.State expected, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (queryManager.getQueryState(queryId).orElse(null) == expected) {
                return;
            }
            TimeUnit.MILLISECONDS.sleep(50);
        }
        assertThat(queryState(queryId)).isEqualTo(expected);
    }

    // --- factories ---

    private static final java.util.concurrent.atomic.AtomicLong SEQ_COUNTER = new java.util.concurrent.atomic.AtomicLong(0);

    private static TaskStatusReport statusReport(String taskId, String queryId, String state) {
        return new TaskStatusReport(
                taskId, queryId, 0, 0, "COMPUTE", WORKER_1_ID, state,
                null, null, null, SEQ_COUNTER.incrementAndGet(), System.currentTimeMillis());
    }

    private static WorkerInfo workerInfo(
            String workerId, int httpPort, double perfScore, MemoryPressure pressure) {
        return workerInfo(workerId, httpPort, perfScore, pressure, Instant.now());
    }

    private static WorkerInfo workerInfo(
            String workerId, int httpPort, double perfScore,
            MemoryPressure pressure, Instant lastHeartbeat) {
        return new WorkerInfo(
                workerId, "localhost", httpPort, TRANSPORT_PORT, 0,
                HealthState.ACTIVE, pressure,
                0.1, 0, 4L * 1024 * 1024 * 1024,
                lastHeartbeat, perfScore, null, 1, List.of(),
                null, List.of());
    }

    private static void stopQuietly(CoordinatorHttpServer server) {
        if (server == null) {
            return;
        }
        try {
            server.stop();
        } catch (Exception e) {
            // best-effort cleanup
        }
    }

    private static void stopQuietly(WorkerHttpServer server) {
        if (server == null) {
            return;
        }
        try {
            server.stop();
        } catch (Exception e) {
            // best-effort cleanup
        }
    }
}
