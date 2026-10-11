package org.pangrova.testing.e2e.shuffle;

import org.pangrova.coordinator.http.CoordinatorHttpServer;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
// StageFragmenter removed from prod — QueryDispatcher now takes only (deps, statementDispatcher)
import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.coordinator.query.QueryManagerContext;
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
import org.pangrova.planner.SqlCompiler;
import org.pangrova.testing.TestObservability;
import org.pangrova.protocol.rest.query.ResultEndpoint;
import org.pangrova.resource.MemoryPressure;
import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.TestCluster;
import org.pangrova.testing.e2e.CanonicalWorkerInventory;
import org.pangrova.testing.e2e.E2ETestFramework;
import org.pangrova.testing.e2e.SqlE2ETestBase;
import org.pangrova.testing.e2e.runner.SqlTestRunner;
import org.pangrova.testing.e2e.setup.TestDataSetup;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.io.Closeable;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

/**
 * Integration-test base for coordinator routing through virtual worker identities.
 *
 * <p>Registers two worker identities with the coordinator, both pointing to
 * the same real {@code WorkerHttpServer} and {@code TransportServer}. This
 * exercises coordinator selection and routing logic, but it is not a
 * multi-process or distributed-system test and must not be presented as one.
 */
@Tag("routing-integration")
public abstract class VirtualWorkerRoutingIntegrationTestBase extends SqlE2ETestBase {

    private static final int WORKER_COUNT = 2;
    private static final long WORKER_MEMORY_BYTES = 4L * 1024 * 1024 * 1024;

    private static CoordinatorHttpServer coordinatorServer;
    private static QueryManager routingQueryManager;
    private static WorkerManager routingWorkerManager;
    private static CoordinatorTaskTracker routingTaskTracker;
    private static RemoteTaskDispatcher routingDispatcher;
    private static ExecutorService routingAsyncExecutor;

    // -----------------------------------------------------------------------
    // Lifecycle: overlay a virtual-worker routing coordinator on the shared worker
    // -----------------------------------------------------------------------

    @BeforeAll
    static void initVirtualWorkerRoutingHarness() {
        isDockerReady();
        testCluster = createRoutingHarness();
        framework = new E2ETestFramework(
                testCluster, VirtualWorkerRoutingIntegrationTestBase::executeViaRoutingPipeline);
        sqlRunner = new SqlTestRunner(framework);
        dataSetup = new TestDataSetup(sqlRunner);
        framework.setup();
    }

    @AfterAll
    static void stopVirtualWorkerRoutingHarness() {
        if (framework != null) {
            framework.teardown();
        }
        if (routingDispatcher != null) {
            routingDispatcher.close();
            routingDispatcher = null;
        }
        if (routingAsyncExecutor != null) {
            routingAsyncExecutor.shutdownNow();
            routingAsyncExecutor = null;
        }
        stopCoordinatorQuietly(coordinatorServer);
        coordinatorServer = null;
        routingQueryManager = null;
        routingWorkerManager = null;
        routingTaskTracker = null;
        routingPipelineRefreshed = false;
    }

    // -----------------------------------------------------------------------
    // Virtual-worker routing harness wiring
    // -----------------------------------------------------------------------

    private static TestCluster createRoutingHarness() {
        return new TestCluster(
                VirtualWorkerRoutingIntegrationTestBase::startCoordinator,
                VirtualWorkerRoutingIntegrationTestBase::registerVirtualWorkerRoutes);
    }

    private static Closeable startCoordinator() {
        routingTaskTracker = new CoordinatorTaskTracker();
        QueryDispatcher dispatcher = buildRoutingDispatcher();
        routingQueryManager = new QueryManager(routingQueryManagerContext(dispatcher));

        // Wire stageCompletionCallback so TaskStatusReceiver routes terminal
        // events to DAGDispatchEngine — required for DAG completion in E2E.
        var dagEngine = dispatcher.dagEngine();
        org.pangrova.coordinator.query.execution.dispatch.dag.StageCompletionCallback stageCallback =
                new org.pangrova.coordinator.query.execution.dispatch.dag.StageCompletionCallback() {
                    @Override
                    public void onTaskTerminal(String queryId, String taskId, String state,
                                               org.pangrova.protocol.rest.error.ErrorDetail error) {
                        dagEngine.onTaskTerminal(queryId, taskId, state, error);
                    }

                    @Override
                    public boolean isQueryComplete(String queryId) {
                        return dagEngine.isQueryComplete(queryId);
                    }
                };
        TaskStatusReceiver receiver = new TaskStatusReceiver(
                new TaskStatusReceiverContext(
                        routingQueryManager, routingTaskTracker, routingWorkerManager,
                        null, null, null,
                        routingQueryManager.getStateJournal(),
                        null, null, stageCallback, null, null));
        coordinatorServer = new CoordinatorHttpServer(
                0, receiver, routingQueryManager, null);
        startHttpServer(coordinatorServer);

        // Point the worker's status reporter to this routing-test coordinator
        // so task completion reports reach the correct DAGDispatchEngine instance.
        // We must NOT overwrite the parent's coordinatorHttpServer — that would
        // break the parent's rebuildQueryPipeline lifecycle. Instead, call
        // updateTarget directly on the shared status reporter.
        configureStatusReporterForPort(coordinatorServer.getPort());

        return () -> stopCoordinatorQuietly(coordinatorServer);
    }

    /**
     * Registers N virtual workers all pointing to the parent's real
     * WorkerHttpServer port. The coordinator sees N distinct workers
     * and triggers shuffle partitioning, but all tasks execute on the
     * single real worker with NativeTaskExecutor.
     */
    private static Closeable registerVirtualWorkerRoutes() {
        int realHttpPort = workerHttpPort();
        int realTransportPort = coordinatorTransportServer != null ? coordinatorTransportServer.getPort() : 0;
        for (int i = 1; i <= WORKER_COUNT; i++) {
            registerWorkerNode(realHttpPort, realTransportPort, "e2e-worker-" + i);
        }
        return () -> { /* nothing to close — parent owns the real worker */ };
    }

    // -----------------------------------------------------------------------
    // Component construction
    // -----------------------------------------------------------------------

    private static QueryDispatcher buildRoutingDispatcher() {
        routingWorkerManager = new WorkerManager();
        RemoteTaskDispatcher dispatcher = new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), TestObservability.TRACING);
        routingDispatcher = dispatcher;
        SqlCompiler compiler = buildSqlCompiler();
        // Late-bound NativeDmlCompileFunction for native DML execution.
        // In E2E tests, completes QueryStateMachine lifecycle directly.
        var queryDispatcherRef = new java.util.concurrent.atomic.AtomicReference<QueryDispatcher>();
        org.pangrova.coordinator.query.execution.dispatch.NativeDmlCompileFunction dmlDispatch =
                (sm, session, stmt) -> {
                    sm.tryTransition(org.pangrova.coordinator.query.QueryStateMachine.State.SCHEDULING);
                    sm.tryTransition(org.pangrova.coordinator.query.QueryStateMachine.State.RUNNING);
                    sm.tryTransition(org.pangrova.coordinator.query.QueryStateMachine.State.FINISHING);
                    sm.tryTransition(org.pangrova.coordinator.query.QueryStateMachine.State.FINISHED);
                    return new org.pangrova.coordinator.query.execution.dispatch.DispatchResult(
                            "localhost", session.queryId(),
                            new org.pangrova.protocol.rest.query.ResultEndpoint(
                                    "localhost", 0, "dml-noop", "grpc"));
                };
        var deps = QueryDispatcherContext.withDefaults(
                compiler, null, null, null,
                dispatcher,
                new TaskScheduler(new WorkerPerformanceScorer()),
                routingWorkerManager,
                routingTaskTracker,
                metadataBootstrap != null ? metadataBootstrap.catalogRegistry() : null,
                null, null, 0L);
        var sd = metadataBootstrap != null
                ? SqlE2ETestBase.buildStatementDispatcher(
                        dmlDispatch, queryDispatcherRef, routingTaskTracker,
                        coordinatorResultService, null, routingWorkerManager)
                : null;
        if (sd != null) {
            sd.updateCompiler(compiler);
        }
        var queryDispatcher = new QueryDispatcher(deps, sd);
        queryDispatcherRef.set(queryDispatcher);
        return queryDispatcher;
    }

    /**
     * Rebuilds the virtual-worker routing pipeline after connector registration.
     * The parent's ensureConnectorsRegistered runs in @BeforeEach — after
     * @BeforeAll has already built the initial dispatcher with an empty
     * catalog schema. This hook refreshes the compiler so Calcite can
     * resolve the newly registered catalogs.
     */
    static void refreshRoutingPipeline() {
        if (routingQueryManager == null) return;
        // Preserve existing workers before rebuilding
        var existingWorkers = routingWorkerManager != null
                ? routingWorkerManager.getActiveWorkers() : List.<WorkerInfo>of();
        QueryDispatcher freshDispatcher = buildRoutingDispatcher();
        // Re-register workers that were lost during rebuild
        for (var w : existingWorkers) {
            routingWorkerManager.registerWorker(w);
        }
        routingQueryManager = new QueryManager(routingQueryManagerContext(freshDispatcher));
        var dagEngine = freshDispatcher.dagEngine();
        org.pangrova.coordinator.query.execution.dispatch.dag.StageCompletionCallback stageCallback =
                new org.pangrova.coordinator.query.execution.dispatch.dag.StageCompletionCallback() {
                    @Override
                    public void onTaskTerminal(String queryId, String taskId, String state,
                                               org.pangrova.protocol.rest.error.ErrorDetail error) {
                        dagEngine.onTaskTerminal(queryId, taskId, state, error);
                    }

                    @Override
                    public boolean isQueryComplete(String queryId) {
                        return dagEngine.isQueryComplete(queryId);
                    }
                };
        TaskStatusReceiver receiver = new TaskStatusReceiver(
                new TaskStatusReceiverContext(
                        routingQueryManager, routingTaskTracker, routingWorkerManager,
                        null, null, null,
                        routingQueryManager.getStateJournal(),
                        null, null, stageCallback, null, null));
        int prevPort = coordinatorServer.getPort();
        try { coordinatorServer.stop(); } catch (Exception ignored) {}
        coordinatorServer = new CoordinatorHttpServer(prevPort, receiver, routingQueryManager, null);
        startHttpServer(coordinatorServer);
        configureStatusReporterForPort(coordinatorServer.getPort());
    }

    private static void registerWorkerNode(
            int httpPort, int transportPort, String workerId) {
        WorkerInfo info = new WorkerInfo(
                workerId, "localhost", httpPort, transportPort, sharedResultServerPort,
                HealthState.ACTIVE, MemoryPressure.NONE,
                0.0, 0, WORKER_MEMORY_BYTES,
                Instant.now(), 1.0, null, 2,
                null, List.of(), null, CanonicalWorkerInventory.empty());
        routingWorkerManager.registerWorker(info);
    }

    // -----------------------------------------------------------------------
    // Pipeline execution
    // -----------------------------------------------------------------------

    private static volatile boolean routingPipelineRefreshed = false;

    private static List<TypedRow> executeViaRoutingPipeline(String sql) {
        // Lazy refresh: rebuild the routing pipeline after connector
        // registration. ensureConnectorsRegistered runs in @BeforeEach (parent),
        // but the initial dispatcher was built in @BeforeAll with empty catalogs.
        if (!routingPipelineRefreshed) {
            refreshRoutingPipeline();
            routingPipelineRefreshed = true;
        }
        String queryId = "e2e-multi-" + UUID.randomUUID();
        var session = new org.pangrova.coordinator.query.session.CoordinatorSessionContext(
                queryId, Map.of(), null, null);
        var response = routingQueryManager.submitQuery(queryId, sql, session);
        ResultEndpoint endpoint = response.resultEndpoint();
        if (endpoint == null) {
            throw new IllegalStateException(
                    "Routing query did not publish a result endpoint: " + queryId);
        }
        String canonicalTaskId = hasControlPlaneResult(queryId) ? null
                : requireNativeResultTaskId(endpoint, queryId, routingTaskTracker);
        String workerTaskId = resolveWorkerTaskId(canonicalTaskId);
        List<TypedRow> rows;
        try {
            rows = fetchRoutingResultsViaTransport(endpoint, queryId);
            org.awaitility.Awaitility.await("routing-query-terminal: " + queryId)
                    .atMost(Duration.ofMillis(Long.getLong(
                            "pangrova.e2e.poll.timeout.ms", 600_000L)))
                    .until(() -> routingQueryManager.getQueryState(queryId)
                            .map(state -> state == org.pangrova.coordinator.query
                                    .QueryStateMachine.State.FINISHED
                                    || state == org.pangrova.coordinator.query
                                            .QueryStateMachine.State.FAILED
                                    || state == org.pangrova.coordinator.query
                                            .QueryStateMachine.State.CANCELLED)
                            .orElse(false));
            var terminal = routingQueryManager.getQueryState(queryId).orElseThrow();
            if (terminal != org.pangrova.coordinator.query.QueryStateMachine.State.FINISHED) {
                throw new IllegalStateException(
                        "Routing query did not finish successfully: " + queryId
                                + " state=" + terminal);
            }
        } catch (RuntimeException failure) {
            cancelNativeTaskAfterFetch(canonicalTaskId, workerTaskId);
            throw failure;
        }
        releaseNativeTaskAfterTerminal(workerTaskId);
        return rows;
    }

    private static QueryManagerContext routingQueryManagerContext(QueryDispatcher dispatcher) {
        if (routingAsyncExecutor == null || routingAsyncExecutor.isShutdown()) {
            routingAsyncExecutor = org.pangrova.common.concurrent.ManagedExecutorFactory
                    .newBoundedThreadPool("e2e-routing-async-submit", 4, 128);
        }
        return QueryManagerContext.minimalAsync(
                dispatcher, TestObservability.TRACING,
                org.pangrova.coordinator.query.AsyncSubmitConfig.defaultEnabled(),
                routingAsyncExecutor);
    }

    private static List<TypedRow> fetchRoutingResultsViaTransport(
            ResultEndpoint endpoint, String queryId) {
        if (endpoint == null) {
            throw new IllegalStateException("Missing routing result endpoint: queryId=" + queryId);
        }
        // Direct buffer read — same as parent SqlE2ETestBase
        if (coordinatorResultBuffer != null) {
            byte[] buffered = coordinatorResultBuffer.get(queryId);
            if (buffered != null && buffered.length > 0) {
                return SqlE2ETestBase.decodeResultBytes(buffered);
            }
        }
        // Native SELECT results are served via C++ FlightResultServer.
        // Use routingTaskTracker to resolve the native task ID for this
        // routing pipeline (not the parent's taskTracker).
        String taskId = requireNativeResultTaskId(endpoint, queryId, routingTaskTracker);
        String workerId = routingTaskTracker.getWorkerForTask(taskId);
        var worker = workerId == null ? null : routingWorkerManager.getWorker(workerId);
        if (worker == null || worker.resultServerPort() <= 0
                || !worker.host().equals(endpoint.host())
                || worker.resultServerPort() != endpoint.port()) {
            throw new IllegalStateException("Routing result endpoint does not match task placement: queryId="
                    + queryId + " taskId=" + taskId);
        }
        return SqlE2ETestBase.fetchResultsViaFlight(endpoint, queryId, routingTaskTracker);
    }

    // -----------------------------------------------------------------------
    // Server lifecycle helpers
    // -----------------------------------------------------------------------

    private static void startHttpServer(CoordinatorHttpServer server) {
        try {
            server.start();
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to start CoordinatorHttpServer", e);
        }
    }

    private static void stopCoordinatorQuietly(CoordinatorHttpServer server) {
        if (server == null) return;
        try { server.stop(); } catch (Exception ignored) { }
    }
}
