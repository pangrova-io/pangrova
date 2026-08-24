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
import org.pangrova.spi.transport.DataTicket;
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
import org.pangrova.testing.e2e.E2ETestFramework;
import org.pangrova.testing.e2e.SqlE2ETestBase;
import org.pangrova.testing.e2e.runner.SqlTestRunner;
import org.pangrova.testing.e2e.setup.TestDataSetup;
import org.pangrova.worker.transport.TransportClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.io.Closeable;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Abstract base for multi-worker (distributed shuffle) tests.
 *
 * <p>Registers multiple virtual workers with the coordinator, all pointing
 * to the parent's real WorkerHttpServer and TransportServer. This triggers
 * shuffle partitioning paths (RangePartitioner / RoundRobinPartitioner)
 * in the coordinator/planner while reusing the single real worker for
 * actual task execution.
 *
 * <p>Requirements: 6.1-6.6
 */
@Tag("multi-worker")
public abstract class MultiWorkerTestBase extends SqlE2ETestBase {

    private static final int WORKER_COUNT = 2;
    private static final long WORKER_MEMORY_BYTES = 4L * 1024 * 1024 * 1024;

    private static CoordinatorHttpServer coordinatorServer;
    private static QueryManager multiQueryManager;
    private static WorkerManager multiWorkerManager;
    private static CoordinatorTaskTracker multiTaskTracker;
    private static RemoteTaskDispatcher multiDispatcher;

    // -----------------------------------------------------------------------
    // Lifecycle: overlay a multi-worker coordinator on top of parent's worker
    // -----------------------------------------------------------------------

    @BeforeAll
    static void initMultiWorkerCluster() {
        if (!isDockerReady()) return;
        testCluster = createMultiWorkerCluster();
        framework = new E2ETestFramework(
                testCluster, MultiWorkerTestBase::executeViaMultiWorkerPipeline);
        sqlRunner = new SqlTestRunner(framework);
        dataSetup = new TestDataSetup(sqlRunner);
        framework.setup();
    }

    @AfterAll
    static void stopMultiWorkerCluster() {
        if (framework != null) {
            framework.teardown();
        }
        if (multiDispatcher != null) {
            multiDispatcher.close();
            multiDispatcher = null;
        }
        stopCoordinatorQuietly(coordinatorServer);
        coordinatorServer = null;
        multiQueryManager = null;
        multiWorkerManager = null;
        multiTaskTracker = null;
        multiPipelineRefreshed = false;
    }

    // -----------------------------------------------------------------------
    // Multi-worker cluster wiring
    // -----------------------------------------------------------------------

    private static TestCluster createMultiWorkerCluster() {
        return new TestCluster(
                MultiWorkerTestBase::startCoordinator,
                MultiWorkerTestBase::registerVirtualWorkers);
    }

    private static Closeable startCoordinator() {
        multiTaskTracker = new CoordinatorTaskTracker();
        QueryDispatcher dispatcher = buildMultiWorkerDispatcher();
        multiQueryManager = new QueryManager(
                QueryManagerContext.minimal(dispatcher, TestObservability.TRACING));

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
                        multiQueryManager, multiTaskTracker, multiWorkerManager,
                        null, null, null,
                        org.pangrova.coordinator.coordination.inmemory.InMemoryQueryStateJournal.INSTANCE,
                        null, null, stageCallback, null, null));
        coordinatorServer = new CoordinatorHttpServer(
                0, receiver, multiQueryManager, null);
        startHttpServer(coordinatorServer);

        // Point the worker's status reporter to this multi-worker coordinator
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
    private static Closeable registerVirtualWorkers() {
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

    private static QueryDispatcher buildMultiWorkerDispatcher() {
        multiWorkerManager = new WorkerManager();
        RemoteTaskDispatcher dispatcher = new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), TestObservability.TRACING);
        multiDispatcher = dispatcher;
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
                multiWorkerManager,
                multiTaskTracker,
                metadataBootstrap != null ? metadataBootstrap.catalogRegistry() : null,
                null, null, coordinatorResultService, 0L);
        var sd = metadataBootstrap != null
                ? SqlE2ETestBase.buildStatementDispatcher(dmlDispatch, queryDispatcherRef, multiTaskTracker, coordinatorResultService)
                : null;
        if (sd != null) {
            sd.updateCompiler(compiler);
        }
        var queryDispatcher = new QueryDispatcher(deps, sd);
        queryDispatcherRef.set(queryDispatcher);
        return queryDispatcher;
    }

    /**
     * Rebuilds the multi-worker query pipeline after connector registration.
     * The parent's ensureConnectorsRegistered runs in @BeforeEach — after
     * @BeforeAll has already built the initial dispatcher with an empty
     * catalog schema. This hook refreshes the compiler so Calcite can
     * resolve the newly registered catalogs.
     */
    static void refreshMultiWorkerPipeline() {
        if (multiQueryManager == null) return;
        // Preserve existing workers before rebuilding
        var existingWorkers = multiWorkerManager != null
                ? multiWorkerManager.getActiveWorkers() : List.<WorkerInfo>of();
        QueryDispatcher freshDispatcher = buildMultiWorkerDispatcher();
        // Re-register workers that were lost during rebuild
        for (var w : existingWorkers) {
            multiWorkerManager.registerWorker(w);
        }
        multiQueryManager = new QueryManager(
                QueryManagerContext.minimal(freshDispatcher, TestObservability.TRACING));
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
                        multiQueryManager, multiTaskTracker, multiWorkerManager,
                        null, null, null,
                        org.pangrova.coordinator.coordination.inmemory.InMemoryQueryStateJournal.INSTANCE,
                        null, null, stageCallback, null, null));
        int prevPort = coordinatorServer.getPort();
        try { coordinatorServer.stop(); } catch (Exception ignored) {}
        coordinatorServer = new CoordinatorHttpServer(prevPort, receiver, multiQueryManager, null);
        startHttpServer(coordinatorServer);
        configureStatusReporterForPort(coordinatorServer.getPort());
    }

    private static void registerWorkerNode(
            int httpPort, int transportPort, String workerId) {
        WorkerInfo info = new WorkerInfo(
                workerId, "localhost", httpPort, transportPort, sharedResultServerPort,
                HealthState.ACTIVE, MemoryPressure.NONE,
                0.0, 0, WORKER_MEMORY_BYTES,
                Instant.now(), 1.0, null, 1, List.of(),
                null, List.of());
        multiWorkerManager.registerWorker(info);
    }

    // -----------------------------------------------------------------------
    // Pipeline execution
    // -----------------------------------------------------------------------

    private static volatile boolean multiPipelineRefreshed = false;

    private static List<TypedRow> executeViaMultiWorkerPipeline(String sql) {
        // Lazy refresh: rebuild the multi-worker pipeline after connector
        // registration. ensureConnectorsRegistered runs in @BeforeEach (parent),
        // but the initial dispatcher was built in @BeforeAll with empty catalogs.
        if (!multiPipelineRefreshed) {
            refreshMultiWorkerPipeline();
            multiPipelineRefreshed = true;
        }
        String queryId = "e2e-multi-" + UUID.randomUUID();
        var session = new org.pangrova.coordinator.query.CoordinatorSessionContext(
                queryId, Map.of(), null, null);
        var response = multiQueryManager.submitQuery(queryId, sql, session);
        ResultEndpoint endpoint = response.resultEndpoint();
        return fetchResultsViaTransport(endpoint, queryId);
    }

    private static List<TypedRow> fetchResultsViaTransport(
            ResultEndpoint endpoint, String queryId) {
        if (endpoint == null) {
            return List.of();
        }
        // Direct buffer read — same as parent SqlE2ETestBase
        if (coordinatorResultBuffer != null) {
            byte[] buffered = coordinatorResultBuffer.get(queryId);
            if (buffered != null && buffered.length > 0) {
                return SqlE2ETestBase.decodeResultBytes(buffered);
            }
        }
        // Native SELECT results are served via C++ FlightResultServer.
        // Use multiTaskTracker to resolve the native task ID for this
        // multi-worker pipeline (not the parent's taskTracker).
        if (sharedResultServerPort > 0 && multiTaskTracker != null) {
            return SqlE2ETestBase.fetchResultsViaFlight(queryId, multiTaskTracker);
        }
        DataTicket ticket = new DataTicket(
                queryId, "result", 0, DataTicket.TicketType.RESULT);
        try (TransportClient client = new TransportClient(
                endpoint.host(), endpoint.port())) {
            byte[] resultBytes = client.doGet(ticket);
            return SqlE2ETestBase.decodeResultBytes(resultBytes);
        }
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
