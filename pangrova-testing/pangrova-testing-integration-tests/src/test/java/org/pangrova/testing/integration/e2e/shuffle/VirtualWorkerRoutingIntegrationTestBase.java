package org.pangrova.testing.integration.e2e.shuffle;

import org.pangrova.coordinator.query.execution.AsyncSubmitConfig;
import org.pangrova.coordinator.http.CoordinatorHttpServer;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
// StageFragmenter removed from prod — QueryDispatcher now takes only (deps, statementDispatcher)
import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.coordinator.query.QueryManagerContext;
import org.pangrova.coordinator.query.task.TaskStatusReceiver;
import org.pangrova.coordinator.query.task.TaskStatusReceiverContext;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.scheduler.core.placement.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.scheduler.api.placement.HealthState;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.scheduler.core.scoring.WorkerPerformanceScorer;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.testing.testkit.TestObservability;
import org.pangrova.client.core.protocol.ResultEndpoint;
import org.pangrova.common.value.MemoryPressure;
import org.pangrova.connector.spi.data.value.TypedRow;
import org.pangrova.testing.testkit.TestCluster;
import org.pangrova.testing.integration.e2e.E2ETestFramework;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.runner.SqlTestRunner;
import org.pangrova.testing.integration.e2e.setup.TestDataSetup;
import org.pangrova.worker.task.RemoteTaskStatusReporter;
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
    private static RemoteTaskStatusReporter routingStatusReporter;
    private static E2ETestFramework routingFramework;
    protected static SqlTestRunner routingSqlRunner;

    protected static SqlTestRunner setupRunner() {
        return java.util.Objects.requireNonNull(sqlRunner, "Shared SQL setup runner is absent");
    }

    protected static TestDataSetup setupData() {
        return java.util.Objects.requireNonNull(dataSetup, "Shared table setup is absent");
    }

    // -----------------------------------------------------------------------
    // Lifecycle: overlay a virtual-worker routing coordinator on the shared worker
    // -----------------------------------------------------------------------

    @BeforeAll
    static void initVirtualWorkerRoutingHarness() {
        isDockerReady();
        initializeRoutingHarness(createRoutingHarness());
    }

    static void initializeRoutingHarness(TestCluster harness) {
        if (routingFramework != null) {
            throw new IllegalStateException("A routing harness is already owned");
        }
        routingFramework = new E2ETestFramework(
                harness, VirtualWorkerRoutingIntegrationTestBase::executeViaRoutingPipeline);
        routingSqlRunner = new SqlTestRunner(routingFramework);
        routingFramework.setup();
    }

    @AfterAll
    static void stopVirtualWorkerRoutingHarness() {
        Throwable primary = null;
        try {
            closeRoutingHarness();
        } catch (RuntimeException | Error failure) {
            primary = failure;
            throw failure;
        } finally {
            try {
                restoreSharedStatusDelivery();
                configureStatusReporter();
            } catch (RuntimeException | Error cleanup) {
                if (primary == null) throw cleanup;
                primary.addSuppressed(cleanup);
            }
        }
    }

    private static void closeRoutingHarness() {
        // Retain each owner until its actual teardown returns so failed cleanup can be retried.
        if (routingFramework != null) {
            routingFramework.teardown();
            routingFramework = null;
        }
        routingSqlRunner = null;
        if (routingDispatcher != null) {
            routingDispatcher.close();
            routingDispatcher = null;
        }
        if (routingAsyncExecutor != null) {
            org.pangrova.common.concurrent.ExecutorDrain.shutdownAndAwait(routingAsyncExecutor, 5000L);
            routingAsyncExecutor = null;
        }
        if (routingStatusReporter != null) {
            routingStatusReporter.close();
            routingStatusReporter = null;
        }
        if (routingQueryManager != null) {
            routingQueryManager.stopPeriodicCleanup();
            routingQueryManager = null;
        }
        stopCoordinator(coordinatorServer);
        coordinatorServer = null;
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

        // Use the same DAG owner for task reports and terminal finalization.
        TaskStatusReceiver receiver = new TaskStatusReceiver(
                new TaskStatusReceiverContext(
                        routingQueryManager, routingTaskTracker, routingWorkerManager,
                        null, null, null,
                        routingQueryManager.getStateJournal(),
                        null, null, dispatcher.dagEngine(), null, null));
        coordinatorServer = new CoordinatorHttpServer(
                0, receiver, routingQueryManager, null);
        startHttpServer(coordinatorServer);

        routingStatusReporter = new RemoteTaskStatusReporter(
                "http://localhost:" + coordinatorServer.getPort(),
                TestObservability.TRACING, E2E_INTERNAL_AUTH_SECRET);
        configureVirtualRoutingStatusDelivery(
                VirtualWorkerRoutingIntegrationTestBase::deliverStatusToOwningCoordinator);

        return () -> stopCoordinator(coordinatorServer);
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
        RemoteTaskDispatcher dispatcher = createE2ETaskDispatcher();
        routingDispatcher = dispatcher;
        SqlCompiler compiler = buildSqlCompiler();
        // Routing assertions exercise SELECT. Setup DML runs through the shared
        // authenticated pipeline; this branch must never report an unwritten row as committed.
        var queryDispatcherRef = new java.util.concurrent.atomic.AtomicReference<QueryDispatcher>();
        org.pangrova.coordinator.query.execution.dispatch.NativeDmlCompileFunction dmlDispatch =
                (sm, session, stmt) -> {
                    throw new UnsupportedOperationException(
                            "Virtual routing harness accepts reads only; prepare data through the shared pipeline");
                };
        var authority = e2eAccessControl();
        var deps = QueryDispatcherContext.withDefaults(
                compiler,
                new org.pangrova.coordinator.query.security.AccessControlEnforcer(authority),
                new org.pangrova.planner.security.ColumnMaskApplier(authority),
                new org.pangrova.planner.security.RowFilterApplier(authority),
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
        wireE2EOwnership(queryDispatcher);
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
        TaskStatusReceiver receiver = new TaskStatusReceiver(
                new TaskStatusReceiverContext(
                        routingQueryManager, routingTaskTracker, routingWorkerManager,
                        null, null, null,
                        routingQueryManager.getStateJournal(),
                        null, null, freshDispatcher.dagEngine(), null, null));
        int prevPort = coordinatorServer.getPort();
        try { coordinatorServer.stop(); } catch (Exception ignored) {}
        coordinatorServer = new CoordinatorHttpServer(prevPort, receiver, routingQueryManager, null);
        startHttpServer(coordinatorServer);
        routingStatusReporter.updateTarget("http://localhost:" + coordinatorServer.getPort());
    }

    private static void registerWorkerNode(
            int httpPort, int transportPort, String workerId) {
        WorkerInfo info = new WorkerInfo(
                workerId, workerAdvertisedHost(), httpPort, transportPort, sharedResultServerPort,
                HealthState.ACTIVE, MemoryPressure.NONE,
                0.0, 0, WORKER_MEMORY_BYTES,
                Instant.now(), 1.0, null, 2,
                null, List.of(), null,
                java.util.Objects.requireNonNull(workerManager.getWorker(WORKER_ID)).capabilityInventory(),
                workerManager.getWorker(WORKER_ID).workerExecutionIdentity());
        routingWorkerManager.registerWorker(info);
    }

    /**
     * This suite deliberately routes two logical identities to one physical Worker.
     * Project only the REST worker label for tasks owned by this routing coordinator;
     * all canonical Native attempt and coverage evidence is left intact. It does not
     * prove worker identity authentication or cross-process execution.
     */
    private static void deliverStatusToOwningCoordinator(
            org.pangrova.protocol.rest.task.TaskStatusReport report) {
        if (routingTaskTracker.getWorkerForTask(report.taskId()) == null) {
            reportToSharedCoordinator(report);
        } else {
            routingStatusReporter.report(projectVirtualWorkerIdentity(report));
        }
    }

    private static org.pangrova.protocol.rest.task.TaskStatusReport projectVirtualWorkerIdentity(
            org.pangrova.protocol.rest.task.TaskStatusReport report) {
        var tracker = routingTaskTracker;
        String assignedWorker = tracker == null ? null : tracker.getWorkerForTask(report.taskId());
        if (assignedWorker == null || assignedWorker.equals(report.workerId())) {
            return report;
        }
        return new org.pangrova.protocol.rest.task.TaskStatusReport(
                report.taskId(), report.queryId(), report.stageId(), report.partitionId(),
                report.taskType(), assignedWorker, report.state(), report.stats(),
                report.error(), report.writeResult(), report.reportSequence(),
                report.timestampMs(), report.errorCode(), report.errorMetadata(),
                report.attempt(), report.canonicalTaskReport());
    }

    // -----------------------------------------------------------------------
    // Pipeline execution
    // -----------------------------------------------------------------------

    private static volatile boolean routingPipelineRefreshed = false;

    private static void ensureRoutingPipelineRefreshed() {
        if (routingPipelineRefreshed) return;
        synchronized (VirtualWorkerRoutingIntegrationTestBase.class) {
            if (routingPipelineRefreshed) return;
            refreshRoutingPipeline();
            routingPipelineRefreshed = true;
        }
    }

    private static List<TypedRow> executeViaRoutingPipeline(String sql) {
        // Lazy refresh: rebuild the routing pipeline after connector
        // registration. ensureConnectorsRegistered runs in @BeforeEach (parent),
        // but the initial dispatcher was built in @BeforeAll with empty catalogs.
        ensureRoutingPipelineRefreshed();
        String queryId = "e2e-multi-" + UUID.randomUUID();
        var session = new org.pangrova.coordinator.query.session.CoordinatorSessionContext(
                queryId, Map.of(), e2eUserIdentity(), e2eTenantId());
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
                org.pangrova.coordinator.query.execution.AsyncSubmitConfig.defaultEnabled(),
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

    private static void stopCoordinator(CoordinatorHttpServer server) {
        if (server == null) return;
        try {
            server.stop();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while stopping the routing coordinator", interrupted);
        } catch (Exception failure) {
            throw new IllegalStateException("Routing coordinator cleanup did not complete", failure);
        }
    }
}
