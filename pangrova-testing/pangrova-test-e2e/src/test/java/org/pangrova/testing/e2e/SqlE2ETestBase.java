package org.pangrova.testing.e2e;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.pangrova.coordinator.http.CoordinatorHttpServer;
import org.pangrova.coordinator.metadata.MetadataBootstrap;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
// StageFragmenter removed from prod — QueryDispatcher now takes only (deps, statementDispatcher)
import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.coordinator.query.QueryManagerContext;
import org.pangrova.coordinator.query.task.TaskStatusReceiver;
import org.pangrova.coordinator.query.task.TaskStatusReceiverContext;
import org.pangrova.coordinator.query.execution.dispatch.dag.StageCompletionCallback;
import org.pangrova.coordinator.result.CoordinatorResultBuffer;
import org.pangrova.coordinator.result.CoordinatorResultService;
import org.pangrova.coordinator.result.CoordinatorResultWriter;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.spi.transport.DataTicket;
import org.pangrova.coordinator.scheduler.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.HealthState;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.coordinator.worker.WorkerPerformanceScorer;
import org.pangrova.metadata.store.JdbcMetadataStore;
import org.pangrova.metadata.store.MetadataStore;
import org.pangrova.observability.metrics.CoordinatorMetrics;
import org.pangrova.observability.metrics.WorkerMetrics;
import org.pangrova.observability.tracing.TracingHttpFilter;
import org.pangrova.testing.TestObservability;
import org.pangrova.operations.bootstrap.BootstrapPhase;
import org.pangrova.operations.bootstrap.HealthCheckEndpoint;
import org.pangrova.operations.bootstrap.SystemBootstrapSequence;
import org.pangrova.planner.PangrovaPlanner;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.protocol.rest.query.ResultEndpoint;
import org.pangrova.protocol.rest.query.SubmitQueryRequest;
import org.pangrova.protocol.rest.query.SubmitQueryResponse;
import org.pangrova.resource.MemoryPressure;
import org.pangrova.resource.ResourceManager;
import org.pangrova.resource.memory.MemoryArbitrator;
import org.pangrova.resource.spill.SpillManager;
import org.pangrova.resource.thread.ThreadBudgetTracker;
import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.spi.execution.TaskExecutor;
import org.pangrova.spi.observability.MetricsProvider;
import org.pangrova.spi.observability.TracingProvider;
import org.pangrova.planner.substrait.SubstraitConverter;
import org.pangrova.planner.substrait.SubstraitPlanSerializer;
import org.pangrova.testing.ContainerTestBase;
import org.pangrova.testing.TestCluster;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.pangrova.testing.e2e.runner.SqlTestRunner;
import org.pangrova.testing.e2e.setup.MetadataSchemaInitializer;
import org.pangrova.testing.e2e.setup.TestCatalogRegistrar;
import org.pangrova.testing.e2e.setup.TestConnectorHandleFactory;
import org.pangrova.testing.e2e.setup.TestDataSetup;
import org.pangrova.worker.exchange.NativeExchangeTransport;
import org.pangrova.worker.execution.NativeTaskExecutor;
import org.pangrova.worker.jni.NativeRuntime;
import org.pangrova.worker.http.WorkerHttpServer;
import org.pangrova.worker.task.memory.MemoryRefs;
import org.pangrova.worker.task.TransportRefs;
import org.pangrova.worker.task.RemoteTaskStatusReporter;
import org.pangrova.worker.task.TaskManager;
import org.pangrova.worker.task.TaskManagerContext;
import org.pangrova.worker.transport.TransportClient;
import org.pangrova.worker.transport.TransportServer;
import org.apache.calcite.jdbc.CalciteSchema;
import net.jqwik.api.lifecycle.AfterContainer;
import net.jqwik.api.lifecycle.BeforeContainer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;

import java.io.Closeable;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Abstract base for SQL E2E integration tests.
 *
 * <p>Manages PostgreSQL container (metadata store via TestContainers),
 * native library verification, and a real HTTP communication TestCluster
 * (Coordinator + Worker in same JVM, communicating via real HTTP on random ports).
 *
 * <p>Iceberg and Paimon use local filesystem ({@code @TempDir}) for storage.
 * Subclasses configure connectors via {@link #connectorConfigs()}.
 */
@Tag("integration")
@Testcontainers
public abstract class SqlE2ETestBase extends ContainerTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(SqlE2ETestBase.class);

    /**
     * Extends parent's skip logic to also skip jqwik properties when
     * FlightResultServer is unavailable (Arrow Flight not linked).
     * Without this, jqwik properties pass the Docker check but fail with
     * empty SELECT results because native query output cannot be fetched.
     */
    @Override
    public net.jqwik.api.lifecycle.SkipExecutionHook.SkipResult shouldBeSkipped(
            net.jqwik.api.lifecycle.LifecycleContext context) {
        var parentResult = super.shouldBeSkipped(context);
        if (parentResult.isSkipped()) {
            return parentResult;
        }
        // Skip jqwik properties cleanly when the C++ native lib isn't packaged
        // on the classpath, or when a prior bootstrap attempt revealed an ABI
        // mismatch with current Java declarations (stale CMake build).
        // jqwik @BeforeContainer does not honor JUnit Assumptions.abort() —
        // it converts to ERROR — so the skip must happen here before
        // bootstrapTestEnvironment is invoked.
        if (!org.pangrova.worker.jni.NativeLibraryLoader.isFunctional()) {
            return net.jqwik.api.lifecycle.SkipExecutionHook.SkipResult.skip(
                    "Native library not built or ABI mismatch — skipping E2E property tests");
        }
        if (sharedResultServerPort <= 0) {
            return net.jqwik.api.lifecycle.SkipExecutionHook.SkipResult.skip(
                    "FlightResultServer not available (Arrow Flight not linked)");
        }
        return net.jqwik.api.lifecycle.SkipExecutionHook.SkipResult.doNotSkip();
    }

    protected static final String WORKER_ID = "e2e-worker-1";
    private static final long WORKER_MEMORY_BYTES = 2L * 1024 * 1024 * 1024; // 2GB — balances Velox memory needs vs OS process limits
    private static final long POLL_INTERVAL_MS = 50;
    private static final long POLL_TIMEOUT_MS =
            Long.parseLong(System.getProperty("pangrova.e2e.poll.timeout.ms", "600000"));

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(com.fasterxml.jackson.databind.DeserializationFeature.USE_LONG_FOR_INTS, true);
    private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();

    @SuppressWarnings("resource")
    private static PostgreSQLContainer<?> POSTGRES;

    /** Shared directory for Iceberg and Paimon local filesystem storage.
     *  Must persist across ALL test classes in the single-JVM session (forkCount=1).
     *  Using @TempDir on a static field creates a new directory per class, which
     *  causes Velox to fail with file-not-found when reading tables written by
     *  a previous class whose temp dir was cleaned up. */
    protected static Path tempDir;

    protected static TestCluster testCluster;
    protected static E2ETestFramework framework;
    protected static SqlTestRunner sqlRunner;
    protected static TestDataSetup dataSetup;

    // nativeEngineLoaded field removed — NativeRuntime.isInitialized() is the
    // single source of truth for native runtime state (Task 11).

    // --- Cluster components (static for @BeforeAll/@AfterAll lifecycle) ---
    // Protected: resilience tests need access to simulate worker failures.

    protected static CoordinatorHttpServer coordinatorHttpServer;
    protected static WorkerHttpServer workerHttpServer;
    protected static TransportServer coordinatorTransportServer;
    protected static QueryManager queryManager;
    protected static CoordinatorTaskTracker taskTracker;
    protected static WorkerManager workerManager;
    protected static MetadataBootstrap metadataBootstrap;
    private static RemoteTaskStatusReporter statusReporter;
    private static org.pangrova.coordinator.query.task.NativeWriteCommitTracker writeCommitTracker;

    /** Default schema path for Calcite table resolution (e.g. ["iceberg_catalog", "default"]). */
    private static volatile List<String> defaultSchemaPath = List.of("");

    // Observability — NoOp fallback when OTel is not configured
    private static final TracingProvider TRACING_PROVIDER = TestObservability.TRACING;
    private static final MetricsProvider METRICS_PROVIDER = TestObservability.METRICS;
    private static final TracingHttpFilter TRACING_HTTP_FILTER =
            new TracingHttpFilter(TRACING_PROVIDER);

    // Operations — DynamicConfigService removed (R1.4)

    // Health readiness flags — set during bootstrap phases
    private static final AtomicBoolean coordinatorHttpReady = new AtomicBoolean(false);
    private static final AtomicBoolean workerHttpReady = new AtomicBoolean(false);
    private static final AtomicBoolean workerTransportReady = new AtomicBoolean(false);
    private static final AtomicBoolean workerNativeReady = new AtomicBoolean(false);

    // Coordinator-local result buffer — serves constant-fold and DDL/DML results via transport.
    protected static volatile CoordinatorResultBuffer coordinatorResultBuffer;
    protected static volatile CoordinatorResultService coordinatorResultService;
    private static ScheduledExecutorService resultBufferExecutor;

    // Shared MaterializedViewManager — used by both DT DDL executor and planner rewriter
    private static volatile org.pangrova.planner.mv.MaterializedViewManager sharedMvManager;

    // Shared DynamicTableRewriter — used by both planner and refresh executor for target overrides
    private static volatile org.pangrova.planner.mv.DynamicTableRewriter sharedDtRewriter;

    // Configurable CDC lifecycle manager — subclasses set via setCdcAvailable() in @BeforeAll
    protected static volatile boolean cdcAvailableFlag = false;

    private static volatile boolean infrastructureInitialized;

    // Worker-side resources that must be explicitly closed in teardown.
    // Without tracking these, each test class leaks thread pools and native handles.
    private static volatile TaskManager workerTaskManagerRef;
    // The native write executor registered with the C++ bridge. We keep a reference
    // so that, once the worker TaskManager is created, we can wire it for per-task
    // commit metadata draining (TaskStatusCollector relies on this to surface
    // commit metadata to the coordinator).
    private static volatile org.pangrova.worker.jni.write.NativeWriteExecutor sharedNativeWriteExecutor;
    // Additional resources that leak if not closed between test classes.
    private static volatile org.apache.arrow.memory.RootAllocator coordinatorArrowAllocator;
    private static volatile org.pangrova.indexing.storage.IndexCache sharedIndexCache;
    private static volatile RemoteTaskStatusReporter sharedStatusReporter;
    // Coordinator-side resources that leak HttpClient thread pools if not closed.
    private static volatile RemoteTaskDispatcher sharedTaskDispatcher;
    private static volatile org.pangrova.worker.memory.NativeMemoryCoordinator nativeMemoryCoordinatorRef;
    private static volatile org.pangrova.coordinator.query.statement.DdlExecutor sharedDdlExecutor;
    protected static volatile int sharedResultServerPort;

    // State store DataSource — shared PostgreSQL backend for journal, registry, tenant, etc.
    // Created alongside metadata DataSource during bootstrap; exposed for subclass E2E tests.
    private static volatile DataSource stateStoreDataSource;

    // Tracks which test classes have already registered their connectors
    private static final java.util.Set<String> registeredClasses =
            java.util.Collections.synchronizedSet(new java.util.HashSet<>());

    // Tracks which catalog names have been registered this JVM session
    // (used only to skip redundant registration within the SAME class —
    // e.g. when a class has both connectorConfigs() and @BeforeAll registerTestConnectors())
    private static final java.util.Set<String> lastRegisteredCatalogs =
            java.util.Collections.synchronizedSet(new java.util.HashSet<>());

    /**
     * jqwik property guard: skips property trials when infrastructure is unavailable.
     * jqwik's SkipExecutionHook is unreliable across module boundaries; this
     * BeforeProperty hook provides a guaranteed skip via Assume.that().
     */
    @net.jqwik.api.lifecycle.BeforeProperty
    void assumeInfrastructureReady() {
        net.jqwik.api.Assume.that(isDockerReady() && sqlRunner != null);
    }

    /**
     * Auto-registers connectors on first test execution per subclass.
     * Subclasses only need to implement {@link #connectorConfigs()} — no manual
     * {@code registerTestConnectors()} call required in subclass @BeforeAll.
     */
    @BeforeEach
    void ensureConnectorsRegistered() {
        org.junit.jupiter.api.Assumptions.assumeTrue(isDockerReady(),
                "Docker is not available — skipping container-based tests");
        org.junit.jupiter.api.Assumptions.assumeTrue(sharedResultServerPort > 0,
                "FlightResultServer not available (Arrow Flight not linked) — "
                + "native SELECT results cannot be fetched");
        Class<?> registrationKey = getClass();
        while (registrationKey.getEnclosingClass() != null
               && SqlE2ETestBase.class.isAssignableFrom(registrationKey.getEnclosingClass())) {
            registrationKey = registrationKey.getEnclosingClass();
        }
        String className = registrationKey.getName();
        if (registeredClasses.add(className)) {
            registerTestConnectors(connectorConfigs());
        }
    }

    @AfterEach
    void releaseFinishedNativeExecutors() {
        if (workerTaskManagerRef != null) {
            workerTaskManagerRef.releaseTerminalTaskExecutors();
        }
    }

    // -----------------------------------------------------------------------
    // Lifecycle: @BeforeAll — single method to guarantee execution order
    // (JUnit 5 does not guarantee ordering of multiple @BeforeAll methods)
    // -----------------------------------------------------------------------

    @BeforeAll
    @BeforeContainer
    static void bootstrapTestEnvironment() {
        System.setProperty("pangrova.http.max-requests-per-minute", "100000");

        if (infrastructureInitialized) {
            if (!SharedTestContainers.POSTGRES.isRunning()) {
                LOG.warn("event=pg_container_died action=restarting");
                int maxRetries = 3;
                Exception lastError = null;
                for (int attempt = 1; attempt <= maxRetries; attempt++) {
                    try {
                        SharedTestContainers.POSTGRES.start();
                        POSTGRES = SharedTestContainers.POSTGRES;
                        // Poll SELECT 1 for up to 20s before declaring the
                        // container ready. testcontainers' isRunning() only
                        // checks Docker state, but PG may still be replaying
                        // WAL and reject queries.
                        waitForPostgresReady(Duration.ofSeconds(20));
                        reinitializeMetadataAfterPgRestart();
                        lastError = null;
                        break;
                    } catch (Exception e) {
                        lastError = e;
                        LOG.warn("event=pg_restart_attempt attempt={} error={}", attempt, e.getMessage());
                        try {
                            // Retry-with-backoff: the next attempt waits proportionally
                            // longer. Awaitility is unsuitable here because the wait is
                            // an unconditional backoff between independent restart
                            // attempts, not a poll for an external condition.
                            // CHECKSTYLE.OFF: RegexpSinglelineJava - intentional retry backoff
                            Thread.sleep(1000L * attempt);
                            // CHECKSTYLE.ON: RegexpSinglelineJava
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                }
                if (lastError != null) {
                    throw new IllegalStateException(
                            "Shared PostgreSQL container died and restart failed after " + maxRetries + " attempts: "
                            + lastError.getMessage(), lastError);
                }
            }
            if (coordinatorHttpServer == null || coordinatorHttpServer.getPort() <= 0) {
                LOG.warn("event=coordinator_http_dead action=full_rebuild");
                rebuildQueryPipeline();
            }
            return;
        }

        ContainerTestBase.ensureDockerAvailable();
        if (!isDockerReady()) {
            return;
        }

        if (tempDir == null) {
            try {
                java.nio.file.Path tmpRoot = java.nio.file.Path.of(System.getProperty("java.io.tmpdir"));
                java.nio.file.Files.createDirectories(tmpRoot);
                tempDir = java.nio.file.Files.createTempDirectory("pangrova-e2e-shared");
            } catch (java.io.IOException e) {
                throw new IllegalStateException("Failed to create shared temp directory", e);
            }
        }

        org.junit.jupiter.api.Assumptions.assumeTrue(
                org.pangrova.worker.jni.NativeLibraryLoader.isAvailable(),
                "Native library not built (run `cmake --build` + `mvn process-resources -pl pangrova-worker`); skipping E2E tests");
        if (!NativeRuntime.isInitialized()) {
            byte[] configBytes = org.pangrova.protocol.v1.NativeInitConfig.newBuilder()
                    .setMaxMemoryBytes(WORKER_MEMORY_BYTES)
                    .setIoThreadCount(4)
                    .build()
                    .toByteArray();
            try {
                var coordinator = new org.pangrova.worker.memory.NativeMemoryCoordinator(WORKER_MEMORY_BYTES);
                nativeMemoryCoordinatorRef = coordinator;
                NativeRuntime.initNativeRuntime(configBytes, coordinator);
            } catch (UnsatisfiedLinkError e) {
                org.pangrova.worker.jni.NativeLibraryLoader.markUnsupported();
                org.junit.jupiter.api.Assumptions.abort(
                        "Native library ABI mismatch (rebuild via `cmake --build` + "
                                + "`mvn process-resources -pl pangrova-worker`): " + e.getMessage());
            }
        }

        POSTGRES = SharedTestContainers.POSTGRES;
        sharedNetwork = SharedTestContainers.NETWORK;

        if (POSTGRES == null || !POSTGRES.isRunning()) {
            throw new IllegalStateException(
                    "SharedTestContainers failed to start PostgreSQL (AVAILABLE="
                    + SharedTestContainers.AVAILABLE + "). Check stderr for details.");
        }

        testCluster = createTestCluster();
        framework = new E2ETestFramework(testCluster, SqlE2ETestBase::executeViaRealPipeline);
        sqlRunner = new SqlTestRunner(framework);
        dataSetup = new TestDataSetup(sqlRunner);
        framework.setup();
        rebuildQueryPipeline();

        infrastructureInitialized = true;
        Runtime.getRuntime().addShutdownHook(new Thread(
                SqlE2ETestBase::shutdownSharedInfrastructure,
                "e2e-infra-cleanup"));
    }

    // -----------------------------------------------------------------------
    // Lifecycle: @AfterAll — single method to guarantee teardown order
    // (reverse of bootstrap: cluster → containers → native)
    // -----------------------------------------------------------------------

    @AfterAll
    @AfterContainer
    static void teardownTestEnvironment() {
        // Only reset per-class tracking state. Catalogs and shared managers
        // survive across test classes (forkCount=1, single JVM). Each class
        // re-registers its connectors via ensureConnectorsRegistered() which
        // triggers a pipeline rebuild with the correct Calcite schema.
        // Full cleanup happens in shutdownSharedInfrastructure() at JVM exit.
        resetPerClassState();
    }

    private static void shutdownSharedInfrastructure() {
        if (framework != null) framework.teardown();
        stopQuietly(workerHttpServer);
        stopCoordinatorTransportQuietly();
        stopQuietly(coordinatorHttpServer);
        if (resultBufferExecutor != null) resultBufferExecutor.shutdownNow();
        if (coordinatorResultBuffer != null) coordinatorResultBuffer.close();
        if (metadataBootstrap != null) {
            try {
                metadataBootstrap.shutdown();
            } catch (Exception e) {
                LOG.warn("event=metadata_shutdown_error error={}", e.getMessage());
            }
        }
        if (workerTaskManagerRef != null) {
            try {
                workerTaskManagerRef.close();
            } catch (Exception e) {
                LOG.warn("event=task_manager_close_error error={}", e.getMessage());
            }
        }
        closeQuietly(coordinatorArrowAllocator, "arrow_allocator");
        closeQuietly(sharedStatusReporter, "status_reporter");
        closeQuietly(sharedTaskDispatcher, "task_dispatcher");
        if (queryManager != null) queryManager.stopPeriodicCleanup();
        if (SharedTestContainers.POSTGRES != null) SharedTestContainers.POSTGRES.stop();
        if (SharedTestContainers.NETWORK != null) SharedTestContainers.NETWORK.close();
    }

    private static void resetPerClassState() {
        registeredClasses.clear();
        lastRegisteredCatalogs.clear();
        defaultSchemaPath = List.of("");
        cdcAvailableFlag = false;
        drainNativeTasksBetweenClasses();
    }

    /**
     * Cancels all running native tasks and cleans up terminal ones between
     * test classes. Without this, result tasks stuck in RUNNING (waiting for
     * DoGet) accumulate across test classes in the single-JVM session and
     * exhaust the Velox folly executor thread pool, causing subsequent
     * queries to fail with NOT_FOUND because new tasks never get scheduled.
     */
    private static void drainNativeTasksBetweenClasses() {
        if (workerTaskManagerRef == null) {
            return;
        }
        try {
            int drained = workerTaskManagerRef.cancelAllRunningTasks();
            if (drained > 0) {
                LOG.info("event=inter_class_drain drained={}", drained);
                // F-E2E-02 fix: instead of fixed Thread.sleep(2000), poll the
                // worker task manager until all cancelled tasks reach a terminal
                // state OR a 15s ceiling elapses (F-DOGET-04: increased from 5s
                // to give cancel+release sufficient time when folly threads are
                // under heavy load). Awaitility avoids over-waiting when cleanup
                // is fast and provides a structured failure surface when it stalls.
                try {
                    org.awaitility.Awaitility.await("native-task-drain")
                            .atMost(java.time.Duration.ofSeconds(15))
                            .pollInterval(java.time.Duration.ofMillis(100))
                            .pollDelay(java.time.Duration.ZERO)
                            .ignoreExceptions()
                            .until(() -> workerTaskManagerRef.cancelAllRunningTasks() == 0);
                } catch (org.awaitility.core.ConditionTimeoutException ignored) {
                    LOG.warn("event=inter_class_drain_timeout note=tasks_still_running");
                }
            }
        } catch (Exception e) {
            LOG.warn("event=inter_class_drain_error error={}", e.getMessage());
        }
        if (nativeMemoryCoordinatorRef != null) {
            nativeMemoryCoordinatorRef.resetBudget();
        }
    }

    // -----------------------------------------------------------------------
    // Subclass extension point
    // -----------------------------------------------------------------------

    /** Override to provide connector configurations for the test suite. */
    protected abstract List<ConnectorTestConfig> connectorConfigs();

    /**
     * Registers connector catalogs and rebuilds the query pipeline.
     * Must be called from subclass @BeforeAll after base class initialization completes.
     * Catalogs are registered via CatalogRegistry; Calcite schema and orchestrator are rebuilt.
     */
    protected static void registerTestConnectors(List<ConnectorTestConfig> configs) {
        if (!isDockerReady()) {
            return;
        }
        if (configs.isEmpty()) {
            // Register a "default" catalog so unqualified table/schema references resolve
            configs = List.of(ConnectorTestConfig.icebergLocal(
                    "default", "default", icebergWarehouse().toString()));
        }
        // Ensure a "default" catalog always exists for unqualified references
        boolean hasDefault = configs.stream().anyMatch(c -> "default".equals(c.catalogName()));
        if (!hasDefault) {
            var mutable = new java.util.ArrayList<>(configs);
            mutable.add(ConnectorTestConfig.icebergLocal(
                    "default", "default", icebergWarehouse().toString()));
            configs = mutable;
        }
        ConnectorTestConfig first = configs.get(0);
        defaultSchemaPath = List.of(first.catalogName(), first.schemaName());
        java.util.Set<String> catalogNames = new java.util.LinkedHashSet<>();
        for (ConnectorTestConfig config : configs) {
            catalogNames.add(config.catalogName());
        }
        if (lastRegisteredCatalogs.equals(catalogNames)) {
            LOG.debug("Catalogs {} already registered for this class — skipping redundant rebuild", catalogNames);
            return;
        }
        var registry = metadataBootstrap.catalogRegistry();
        for (ConnectorTestConfig config : configs) {
            try {
                registry.unregister(config.catalogName());
            } catch (Exception e) {
                // Catalog may not exist yet — that's fine
            }
        }
        TestCatalogRegistrar.registerAll(registry, configs);
        lastRegisteredCatalogs.clear();
        lastRegisteredCatalogs.addAll(catalogNames);
        rebuildQueryPipeline();
        LOG.info("Registered {} test connectors and rebuilt query pipeline", configs.size());
    }

    private static void rebuildQueryPipeline() {
        if (queryManager != null) {
            queryManager.stopPeriodicCleanup();
        }
        QueryDispatcher dispatcher = buildQueryDispatcher();
        queryManager = new QueryManager(QueryManagerContext.minimal(dispatcher, TRACING_PROVIDER));
        var dagEngine = dispatcher.dagEngine();
        StageCompletionCallback stageCallback = new StageCompletionCallback() {
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
                new TaskStatusReceiverContext(queryManager, taskTracker, workerManager,
                        null, null, null,
                        org.pangrova.coordinator.coordination.inmemory.InMemoryQueryStateJournal.INSTANCE,
                        metadataBootstrap.catalogRegistry(), writeCommitTracker,
                        stageCallback, null, null));
        // Coordinator HTTP server already running — QueryManager is referenced by resources
        // via the existing server instance, so we need to update the reference.
        // Since QueryResource holds a direct reference to QueryManager,
        // we rebuild the HTTP server with the new QueryManager.
        rebuildCoordinatorHttp(receiver);
    }

    private static void rebuildCoordinatorHttp(TaskStatusReceiver receiver) {
        int previousPort = coordinatorHttpServer.getPort();
        if (previousPort <= 0) {
            previousPort = 0;
        }
        try {
            coordinatorHttpServer.stop();
        } catch (Exception e) {
            LOG.debug("Error stopping old coordinator HTTP: {}", e.getMessage());
        }
        HealthCheckEndpoint healthEndpoint = createCoordinatorHealthEndpoint();
        try {
            coordinatorHttpServer = new CoordinatorHttpServer(
                    previousPort, receiver, queryManager, TRACING_HTTP_FILTER, null, healthEndpoint);
            startServerOrFail(coordinatorHttpServer);
        } catch (RuntimeException e) {
            // Re-binding the previous port can race with the old server's socket
            // teardown (BindException: Address already in use). Fall back to a
            // random port — startServerOrFail re-syncs the status reporter, so
            // the shared worker follows the new port.
            LOG.warn("event=coordinator_http_rebind_failed port={} error={} — retrying on random port",
                    previousPort, e.getMessage());
            coordinatorHttpServer = new CoordinatorHttpServer(
                    0, receiver, queryManager, TRACING_HTTP_FILTER, null, healthEndpoint);
            startServerOrFail(coordinatorHttpServer);
        }
        LOG.info("Rebuilt coordinator HTTP on port {}", coordinatorHttpServer.getPort());
    }

    // -----------------------------------------------------------------------
    // Infrastructure accessors (for property-based infrastructure verification)
    // -----------------------------------------------------------------------

    /** Returns the CoordinatorHttpServer port (for infrastructure property tests). */
    protected static int coordinatorHttpPort() {
        return coordinatorHttpServer != null ? coordinatorHttpServer.getPort() : -1;
    }

    /** Returns the WorkerHttpServer port (for infrastructure property tests). */
    protected static int workerHttpPort() {
        return workerHttpServer != null ? workerHttpServer.getPort() : -1;
    }

    // -----------------------------------------------------------------------
    // PostgreSQL metadata store accessors
    // -----------------------------------------------------------------------

    /** PostgreSQL JDBC URL for metadata store. */
    protected static String pgJdbcUrl() {
        return POSTGRES.getJdbcUrl();
    }

    protected static String pgUsername() {
        return POSTGRES.getUsername();
    }

    protected static String pgPassword() {
        return POSTGRES.getPassword();
    }

    /** @deprecated Use {@link #pgJdbcUrl()} instead. */
    @Deprecated
    protected static String mysqlJdbcUrl() {
        return pgJdbcUrl();
    }

    /** @deprecated Use {@link #pgUsername()} instead. */
    @Deprecated
    protected static String mysqlUsername() {
        return pgUsername();
    }

    /** @deprecated Use {@link #pgPassword()} instead. */
    @Deprecated
    protected static String mysqlPassword() {
        return pgPassword();
    }

    /** Iceberg warehouse path under the shared temp directory. */
    protected static Path icebergWarehouse() {
        return tempDir.resolve("iceberg-warehouse");
    }

    /** Paimon warehouse path under the shared temp directory. */
    protected static Path paimonWarehouse() {
        return tempDir.resolve("paimon-warehouse");
    }

    /**
     * Returns the shared DataSource backed by TestContainers PostgreSQL for state stores
     * (query state journal, worker registry, coordinator registry, tenant, etc.).
     * Available after {@link #bootstrapTestEnvironment()} completes.
     */
    protected static DataSource stateStoreDataSource() {
        return stateStoreDataSource;
    }

    // -----------------------------------------------------------------------
    // Real pipeline execution
    // -----------------------------------------------------------------------

    /**
     * Executes SQL through the real HTTP pipeline:
     * POST /v1/query/submit → poll state → fetch results via transport.
     */
    private static List<TypedRow> executeViaRealPipeline(String sql) {
        try {
            // Registry-based per-query buffer isolation (P0-FPV-5):
            // TaskResultPipelineWirer.registerQueryBuffer() creates a dedicated
            // OutputBufferManager per queryId in the registry. No global swap needed.

            SubmitQueryResponse submitResponse = submitQueryViaHttp(sql);
            String queryId = submitResponse.queryId();
            List<String> columnNames = submitResponse.columnNames();

            String terminalBody = pollUntilTerminal(queryId);
            var terminalJson = MAPPER.readTree(terminalBody);
            String terminalState = terminalJson.get("state").asText();
            if ("FAILED".equals(terminalState)) {
                String detail = extractErrorDetail(terminalJson);
                throw new RuntimeException("Query failed: " + queryId + " — " + detail);
            }

            List<TypedRow> rows = fetchResultsViaTransport(
                    submitResponse.resultEndpoint(), queryId);
            List<TypedRow> remapped = remapColumnNames(rows, columnNames);
            return remapped;
        } catch (TimeoutException e) {
            throw new RuntimeException("Query polling timed out after " + POLL_TIMEOUT_MS + "ms", e);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Pipeline execution failed: " + e.getMessage(), e);
        }
    }

    private static SubmitQueryResponse submitQueryViaHttp(String sql) throws Exception {
        String url = "http://localhost:" + coordinatorHttpServer.getPort() + "/v1/query/submit";
        SubmitQueryRequest request = new SubmitQueryRequest(sql, Map.of());
        byte[] body = MAPPER.writeValueAsBytes(request);

        HttpRequest httpReq = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .build();

        HttpResponse<String> resp = HTTP_CLIENT.send(httpReq, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 200) {
            throw new RuntimeException("Submit failed HTTP " + resp.statusCode() + ": " + resp.body());
        }
        String respBody = resp.body();
        if (respBody == null || respBody.isEmpty() || respBody.charAt(0) != '{') {
            System.err.println("[E2E-DEBUG] submitQueryViaHttp non-JSON response: status="
                    + resp.statusCode() + " contentType="
                    + resp.headers().firstValue("Content-Type").orElse("NONE")
                    + " bodyLength=" + (respBody == null ? "null" : respBody.length())
                    + " bodyPreview=" + (respBody == null ? "null"
                        : respBody.substring(0, Math.min(200, respBody.length()))));
        }
        return MAPPER.readValue(respBody, SubmitQueryResponse.class);
    }

    private static String pollUntilTerminal(String queryId) throws Exception {
        String url = "http://localhost:" + coordinatorHttpServer.getPort()
                + "/v1/query/" + queryId + "/state";

        // F-E2E-02 fix: replace hand-rolled while+Thread.sleep with Awaitility
        // deterministic polling. The lambda's side-effect (last response body)
        // is captured into a single-element holder so we can return it after
        // the condition resolves.
        String[] terminalBody = new String[1];
        try {
            org.awaitility.Awaitility.await("query-terminal: " + queryId)
                    .atMost(java.time.Duration.ofMillis(POLL_TIMEOUT_MS))
                    .pollInterval(java.time.Duration.ofMillis(POLL_INTERVAL_MS))
                    .pollDelay(java.time.Duration.ZERO)
                    .ignoreExceptions()
                    .until(() -> {
                        HttpRequest req = HttpRequest.newBuilder().uri(URI.create(url)).GET().build();
                        HttpResponse<String> resp = HTTP_CLIENT.send(req, HttpResponse.BodyHandlers.ofString());
                        String body = resp.body();
                        String state = MAPPER.readTree(body).get("state").asText();
                        if ("FINISHED".equals(state) || "FAILED".equals(state)) {
                            terminalBody[0] = body;
                            return true;
                        }
                        return false;
                    });
        } catch (org.awaitility.core.ConditionTimeoutException e) {
            throw new TimeoutException("Query " + queryId + " did not reach terminal state");
        }
        return terminalBody[0];
    }

    private static String extractErrorDetail(com.fasterxml.jackson.databind.JsonNode json) {
        var errorNode = json.get("error");
        if (errorNode != null && !errorNode.isNull()) {
            String code = errorNode.has("error_code") ? errorNode.get("error_code").asText() : "";
            String msg = errorNode.has("message") ? errorNode.get("message").asText() : "";
            return code.isEmpty() ? msg : "[" + code + "] " + msg;
        }
        return "no error detail";
    }


    /**
     * Remaps Arrow/internal column names to SQL-level aliases.
     * Native pipeline returns Substrait-generated names (e.g. w0$o0, proj_2);
     * this restores the SQL aliases from Calcite RelNode field names using
     * name-match-first, then positional fallback for synthetic columns.
     */
    private static List<TypedRow> remapColumnNames(
            List<TypedRow> rows, List<String> columnNames) {
        if (columnNames == null || columnNames.isEmpty() || rows.isEmpty()) {
            return rows;
        }
        return rows.stream().map(row -> remapRow(row, columnNames)).toList();
    }

    /**
     * Remaps a single row from Arrow/Substrait column names to SQL aliases.
     *
     * <p>Arrow IPC may return columns in a different order than the SQL projection
     * (e.g. computed columns like {@code proj_2} appear before source columns).
     * Strategy: match Arrow keys that exactly equal a SQL alias by name first,
     * then assign remaining synthetic keys to remaining unmatched aliases
     * in their relative order within the SQL projection.</p>
     */
    private static TypedRow remapRow(
            TypedRow row, List<String> columnNames) {
        List<String> arrowKeys = new java.util.ArrayList<>(row.columnNames());
        if (arrowKeys.size() != columnNames.size()) {
            // Fallback: positional mapping when column counts differ
            TypedRow.Builder b = TypedRow.builder();
            for (int i = 0; i < arrowKeys.size() && i < columnNames.size(); i++) {
                b.put(columnNames.get(i), row.get(arrowKeys.get(i)));
            }
            return b.build();
        }

        // Build mapping: sqlAlias → arrowKey
        String[] mapping = new String[columnNames.size()];
        boolean[] arrowUsed = new boolean[arrowKeys.size()];

        // Pass 1: exact name match (Arrow key == SQL alias, case-insensitive)
        for (int s = 0; s < columnNames.size(); s++) {
            for (int a = 0; a < arrowKeys.size(); a++) {
                if (!arrowUsed[a] && arrowKeys.get(a).equalsIgnoreCase(columnNames.get(s))) {
                    mapping[s] = arrowKeys.get(a);
                    arrowUsed[a] = true;
                    break;
                }
            }
        }

        // Pass 2: assign remaining unmatched Arrow keys to remaining SQL aliases
        // in their relative order (handles synthetic names like proj_2, w0$o0)
        java.util.List<Integer> unmatchedArrow = new java.util.ArrayList<>();
        for (int a = 0; a < arrowKeys.size(); a++) {
            if (!arrowUsed[a]) {
                unmatchedArrow.add(a);
            }
        }
        int uIdx = 0;
        for (int s = 0; s < mapping.length; s++) {
            if (mapping[s] == null && uIdx < unmatchedArrow.size()) {
                mapping[s] = arrowKeys.get(unmatchedArrow.get(uIdx++));
            }
        }

        TypedRow.Builder b = TypedRow.builder();
        for (int s = 0; s < mapping.length; s++) {
            b.put(columnNames.get(s), row.get(mapping[s]));
        }
        return b.build();
    }

    private static List<TypedRow> fetchResultsViaTransport(
            ResultEndpoint endpoint, String queryId) {
        if (endpoint == null) {
            return List.of();
        }
        // Direct buffer read bypasses the coordinator transport server entirely.
        if (coordinatorResultBuffer != null) {
            byte[] buffered = coordinatorResultBuffer.get(queryId);
            if (buffered != null && buffered.length > 0) {
                System.err.println("[E2E-DIAG] fetchResults: coordinatorResultBuffer HIT queryId=" + queryId
                        + " bytes=" + buffered.length);
                return decodeResultBytes(buffered);
            }
        }
        // Native result tasks serve data via C++ FlightResultServer on
        // sharedResultServerPort. The coordinator transport server only
        // holds constant-fold and DDL/DML results. For native SELECT
        // queries, connect directly to FlightResultServer via Arrow Flight.
        if (sharedResultServerPort > 0) {
            System.err.println("[E2E-DIAG] fetchResults: using Flight DoGet queryId=" + queryId
                    + " port=" + sharedResultServerPort);
            List<TypedRow> rows = fetchResultsViaFlight(queryId, taskTracker);
            System.err.println("[E2E-DIAG] fetchResults: Flight returned " + rows.size() + " rows queryId=" + queryId);
            return rows;
        }
        // Fallback to transport for non-native results
        System.err.println("[E2E-DIAG] fetchResults: using TransportClient queryId=" + queryId);
        DataTicket ticket = new DataTicket(
                queryId, "result", 0, DataTicket.TicketType.RESULT);
        try (TransportClient client = new TransportClient(endpoint.host(), endpoint.port())) {
            byte[] resultBytes = client.doGet(ticket);
            return decodeResultBytes(resultBytes);
        }
    }

    private static final int FLIGHT_FETCH_MAX_RETRIES = 3;
    private static final long FLIGHT_FETCH_RETRY_DELAY_MS = 3_000L;

    /**
     * Per-batch idle timeout for server-streaming DoGet (F-DOGET-02).
     *
     * <p>Unlike a call-level gRPC deadline (which kills the entire stream after N seconds
     * regardless of data flow), this is applied to each individual {@code stream.next()}
     * call. If the server produces batches continuously, the stream runs indefinitely;
     * if no batch arrives within this window, the task is considered stuck.</p>
     *
     * <p>This follows the industry-standard pattern used by Trino, Dremio, and
     * Spark Connect for server-streaming RPCs.</p>
     */
    private static final long FLIGHT_BATCH_IDLE_TIMEOUT_SECONDS = 60;

    /**
     * Daemon-thread executor for async {@code stream.next()} calls.
     * Each batch read is submitted to this executor so the main thread
     * can enforce an idle timeout via {@link java.util.concurrent.Future#get(long, java.util.concurrent.TimeUnit)}.
     * Daemon threads ensure JVM shutdown is never blocked by abandoned streams.
     */
    private static final java.util.concurrent.ExecutorService FLIGHT_BATCH_EXECUTOR =
            java.util.concurrent.Executors.newCachedThreadPool(r -> {
                Thread t = new Thread(r, "flight-idle-watchdog");
                t.setDaemon(true);
                return t;
            });

    /**
     * Fetches results directly from the C++ FlightResultServer via Arrow Flight DoGet.
     * The ticket is the native task ID registered in Velox OutputBufferManager.
     *
     * <h4>Timeout strategy (production-standard per-batch idle detection)</h4>
     * <p>Instead of setting a gRPC call-level deadline (which would kill long-running
     * but healthy streams after a fixed wall-clock duration), each individual
     * {@code stream.next()} call is bounded by {@link #FLIGHT_BATCH_IDLE_TIMEOUT_SECONDS}.
     * This means:</p>
     * <ul>
     *   <li>Large result sets that continuously produce batches — never timeout</li>
     *   <li>Stuck tasks that stop producing data — detected within 60s</li>
     * </ul>
     *
     * <h4>Retry strategy</h4>
     * <p>Retries on NOT_FOUND errors only. The C++ FlightResultServer already
     * retries for ~5s when a task is not yet registered. Java-side retries
     * handle the case where the native task is still being dispatched or where
     * the folly executor hasn't started the task yet. Timeout errors are NOT
     * retried — with the async execution model, timeouts indicate genuine
     * server/network failure rather than thread-pool starvation.</p>
     *
     * <h4>Cleanup (F-DOGET-03)</h4>
     * <p>On final failure, cancels the native task via WorkerTaskManager and
     * always releases the task executor to prevent resource leaks
     * (stuck result tasks hold folly threads and native memory pools
     * indefinitely because PartitionedOutputNode waits for DoGet).</p>
     */
    protected static List<TypedRow> fetchResultsViaFlight(
            String queryId, CoordinatorTaskTracker tracker) {
        String nativeTaskId = resolveNativeTaskId(queryId, tracker);
        if (nativeTaskId == null) {
            LOG.warn("event=flight_fetch_no_task_id queryId={}", queryId);
            return List.of();
        }
        var location = org.apache.arrow.flight.Location.forGrpcInsecure(
                "localhost", sharedResultServerPort);

        Exception lastException = null;
        for (int attempt = 0; attempt <= FLIGHT_FETCH_MAX_RETRIES; attempt++) {
            if (attempt > 0) {
                LOG.info("event=flight_fetch_retry queryId={} taskId={} attempt={}/{}",
                        queryId, nativeTaskId, attempt, FLIGHT_FETCH_MAX_RETRIES);
                try {
                    // Retry-with-backoff between Flight DoGet attempts. Cannot
                    // be expressed as Awaitility polling because the operation
                    // itself is the request, not an observable condition.
                    // CHECKSTYLE.OFF: RegexpSinglelineJava - intentional retry backoff
                    Thread.sleep(FLIGHT_FETCH_RETRY_DELAY_MS);
                    // CHECKSTYLE.ON: RegexpSinglelineJava
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            try (var allocator = new org.apache.arrow.memory.RootAllocator(64L * 1024 * 1024);
                 var client = org.apache.arrow.flight.FlightClient.builder(allocator, location).build();
                 var stream = client.getStream(
                         new org.apache.arrow.flight.Ticket(nativeTaskId.getBytes(
                                 java.nio.charset.StandardCharsets.UTF_8)))) {
                List<TypedRow> rows = new java.util.ArrayList<>();
                while (awaitNextBatch(stream)) {
                    appendBatchRows(stream.getRoot(), rows);
                }
                LOG.info("event=flight_fetch_complete queryId={} taskId={} rows={} attempt={}",
                        queryId, nativeTaskId, rows.size(), attempt);
                cancelNativeTaskAfterFetch(nativeTaskId);
                return rows;
            } catch (Exception e) {
                lastException = e;
                String msg = e.getMessage() != null ? e.getMessage() : "";
                boolean retryable = msg.contains("NOT_FOUND");
                LOG.warn("event=flight_fetch_failed queryId={} taskId={} attempt={}/{} retryable={} error={}",
                        queryId, nativeTaskId, attempt, FLIGHT_FETCH_MAX_RETRIES, retryable, msg);
                if (!retryable) {
                    break;
                }
            }
        }

        LOG.error("event=flight_fetch_exhausted queryId={} taskId={} error={}",
                queryId, nativeTaskId,
                lastException != null ? lastException.getMessage() : "unknown", lastException);
        cancelNativeTaskAfterFetch(nativeTaskId);
        return List.of();
    }

    private static void cancelNativeTaskAfterFetch(String coordinatorTaskId) {
        if (workerTaskManagerRef == null || coordinatorTaskId == null) {
            return;
        }
        try {
            // TaskManager uses worker task IDs (queryId-NNN), so resolve first
            String workerTaskId = workerTaskManagerRef.resolveWorkerTaskId(coordinatorTaskId);
            String taskIdToCancel = workerTaskId != null ? workerTaskId : coordinatorTaskId;
            var result = workerTaskManagerRef.cancelTask(taskIdToCancel);
            // Always release the task executor after cancel to reclaim folly threads.
            // Previously only released on cancel failure, but successful cancel also
            // needs join() to ensure the folly thread is actually freed (F-DOGET-03).
            workerTaskManagerRef.releaseTaskExecutor(taskIdToCancel);
            LOG.info("event=flight_fetch_cancel_task coordinatorId={} workerTaskId={} result={}",
                    coordinatorTaskId, taskIdToCancel, result);
        } catch (Exception e) {
            LOG.warn("event=flight_fetch_cancel_failed taskId={} error={}",
                    coordinatorTaskId, e.getMessage());
        }
    }

    /**
     * Awaits the next batch from a Flight stream with per-batch idle timeout.
     *
     * <p>This implements the production-standard pattern for server-streaming RPCs:
     * instead of setting a gRPC call-level deadline (which kills healthy long-running
     * streams), each individual {@code stream.next()} is bounded by
     * {@link #FLIGHT_BATCH_IDLE_TIMEOUT_SECONDS}. If the server continuously produces
     * batches, the overall stream runs indefinitely; if it stops producing data
     * (task stuck, folly thread leak, deadlock), we detect it within the idle window.</p>
     *
     * <p>The call is submitted to {@link #FLIGHT_BATCH_EXECUTOR} so the main thread
     * can enforce the timeout via {@link java.util.concurrent.Future#get(long, java.util.concurrent.TimeUnit)}.
     * On timeout, {@code future.cancel(true)} interrupts the blocked gRPC read,
     * and the stream's try-with-resources closure cancels the underlying RPC.</p>
     *
     * @param stream the Arrow Flight stream to read from
     * @return true if a batch is available, false if the stream is exhausted
     * @throws RuntimeException wrapping TimeoutException if idle timeout is exceeded
     */
    private static boolean awaitNextBatch(org.apache.arrow.flight.FlightStream stream) {
        var future = FLIGHT_BATCH_EXECUTOR.submit(stream::next);
        try {
            return future.get(FLIGHT_BATCH_IDLE_TIMEOUT_SECONDS,
                    java.util.concurrent.TimeUnit.SECONDS);
        } catch (java.util.concurrent.TimeoutException e) {
            future.cancel(true); // interrupt the blocked gRPC read
            throw new RuntimeException(
                    "Flight DoGet idle timeout: no batch received within "
                    + FLIGHT_BATCH_IDLE_TIMEOUT_SECONDS
                    + "s — native task is likely stuck (F-DOGET-02)", e);
        } catch (java.util.concurrent.ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException re) {
                throw re;
            }
            throw new RuntimeException(cause);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted waiting for Flight batch", e);
        }
    }

    /**
     * Resolves the native task ID for a query from the task tracker.
     * The coordinator tracks its own dispatch UUID, but the C++
     * OutputBufferManager registers under the coordinator dispatch UUID
     * (the taskId from the SubmitTaskRequest proto, which the C++ native
     * executor receives and passes to Velox Task::create). This is NOT
     * the worker-generated queryId-NNN format — the worker ID is only
     * used for Java-side task management.
     */
    protected static String resolveNativeTaskId(
            String queryId, CoordinatorTaskTracker tracker) {
        if (tracker != null) {
            String coordinatorTaskId = tracker.getLatestStartedTaskId(queryId);
            if (coordinatorTaskId == null) {
                var taskIds = tracker.getTaskIds(queryId);
                if (taskIds != null && !taskIds.isEmpty()) {
                    coordinatorTaskId = taskIds.iterator().next();
                }
            }
            return coordinatorTaskId;
        }
        return null;
    }

    /**
     * Decodes Arrow IPC stream bytes into a list of row maps.
     * Uses Arrow SDK ArrowStreamReader to materialize VectorSchemaRoot batches.
     *
     * <p>Note: Worker results from C++ FlightResultServer use Velox PrestoVectorSerde
     * binary blob format (schema: {data: binary}). These require native-side
     * deserialization which is not yet implemented. Such results decode as empty.</p>
     */
    protected static List<TypedRow> decodeResultBytes(byte[] resultBytes) {
        if (resultBytes == null || resultBytes.length == 0) {
            return List.of();
        }
        List<TypedRow> rows = new java.util.ArrayList<>();
        try (var allocator = new org.apache.arrow.memory.RootAllocator();
             var reader = new org.apache.arrow.vector.ipc.ArrowStreamReader(
                     new java.io.ByteArrayInputStream(resultBytes), allocator)) {
            while (reader.loadNextBatch()) {
                appendBatchRows(reader.getVectorSchemaRoot(), rows);
            }
        } catch (Exception e) {
            LOG.warn("Failed to decode Arrow IPC bytes ({} bytes): {}",
                    resultBytes.length, e.getMessage());
        }
        return rows;
    }

    protected static void appendBatchRows(
            org.apache.arrow.vector.VectorSchemaRoot root,
            List<TypedRow> rows) {
        int rowCount = root.getRowCount();
        int colCount = root.getFieldVectors().size();
        if (rows.isEmpty()) {
            System.err.println("[E2E-DIAG] Arrow schema: " + root.getSchema());
            for (var v : root.getFieldVectors()) {
                System.err.println("[E2E-DIAG]   field=" + v.getName()
                        + " type=" + v.getField().getType()
                        + " class=" + v.getClass().getSimpleName());
            }
            if (rowCount > 0) {
                System.err.println("[E2E-DIAG] First row values:");
                for (int c = 0; c < colCount; c++) {
                    var vector = root.getFieldVectors().get(c);
                    Object val = vector.isNull(0) ? "NULL" : vector.getObject(0);
                    System.err.println("[E2E-DIAG]   col[" + c + "]=" + vector.getName()
                            + " value=" + val + " (" + (val == null ? "null" : val.getClass().getSimpleName()) + ")");
                }
            }
        }
        for (int r = 0; r < rowCount; r++) {
            TypedRow.Builder b = TypedRow.builder();
            for (int c = 0; c < colCount; c++) {
                var vector = root.getFieldVectors().get(c);
                String name = vector.getName();
                // Velox PrestoVectorSerde deserialized with ROW({}) produces empty field names.
                // Use positional placeholder so remapColumnNames Pass 2 can map by position.
                if (name == null || name.isEmpty()) {
                    name = "_col" + c;
                }
                Object value = normalizeArrowValue(vector, r);
                b.putRaw(name, value);
            }
            rows.add(b.build());
        }
    }

    /**
     * Normalizes Arrow vector values to standard Java types.
     * Arrow returns Text for VARCHAR, epoch-days Integer for DATE, etc.
     */
    private static Object normalizeArrowValue(
            org.apache.arrow.vector.FieldVector vector, int row) {
        if (vector.isNull(row)) {
            return null;
        }
        Object value = vector.getObject(row);
        // Arrow Text → Java String.
        // Velox pads fixed-length CHAR literals with trailing spaces
        // (e.g. CASE WHEN string constants). Strip at the transport boundary
        // so individual tests don't need per-assertion trimming.
        if (value instanceof org.apache.arrow.vector.util.Text text) {
            return stripTrailing(text.toString());
        }
        // Arrow DateDayVector returns epoch days as Integer → LocalDate string
        if (vector instanceof org.apache.arrow.vector.DateDayVector) {
            return java.time.LocalDate.ofEpochDay(((Number) value).longValue()).toString();
        }
        // Arrow TimestampVector returns epoch millis/micros → Instant string
        if (vector instanceof org.apache.arrow.vector.TimeStampVector) {
            return value.toString();
        }
        return value;
    }

    /** Strips trailing whitespace from a string. Handles Velox CHAR padding. */
    private static String stripTrailing(String s) {
        int end = s.length();
        while (end > 0 && s.charAt(end - 1) <= ' ') {
            end--;
        }
        return end == s.length() ? s : s.substring(0, end);
    }

    // -----------------------------------------------------------------------
    // TestCluster wiring
    // -----------------------------------------------------------------------

    /**
     * Creates a TestCluster wired with real HTTP communication.
     * Coordinator runs CoordinatorHttpServer (random port) for status reports.
     * Worker runs WorkerHttpServer (random port) for task dispatch.
     */
    protected static TestCluster createTestCluster() {
        return new TestCluster(
                SqlE2ETestBase::startCoordinator,
                SqlE2ETestBase::startWorker
        );
    }

    /**
     * Coordinator bootstrap via SystemBootstrapSequence:
     * INFRASTRUCTURE: Config → Observability
     * COORDINATORS: Metadata → Security → QueryManager → HTTP
     * READINESS: HA registration
     */
    private static Closeable startCoordinator() {

        SystemBootstrapSequence bootstrap = new SystemBootstrapSequence(List.of(
                phase -> executeCoordinatorPhase(phase)
        ));
        bootstrap.run();

        return () -> {
            stopQuietly(coordinatorHttpServer);
            shutdownMetadata();
        };
    }

    private static void executeCoordinatorPhase(BootstrapPhase phase) {
        switch (phase) {
            case INFRASTRUCTURE -> initCoordinatorInfrastructure();
            case COORDINATORS -> initCoordinatorServices();
            case WORKERS -> { /* no-op for coordinator */ }
            case READINESS -> initCoordinatorReadiness();
        }
    }

    private static void initCoordinatorInfrastructure() {
        // Observability: NoOp providers (no real OTel endpoint in tests)
        LOG.info("Coordinator INFRASTRUCTURE: observability initialized");
    }

    private static void initCoordinatorServices() {
        System.err.println("[E2E-DEBUG] initCoordinatorServices: start");
        taskTracker = new CoordinatorTaskTracker();
        System.err.println("[E2E-DEBUG] initCoordinatorServices: taskTracker created");
        metadataBootstrap = initializeMetadata();
        System.err.println("[E2E-DEBUG] initCoordinatorServices: metadata initialized");
        writeCommitTracker = new org.pangrova.coordinator.query.task.NativeWriteCommitTracker();
        registerNativeWriter();
        QueryDispatcher dispatcher = buildQueryDispatcher();
        System.err.println("[E2E-DEBUG] initCoordinatorServices: dispatcher built");
        queryManager = new QueryManager(QueryManagerContext.minimal(dispatcher, TRACING_PROVIDER));
        System.err.println("[E2E-DEBUG] initCoordinatorServices: queryManager created");
        new CoordinatorMetrics(METRICS_PROVIDER, () -> (double) workerManager.getActiveWorkers().size());
        System.err.println("[E2E-DEBUG] initCoordinatorServices: metrics created");

        TaskStatusReceiver receiver = new TaskStatusReceiver(
                new TaskStatusReceiverContext(queryManager, taskTracker, workerManager,
                        null, null, null,
                        org.pangrova.coordinator.coordination.inmemory.InMemoryQueryStateJournal.INSTANCE,
                        metadataBootstrap.catalogRegistry(), writeCommitTracker, null, null, null));
        System.err.println("[E2E-DEBUG] initCoordinatorServices: receiver created");
        HealthCheckEndpoint healthEndpoint = createCoordinatorHealthEndpoint();
        coordinatorHttpServer = new CoordinatorHttpServer(
                0, receiver, queryManager, TRACING_HTTP_FILTER, null, healthEndpoint);
        System.err.println("[E2E-DEBUG] initCoordinatorServices: http server created");
        startServerOrFail(coordinatorHttpServer);
        coordinatorHttpReady.set(true);
        LOG.info("Coordinator COORDINATORS: metadata + queryManager + HTTP started on port {}",
                coordinatorHttpServer.getPort());
    }

    private static void initCoordinatorReadiness() {
        // HA registration — in tests, coordinator is always leader
        LOG.info("Coordinator READINESS: HA registration complete");
    }

    /**
     * Worker bootstrap via SystemBootstrapSequence:
     * INFRASTRUCTURE: Config → Observability
     * WORKERS: NativeRuntime.nativeInit() + NativeTaskExecutor → ResourceManager → TaskManager → Transport → HTTP
     * READINESS: Cluster registration
     */
    protected static Closeable startWorker() {
        // Mutable holders for cross-phase references
        final TaskExecutor[] executorHolder = new TaskExecutor[1];
        final TaskManager[] taskManagerHolder = new TaskManager[1];

        SystemBootstrapSequence bootstrap = new SystemBootstrapSequence(List.of(
                phase -> executeWorkerPhase(phase, executorHolder, taskManagerHolder)
        ));
        bootstrap.run();

        return () -> {
            stopQuietly(workerHttpServer);
            stopCoordinatorTransportQuietly();
        };
    }

    private static void executeWorkerPhase(BootstrapPhase phase,
                                            TaskExecutor[] executorHolder,
                                            TaskManager[] taskManagerHolder) {
        switch (phase) {
            case INFRASTRUCTURE -> initWorkerInfrastructure();
            case COORDINATORS -> { /* no-op for worker */ }
            case WORKERS -> initWorkerServices(executorHolder, taskManagerHolder);
            case READINESS -> initWorkerReadiness();
        }
    }

    private static void initWorkerInfrastructure() {
        LOG.info("Worker INFRASTRUCTURE: config + observability initialized");
    }

    private static void initWorkerServices(TaskExecutor[] executorHolder,
                                            TaskManager[] taskManagerHolder) {
        // Phase 1: native engine (mandatory)
        TaskExecutor taskExecutor = initNativeExecutor();
        executorHolder[0] = taskExecutor;
        workerNativeReady.set(true);

        // Phase 2: resource management + task manager (no legacy buffer managers)
        ResourceManager resourceManager = createResourceManager();
        TaskManager workerTaskManager = createTaskManager(taskExecutor, resourceManager);
        taskManagerHolder[0] = workerTaskManager;
        workerTaskManagerRef = workerTaskManager;
        // If the native write executor was already registered during coordinator
        // bootstrap (registerNativeWriter() in initCoordinatorServices), wire it
        // into the worker TaskManager now so write task FINISHED reports include
        // the per-task commit metadata required for distributed write commits.
        if (sharedNativeWriteExecutor != null) {
            workerTaskManager.setWriteExecutor(sharedNativeWriteExecutor);
        }
        new WorkerMetrics(METRICS_PROVIDER, () -> 0.0, () -> 0.0);

        // Phase 3: Coordinator-side transport + HTTP servers with health endpoint
        coordinatorTransportServer = startCoordinatorTransportServer();
        workerTransportReady.set(true);

        HealthCheckEndpoint healthEndpoint = createWorkerHealthEndpoint();
        int workerTransportPort = sharedResultServerPort > 0
                ? sharedResultServerPort
                : coordinatorTransportServer.getPort();
        workerHttpServer = startWorkerHttpServer(workerTaskManager, taskExecutor,
                workerTransportPort, healthEndpoint);
        workerHttpReady.set(true);

        LOG.info("Worker WORKERS: native + resources + taskManager + transport:{} + http:{} started",
                workerTransportPort, workerHttpServer.getPort());
    }

    private static void initWorkerReadiness() {
        // Phase 4: register with real transport port (native FlightResultServer for SELECT queries)
        int workerTransportPort = sharedResultServerPort > 0
                ? sharedResultServerPort
                : coordinatorTransportServer.getPort();
        registerWorker(workerHttpServer.getPort(), workerTransportPort);
        configureStatusReporter();
        LOG.info("Worker READINESS: cluster registration complete");
    }

    private static TaskExecutor initNativeExecutor() {
        // nativeInit already called in bootstrapTestEnvironment — skip duplicate init
        return new NativeTaskExecutor();
    }

    private static TaskManager createTaskManager(
            TaskExecutor executor, ResourceManager resourceManager) {
        int coordPort = coordinatorHttpServer != null ? coordinatorHttpServer.getPort() : 0;
        statusReporter = new RemoteTaskStatusReporter("http://localhost:" + coordPort, TRACING_PROVIDER);
        sharedStatusReporter = statusReporter;
        TransportRefs transportDeps = createTestTransportRefs(executor);
        TaskManagerContext deps = new TaskManagerContext(
                statusReporter, executor,
                new MemoryRefs(resourceManager),
                transportDeps,
                java.util.concurrent.Executors.newCachedThreadPool(r -> {
                    Thread t = new Thread(r, "test-completion-monitor");
                    t.setDaemon(true);
                    return t;
                }));
        return new TaskManager(deps, WORKER_ID, 0.0);
    }

    private static TransportRefs createTestTransportRefs(TaskExecutor executor) {
        try {
            var shuffleTransport = new org.pangrova.worker.shuffle.NativeShuffleTransport(executor, "localhost");
            var resultTransport = new org.pangrova.worker.result.NativeResultTransport(executor, "localhost");
            resultTransport.startServer(0);
            sharedResultServerPort = resultTransport.boundPort();
            var exchangeTransport = new org.pangrova.worker.exchange.NativeExchangeTransport(executor);
            LOG.info("event=e2e_transport_initialized resultPort={}", sharedResultServerPort);
            return new TransportRefs(shuffleTransport, resultTransport, exchangeTransport);
        } catch (Exception e) {
            LOG.warn("event=e2e_transport_init_failed error={} fallback=empty", e.getMessage());
            return TransportRefs.empty();
        }
    }

    private static ResourceManager createResourceManager() {
        MemoryArbitrator arbitrator = new MemoryArbitrator(WORKER_MEMORY_BYTES, 128L * 1024 * 1024);
        SpillManager spillManager = new SpillManager();
        ThreadBudgetTracker threadPoolManager = new ThreadBudgetTracker(8, 4, 2);
        return new ResourceManager(arbitrator, threadPoolManager, spillManager);
    }

    /**
     * Starts a coordinator-side transport server for serving constant-fold and DDL/DML results.
     * Worker-side result serving is handled by the C++ FlightResultServer via NativeResultTransport.
     */
    private static TransportServer startCoordinatorTransportServer() {
        // Track consumed queries to ensure single-delivery semantics.
        // coordinatorResultBuffer.get() is non-destructive (returns same data on every call),
        // but the DataProducer SPI contract requires: data bytes → byte[0] (EOF).
        // Without this, streamRemainingBatches loops forever re-delivering the same batch.
        var consumed = java.util.concurrent.ConcurrentHashMap.<String>newKeySet();
        org.pangrova.spi.transport.DataProducer coordinatorProducer = ticket -> {
            if (coordinatorResultBuffer != null) {
                if (consumed.add(ticket.queryId())) {
                    byte[] buffered = coordinatorResultBuffer.get(ticket.queryId());
                    if (buffered != null) {
                        return buffered;
                    }
                }
            }
            return new byte[0];
        };
        TransportServer server = new TransportServer(
                coordinatorProducer, new org.pangrova.spi.transport.TransportServerConfig(0));
        server.start();
        return server;
    }

    private static WorkerHttpServer startWorkerHttpServer(
            TaskManager taskManager, TaskExecutor taskExecutor,
            int transportPort, HealthCheckEndpoint healthEndpoint) {
        var exchangeTransport = new NativeExchangeTransport(taskExecutor);
        var httpConfig = new WorkerHttpServer.WorkerHttpConfig(0, "localhost", transportPort);
        WorkerHttpServer server = new WorkerHttpServer(
                httpConfig, taskManager, exchangeTransport, healthEndpoint);
        startServerOrFail(server);
        return server;
    }

    // -----------------------------------------------------------------------
    // Wiring helpers (each ≤ 20 lines, single responsibility)
    // -----------------------------------------------------------------------

    private static QueryDispatcher buildQueryDispatcher() {
        // StatementDispatcher must be built BEFORE SqlCompiler so that sharedMvManager
        // is initialized when buildSqlCompiler() creates the DynamicTableRewriter.
        // Late-bound NativeDmlCompileFunction: INSERT SELECT, DELETE, UPDATE, MERGE
        // re-enter the QueryDispatcher pipeline for native execution.
        // Uses executeDmlSql() to bypass StatementDispatchPhase and avoid
        // re-entering the statement dispatch pipeline.
        var queryDispatcherRef = new java.util.concurrent.atomic.AtomicReference<QueryDispatcher>();
        org.pangrova.coordinator.query.execution.dispatch.NativeDmlCompileFunction dmlDispatch =
                (sm, session, stmt) -> {
                    QueryDispatcher qd = queryDispatcherRef.get();
                    if (qd == null) {
                        throw new IllegalStateException("QueryDispatcher not initialized");
                    }
                    // Delegate to executeDmlStatement for MERGE (handles Complete MERGE
                    // decomposition with write context registration).
                    if (stmt instanceof org.pangrova.parser.ast.statement.dml.MergeStatement) {
                        return qd.executeDmlStatement(sm, stmt, session);
                    }
                    // DELETE: handle in Java via Iceberg API.
                    // DELETE ALL and partition-level deletes use deleteByFilter (MoR).
                    // Row-level deletes (WHERE on non-partition columns) use native
                    // copy-on-write path: read surviving rows → overwrite.
                    if (stmt instanceof org.pangrova.parser.ast.statement.dml.DeleteStatement delStmt) {
                        if (delStmt.where().isEmpty()) {
                            // DELETE ALL: pure metadata operation
                            var ref = org.pangrova.coordinator.query.statement.TableResolver
                                    .resolveTableRef(delStmt.tableName().parts());
                            var connSession = org.pangrova.coordinator.query.statement.TableResolver
                                    .buildSession(ref.catalog());
                            var catHandle = metadataBootstrap.catalogRegistry().getCatalog(ref.catalog());
                            var meta = catHandle.getMetadata(connSession);
                            var tblHandle = org.pangrova.coordinator.query.statement.TableResolver
                                    .requireTableHandle(meta, connSession, ref);
                            meta.deleteByFilter(connSession, tblHandle,
                                    new org.pangrova.spi.connector.metadata.DeleteFilter.All());
                            LOG.info("event=delete_all table={}", ref);
                            sm.tryTransition(org.pangrova.coordinator.query.QueryStateMachine.State.SCHEDULING);
                            sm.tryTransition(org.pangrova.coordinator.query.QueryStateMachine.State.RUNNING);
                            sm.tryTransition(org.pangrova.coordinator.query.QueryStateMachine.State.FINISHING);
                            sm.tryTransition(org.pangrova.coordinator.query.QueryStateMachine.State.FINISHED);
                            return new org.pangrova.coordinator.query.execution.dispatch.DispatchResult("localhost", "delete-" + session.queryId(),
                                    new org.pangrova.protocol.rest.query.ResultEndpoint(
                                            "localhost", 0, "delete-" + session.queryId(), "local"));
                        }
                        // Row-level DELETE with WHERE: native copy-on-write path.
                        // DmlOperationConverter.convertDelete() inverts the WHERE filter to
                        // produce surviving rows (CoW). The write mode must be OVERWRITE so
                        // the commit replaces existing data with the surviving-rows files.
                        String sql = new org.pangrova.parser.AstPrinter().print(stmt);
                        return qd.executeDmlSql(sm, sql, session, "OVERWRITE");
                    }
                    // INSERT OVERWRITE SELECT: override write mode to OVERWRITE so
                    // native workers produce overwrite-format fragments and
                    // IcebergMetadata.commitAtomicOverwrite() replaces data files.
                    if (stmt instanceof org.pangrova.parser.ast.statement.dml.InsertSelectStatement iss
                            && iss.overwrite()) {
                        String sql = new org.pangrova.parser.AstPrinter().print(stmt);
                        return qd.executeDmlSql(sm, sql, session, "OVERWRITE");
                    }
                    // UPDATE and other DML: compile to native plan and dispatch.
                    String sql = new org.pangrova.parser.AstPrinter().print(stmt);
                    return qd.executeDmlSql(sm, sql, session, null);
                };
        // Build CoordinatorResultService BEFORE StatementDispatcher so DDL/metadata
        // results (SHOW INDEXES, DESCRIBE, etc.) are buffered and returned to clients.
        CoordinatorResultService resultSvc = buildCoordinatorResultService();
        var statementDispatcher = buildStatementDispatcher(dmlDispatch, queryDispatcherRef, taskTracker, resultSvc);
        var indexAwareSplitFetcher = registerIndexHandlers(statementDispatcher);
        statementDispatcher.setSystemTableQueryTask(
                new org.pangrova.coordinator.query.execution.dispatch.task.SystemTableQueryTask(
                        metadataBootstrap.catalogRegistry()));
        SqlCompiler compiler = buildSqlCompiler();
        statementDispatcher.updateCompiler(compiler);
        closeQuietly(sharedTaskDispatcher, "task_dispatcher");
        RemoteTaskDispatcher httpDispatcher = new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), TRACING_PROVIDER);
        sharedTaskDispatcher = httpDispatcher;
        TaskScheduler scheduler = new TaskScheduler(new WorkerPerformanceScorer());
        if (workerManager == null) {
            workerManager = new WorkerManager();
        }
        var security = new org.pangrova.coordinator.query.execution.SecurityContextBundle(null, null, null);
        var scheduling = new org.pangrova.coordinator.query.execution.SchedulingContextBundle(
                httpDispatcher, scheduler, workerManager, taskTracker, null, null, null, null);
        var deps = new QueryDispatcherContext(
                compiler, security, scheduling,
                org.pangrova.coordinator.query.execution.CacheContextBundle.none(),
                metadataBootstrap.catalogRegistry(), null, resultSvc, 0L,
                null, null, null, null, null, null, null, indexAwareSplitFetcher);
        var queryDispatcher = new QueryDispatcher(
                deps, statementDispatcher);
        queryDispatcher.setWriteCommitTracker(writeCommitTracker);
        queryDispatcherRef.set(queryDispatcher);
        return queryDispatcher;
    }

    /**
     * Prepares index DDL infrastructure and query-time index pruning for E2E tests.
     * Creates a minimal IndexDdlExecutor backed by the same PostgreSQL DataSource
     * used for metadata. The index cache is stored for use by other E2E tests.
     * Returns an IndexAwareSplitFetcher for query-time split pruning.
     */
    private static org.pangrova.coordinator.index.IndexAwareSplitFetcher registerIndexHandlers(
            org.pangrova.coordinator.query.execution.dispatch.StatementDispatcher statementDispatcher) {
        try {
            javax.sql.DataSource ds = org.pangrova.testing.e2e.setup.MetadataSchemaInitializer
                    .createDataSource(pgJdbcUrl(), pgUsername(), pgPassword());
            var indexMetadataStore = new org.pangrova.metadata.index.jdbc.JdbcIndexMetadataStore(ds);
            var indexCache = new org.pangrova.indexing.storage.IndexCache(64L * 1024 * 1024);
            sharedIndexCache = indexCache;
            var engineRegistry = new org.pangrova.indexing.IndexEngineRegistry();
            var storageManager = new org.pangrova.indexing.storage.IndexStorageManager();
            var serdeRegistry = org.pangrova.indexing.serde.TypeSerializerRegistry.createDefault();
            var serializer = new org.pangrova.indexing.serde.IndexSerializer(serdeRegistry);
            var config = new org.pangrova.indexing.config.IndexingConfig(
                    "/tmp/pangrova/indexes-e2e", true, null, null);
            var buildDeps = new org.pangrova.indexing.build.IndexBuildContext(
                    config, engineRegistry, storageManager, serializer);
            var buildCoordinator = new org.pangrova.indexing.build.IndexBuildCoordinator(buildDeps, null);
            var accessorResolver = new org.pangrova.coordinator.index.ddl.ConnectorAccessorResolver(
                    metadataBootstrap.catalogRegistry());
            var ddlDeps = new org.pangrova.coordinator.index.ddl.IndexDdlContext(
                    indexMetadataStore, buildCoordinator, metadataBootstrap.catalogRegistry(),
                    accessorResolver);
            var indexExecutor = new org.pangrova.coordinator.index.ddl.IndexDdlExecutor(
                    ddlDeps, indexCache, null);

            var metadataFactory = new org.pangrova.coordinator.query.execution.dispatch.MetadataExecutionFactory(
                    java.util.Map.of());
            metadataFactory.registerTask(org.pangrova.parser.ast.statement.CreateIndexStatement.class,
                    new org.pangrova.coordinator.query.execution.dispatch.task.CreateIndexTask(indexExecutor));
            metadataFactory.registerTask(org.pangrova.parser.ast.statement.DropIndexStatement.class,
                    new org.pangrova.coordinator.query.execution.dispatch.task.DropIndexTask(indexExecutor));
            metadataFactory.registerTask(org.pangrova.parser.ast.statement.ShowIndexesStatement.class,
                    new org.pangrova.coordinator.query.execution.dispatch.task.ShowIndexesTask(indexExecutor));
            metadataFactory.registerTask(org.pangrova.parser.ast.statement.AlterIndexStatement.class,
                    new org.pangrova.coordinator.query.execution.dispatch.task.AlterIndexTask(indexExecutor));
            statementDispatcher.registerFactory(metadataFactory);

            var schemaEvolutionHandler = new org.pangrova.metadata.index.IndexSchemaEvolutionHandler(indexMetadataStore);
            if (sharedDdlExecutor != null) {
                sharedDdlExecutor.setSchemaEvolutionHandler(schemaEvolutionHandler);
            }

            var evalConfig = new org.pangrova.coordinator.index.filter.IndexEvalConfig(null, 5000, 0);
            var splitFilter = new org.pangrova.coordinator.index.filter.IndexSplitFilter(
                    indexMetadataStore, null, evalConfig);
            var indexAwareSplitFetcher = new org.pangrova.coordinator.index.IndexAwareSplitFetcher(
                    metadataBootstrap.catalogRegistry(), splitFilter, indexMetadataStore, accessorResolver);

            LOG.info("event=index_infrastructure_initialized_in_e2e executor_type={} pruning=enabled",
                    indexExecutor.getClass().getSimpleName());
            return indexAwareSplitFetcher;
        } catch (Exception e) {
            LOG.warn("event=index_handler_registration_failed error={}", e.getMessage());
            return null;
        }
    }

    private static CoordinatorResultService buildCoordinatorResultService() {
        if (coordinatorTransportServer == null || !coordinatorTransportServer.isRunning()) {
            return null;
        }
        if (resultBufferExecutor == null) {
            resultBufferExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "result-buffer-expiry");
                t.setDaemon(true);
                return t;
            });
        }
        closeQuietly(coordinatorArrowAllocator, "arrow_allocator");
        coordinatorResultBuffer = new CoordinatorResultBuffer(resultBufferExecutor, 300_000L);
        coordinatorArrowAllocator = new org.apache.arrow.memory.RootAllocator(128L * 1024 * 1024);
        var writer = new CoordinatorResultWriter(coordinatorArrowAllocator);
        var endpoint = new ResultEndpoint("localhost", coordinatorTransportServer.getPort(), "coordinator", "grpc");
        coordinatorResultService = new CoordinatorResultService(coordinatorResultBuffer, writer, endpoint);
        return coordinatorResultService;
    }

    protected static org.pangrova.coordinator.query.execution.dispatch.StatementDispatcher buildStatementDispatcher(
            org.pangrova.coordinator.query.execution.dispatch.NativeDmlCompileFunction dmlDispatch,
            java.util.concurrent.atomic.AtomicReference<QueryDispatcher> queryDispatcherRef,
            org.pangrova.coordinator.task.CoordinatorTaskTracker tt,
            org.pangrova.coordinator.result.CoordinatorResultService resultSvc) {
        var catalog = metadataBootstrap.catalogRegistry();
        var ddlExecutor = new org.pangrova.coordinator.query.statement.DdlExecutor(catalog, null,
                org.pangrova.coordinator.metadata.MetadataSchemaBuilder::invalidateTableCaches);
        sharedDdlExecutor = ddlExecutor;
        var dmlExecutor = new org.pangrova.coordinator.query.statement.dml.DmlExecutor(catalog,
                new org.pangrova.coordinator.query.statement.dml.DmlExecutorConfig(
                        null, dmlDispatch, null,
                        org.pangrova.coordinator.query.statement.dml.DmlExecutorConfig.DEFAULT_MAX_CONCURRENT_DML,
                        120_000L,
                        writeCommitTracker));
        var showDescribeExecutor = new org.pangrova.coordinator.query.statement.ShowDescribeExecutor(catalog);
        var maintenanceExecutor = new org.pangrova.coordinator.query.statement.MaintenanceExecutor(null);
        var dtExecutor = buildDynamicTableDdlExecutor();

        // INSERT SELECT: dispatch to native worker via late-bound QueryDispatcher
        org.pangrova.coordinator.query.execution.dispatch.dag.insert.InsertSelectDispatchPipeline.DispatchFunction
                insertSelectDispatchFn = (sm, session, plannedQuery) -> {
                    QueryDispatcher qd = queryDispatcherRef.get();
                    if (qd == null) {
                        throw new IllegalStateException("QueryDispatcher not initialized");
                    }
                    return qd.dispatchPlan(sm, session, plannedQuery);
                };

        // 2-Factory architecture: MetadataExecutionFactory + PlanExecutionFactory
        var tasks = new java.util.HashMap<Class<? extends org.pangrova.parser.ast.Statement>,
                org.pangrova.coordinator.query.execution.dispatch.MetadataTask<?>>();
        // DDL
        var alterTableTask = new org.pangrova.coordinator.query.execution.dispatch.task.AlterTableTask(ddlExecutor);
        tasks.put(org.pangrova.parser.ast.statement.CreateTableStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.CreateTableTask(ddlExecutor));
        tasks.put(org.pangrova.parser.ast.statement.DropTableStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.DropTableTask(ddlExecutor));
        tasks.put(org.pangrova.parser.ast.statement.AlterTableStatement.AddColumn.class, alterTableTask);
        tasks.put(org.pangrova.parser.ast.statement.AlterTableStatement.DropColumn.class, alterTableTask);
        tasks.put(org.pangrova.parser.ast.statement.AlterTableStatement.RenameColumn.class, alterTableTask);
        tasks.put(org.pangrova.parser.ast.statement.AlterTableStatement.ChangeColumnType.class, alterTableTask);
        tasks.put(org.pangrova.parser.ast.statement.AlterTableStatement.SetPartitionSpec.class, alterTableTask);
        tasks.put(org.pangrova.parser.ast.statement.AlterTableStatement.RenameTable.class, alterTableTask);
        tasks.put(org.pangrova.parser.ast.statement.AlterTableStatement.CreateBranch.class, alterTableTask);
        tasks.put(org.pangrova.parser.ast.statement.AlterTableStatement.DropBranch.class, alterTableTask);
        tasks.put(org.pangrova.parser.ast.statement.AlterTableStatement.CreateTag.class, alterTableTask);
        tasks.put(org.pangrova.parser.ast.statement.AlterTableStatement.DropTag.class, alterTableTask);
        tasks.put(org.pangrova.parser.ast.statement.AlterTableStatement.SetColumnTag.class, alterTableTask);
        tasks.put(org.pangrova.parser.ast.statement.AlterTableStatement.UnsetColumnTag.class, alterTableTask);
        tasks.put(org.pangrova.parser.ast.statement.CreateSchemaStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.catalog.CreateSchemaTask(ddlExecutor));
        tasks.put(org.pangrova.parser.ast.CreateCatalogStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.catalog.CreateCatalogTask(ddlExecutor));
        tasks.put(org.pangrova.parser.ast.statement.DropCatalogStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.catalog.DropCatalogTask(ddlExecutor));
        // DML (INSERT only)
        tasks.put(org.pangrova.parser.ast.InsertStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.InsertValuesTask(dmlExecutor));
        tasks.put(org.pangrova.parser.ast.statement.dml.InsertOverwriteStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.InsertOverwriteTask(dmlExecutor));
        // SHOW / DESCRIBE
        tasks.put(org.pangrova.parser.ast.ShowSchemasStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.catalog.ShowSchemasTask(showDescribeExecutor));
        tasks.put(org.pangrova.parser.ast.statement.ShowTablesStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.ShowTablesTask(showDescribeExecutor));
        tasks.put(org.pangrova.parser.ast.statement.DescribeTableStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.DescribeTableTask(showDescribeExecutor));
        tasks.put(org.pangrova.parser.ast.statement.ShowCatalogsStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.catalog.ShowCatalogsTask(showDescribeExecutor));
        // Maintenance
        tasks.put(org.pangrova.parser.ast.statement.maintenance.CompactStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.CompactTask(maintenanceExecutor));
        tasks.put(org.pangrova.parser.ast.statement.maintenance.VacuumStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.VacuumTask(maintenanceExecutor));
        tasks.put(org.pangrova.parser.ast.statement.maintenance.OptimizeStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.OptimizeTask(maintenanceExecutor));
        // CTAS: create table from derived schema, then INSERT SELECT via native DML pipeline
        tasks.put(org.pangrova.parser.ast.statement.CreateTableAsSelectStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.CtasTask(ddlExecutor, dmlDispatch));
        // Views
        tasks.put(org.pangrova.parser.ast.statement.CreateViewStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.CreateViewTask(catalog));
        tasks.put(org.pangrova.parser.ast.statement.DropViewStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.DropViewTask(catalog));
        tasks.put(org.pangrova.parser.ast.statement.ShowViewsStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.ShowViewsTask(catalog));
        // Procedures
        tasks.put(org.pangrova.parser.ast.statement.maintenance.CallStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.CallProcedureTask(catalog));
        // SHOW metadata (redirects to system table queries)
        var showMetadataTask = new org.pangrova.coordinator.query.execution.dispatch.task.ShowMetadataRedirectTask(catalog);
        tasks.put(org.pangrova.parser.ast.statement.ShowMetadataStatement.ShowSnapshots.class, showMetadataTask);
        tasks.put(org.pangrova.parser.ast.statement.ShowMetadataStatement.ShowFiles.class, showMetadataTask);
        tasks.put(org.pangrova.parser.ast.statement.ShowMetadataStatement.ShowManifests.class, showMetadataTask);
        tasks.put(org.pangrova.parser.ast.statement.ShowMetadataStatement.ShowPartitions.class, showMetadataTask);
        // Incremental read (CHANGES BETWEEN SNAPSHOT x AND y)
        tasks.put(org.pangrova.parser.ast.statement.IncrementalReadStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.IncrementalReadTask(catalog));
        // Storage Profile management
        var spRegistry = new org.pangrova.coordinator.storage.InMemoryStorageProfileRegistry();
        tasks.put(org.pangrova.parser.ast.statement.CreateStorageProfileStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.storage.CreateStorageProfileTask(spRegistry));
        tasks.put(org.pangrova.parser.ast.statement.DropStorageProfileStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.storage.DropStorageProfileTask(spRegistry));
        tasks.put(org.pangrova.parser.ast.statement.ShowStorageProfilesStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.storage.ShowStorageProfilesTask(spRegistry));
        // Branch/Tag SHOW queries
        var branchDeps = new org.pangrova.coordinator.ai.branch.BranchDdlContext(
                metadataBootstrap.catalogRegistry(), null);
        var branchExecutor = new org.pangrova.coordinator.ai.branch.BranchDdlExecutor(branchDeps);
        tasks.put(org.pangrova.parser.ast.statement.ShowBranchesStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.branch.ShowBranchesTask(branchExecutor));
        tasks.put(org.pangrova.parser.ast.statement.ShowTagsStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.branch.ShowTagsTask(branchExecutor));
        var metadataFactory = new org.pangrova.coordinator.query.execution.dispatch.MetadataExecutionFactory(tasks);
        if (dtExecutor != null) {
            metadataFactory.registerTask(org.pangrova.parser.ast.CreateDynamicTableStatement.class,
                    new org.pangrova.coordinator.query.execution.dispatch.task.CreateDynamicTableTask(dtExecutor));
            metadataFactory.registerTask(org.pangrova.parser.ast.CreateContinuousQueryStatement.class,
                    new org.pangrova.coordinator.query.execution.dispatch.task.stream.CreateContinuousQueryTask(dtExecutor));
            metadataFactory.registerTask(org.pangrova.parser.ast.statement.AlterDynamicTableStatement.class,
                    new org.pangrova.coordinator.query.execution.dispatch.task.AlterDynamicTableTask(dtExecutor));
            metadataFactory.registerTask(org.pangrova.parser.ast.statement.DropDynamicTableStatement.class,
                    new org.pangrova.coordinator.query.execution.dispatch.task.DropDynamicTableTask(dtExecutor));
            metadataFactory.registerTask(org.pangrova.parser.ast.statement.ShowDynamicTablesStatement.class,
                    new org.pangrova.coordinator.query.execution.dispatch.task.ShowDynamicTablesTask(dtExecutor));
            metadataFactory.registerTask(org.pangrova.parser.ast.statement.DescribeDynamicTableStatement.class,
                    new org.pangrova.coordinator.query.execution.dispatch.task.DescribeDynamicTableTask(dtExecutor));
        }

        var planFactory = new org.pangrova.coordinator.query.execution.dispatch.PlanExecutionFactory(
                new org.pangrova.coordinator.query.execution.dispatch.PlanExecutionFactoryContext(
                        dmlDispatch, null,
                        (sql, sess) -> sql, writeCommitTracker,
                        metadataBootstrap.catalogRegistry(), null));

        var factories = new java.util.ArrayList<
                org.pangrova.coordinator.query.execution.dispatch.StatementExecutionFactory<?>>();
        factories.add(metadataFactory);
        factories.add(planFactory);

        var context = new org.pangrova.coordinator.query.execution.dispatch.StatementDispatcherContext(
                null, resultSvc, List.copyOf(factories), List.of(), null);
        return new org.pangrova.coordinator.query.execution.dispatch.StatementDispatcher(context);
    }

    private static org.pangrova.coordinator.dynamictable.DynamicTableDdlExecutor buildDynamicTableDdlExecutor() {
        var stateStore = new org.pangrova.metadata.dynamictable.InMemoryDynamicTableStateStore();
        var historyStore = new org.pangrova.metadata.dynamictable.InMemoryRefreshHistoryStore();
        sharedMvManager = new org.pangrova.planner.mv.MaterializedViewManager();
        // Force DynamicTableRewriter recreation so it binds to the new MvManager instance.
        // Without this, a stale rewriter from a previous rebuildQueryPipeline() cycle
        // would reference the old MvManager that has no MV definitions registered.
        sharedDtRewriter = null;
        var dagManager = new org.pangrova.coordinator.dynamictable.DynamicTableDagManager();
        var feasibilityChecker = new org.pangrova.coordinator.dynamictable.eval.IncrementalFeasibilityChecker();
        var cdcLifecycleManager = new ConfigurableCdcLifecycleManager();
        var config = org.pangrova.coordinator.dynamictable.DynamicTableConfig.defaults();
        org.pangrova.coordinator.dynamictable.refresh.pipeline.RefreshExecutor refreshExecutor =
                SqlE2ETestBase::executeRefresh;
        var schedulerDeps = new org.pangrova.coordinator.dynamictable.schedule.RefreshSchedulerContext(
                refreshExecutor, stateStore, config);
        var scheduler = new org.pangrova.coordinator.dynamictable.schedule.RefreshScheduler(schedulerDeps);
        var deps = new org.pangrova.coordinator.dynamictable.DynamicTableDdlContext(
                stateStore, historyStore, sharedMvManager, scheduler,
                dagManager, feasibilityChecker, cdcLifecycleManager,
                refreshExecutor, config);
        return new org.pangrova.coordinator.dynamictable.DynamicTableDdlExecutor(deps);
    }

    /**
     * RefreshExecutor: registers a target override in the shared DynamicTableRewriter.
     *
     * <p>For simple {@code SELECT * FROM table} queries, the override points directly
     * to the source table (zero-lag pass-through). For complex queries (GROUP BY,
     * JOIN, DISTINCT, etc.), the override wraps the defining query as a subquery
     * so aggregation/transformation is applied on every read.</p>
     */
    private static org.pangrova.coordinator.dynamictable.refresh.pipeline.RefreshResult executeRefresh(
            org.pangrova.metadata.dynamictable.DynamicTableState state,
            org.pangrova.metadata.dynamictable.TriggerType trigger) {
        String dtName = state.dtName();
        if (sharedMvManager == null) {
            LOG.warn("event=dt_refresh_skip reason=no_mv_manager dtName={}", dtName);
            return org.pangrova.coordinator.dynamictable.refresh.pipeline.RefreshResult.noOp();
        }
        var mvDef = sharedMvManager.getDefinition(dtName);
        if (mvDef.isEmpty()) {
            LOG.warn("event=dt_refresh_skip reason=no_mv_definition dtName={}", dtName);
            return org.pangrova.coordinator.dynamictable.refresh.pipeline.RefreshResult.noOp();
        }
        try {
            String definingQuery = mvDef.get().query();
            if (isSimpleSelectStar(definingQuery)) {
                String sourceTable = extractSourceTableFromQuery(definingQuery);
                registerSourceTableOverride(dtName, sourceTable);
            } else {
                // Complex query (GROUP BY, JOIN, etc.) — wrap as inline subquery
                // so the transformation is applied on every read.
                String subquery = "(" + definingQuery + ") AS " + dtName;
                registerSourceTableOverride(dtName, subquery);
            }
            enableQueryRewrite(dtName);
            LOG.info("event=dt_refresh_complete dtName={}", dtName);
            return new org.pangrova.coordinator.dynamictable.refresh.pipeline.RefreshResult(
                    org.pangrova.metadata.dynamictable.RefreshStatus.SUCCESS, 0, 0,
                    String.valueOf(System.currentTimeMillis()), 0);
        } catch (Exception e) {
            LOG.error("event=dt_refresh_failed dtName={} error={}", dtName, e.getMessage());
            return org.pangrova.coordinator.dynamictable.refresh.pipeline.RefreshResult.failure(0);
        }
    }

    /**
     * Detects whether a defining query is a simple {@code SELECT * FROM table}
     * that can use pass-through resolution (no GROUP BY, JOIN, DISTINCT, etc.).
     */
    private static boolean isSimpleSelectStar(String definingQuery) {
        try {
            var parser = new org.pangrova.planner.parse.PangrovaParserFactory();
            var ast = parser.parseToAst(definingQuery);
            if (!(ast instanceof org.pangrova.parser.ast.Query query)) {
                return false;
            }
            boolean allStar = query.selectItems().stream()
                    .allMatch(item -> item instanceof org.pangrova.parser.ast.AllColumns);
            return allStar
                    && !query.distinct()
                    && query.groupBy().isEmpty()
                    && query.having().isEmpty()
                    && query.joins().isEmpty()
                    && query.where().isEmpty();
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Extracts the source table name from a defining query (e.g. "SELECT * FROM catalog.schema.table").
     * Parses the SQL to AST and returns the last table reference's simple name.
     */
    private static String extractSourceTableFromQuery(String definingQuery) {
        try {
            var parser = new org.pangrova.planner.parse.PangrovaParserFactory();
            var ast = parser.parseToAst(definingQuery);
            if (ast instanceof org.pangrova.parser.ast.Query query && !query.from().isEmpty()) {
                var tableRef = query.from().get(0);
                List<String> parts = tableRef.name().parts();
                return String.join(".", parts);
            }
        } catch (Exception e) {
            LOG.warn("event=dt_source_extract_failed query={} error={}", definingQuery, e.getMessage());
        }
        // Fallback: strip "SELECT * FROM " prefix and return the full table reference
        String stripped = definingQuery.replaceFirst("(?i)SELECT\\s+.*\\s+FROM\\s+", "").trim();
        return stripped.replaceAll("[\\s;].*", "");
    }

    /**
     * Registers a target override in the shared DynamicTableRewriter so that
     * queries against the DT resolve to the source table directly.
     * Uses the simple (unqualified) name as key because DynamicTableSqlRewriter
     * looks up overrides by simple name extracted from the SQL AST.
     */
    private static void registerSourceTableOverride(String dtName, String sourceTable) {
        if (sharedDtRewriter != null) {
            String simpleName = dtName.contains(".")
                    ? dtName.substring(dtName.lastIndexOf('.') + 1)
                    : dtName;
            sharedDtRewriter.registerTargetOverride(simpleName, sourceTable);
        }
    }

    /**
     * Flips enableQueryRewrite=true on the MV definition so DynamicTableRewriter
     * can resolve SELECT queries against this DT.
     */
    private static void enableQueryRewrite(String dtName) {
        var mvDef = sharedMvManager.getDefinition(dtName);
        if (mvDef.isPresent()) {
            sharedMvManager.register(mvDef.get().withEnableQueryRewrite(true));
        }
    }

    /**
     * Safely drops a Dynamic Table if it exists. Silently ignores errors
     * (e.g. DT does not exist). Used by tests with multiple @Nested classes
     * that share the same DT name to ensure test isolation.
     */
    protected static void ensureDynamicTableDropped(String dtFullName) {
        try {
            sqlRunner.execute("DROP DYNAMIC TABLE " + dtFullName);
        } catch (Exception ignored) {
            // DT may not exist — that's fine
        }
    }

    /**
     * Hook for subclasses to indicate CDC availability.
     * Override to return true in CDC test classes.
     */
    protected static boolean cdcAvailable() {
        return cdcAvailableFlag;
    }

    protected static SqlCompiler buildSqlCompiler() {
        CalciteSchema rootSchema = metadataBootstrap.buildCalciteSchema();
        if (sharedMvManager != null && sharedDtRewriter == null) {
            sharedDtRewriter = new org.pangrova.planner.mv.DynamicTableRewriter(sharedMvManager);
        }
        org.pangrova.planner.PlannerContext deps =
                new org.pangrova.planner.PlannerContext(
                        null, null, null, null, null, sharedDtRewriter, null);
        PangrovaPlanner planner = new PangrovaPlanner(rootSchema, defaultSchemaPath, deps);
        return new SqlCompiler(planner, new SubstraitConverter(), new SubstraitPlanSerializer());
    }

    private static MetadataBootstrap initializeMetadata() {
        try {
            System.err.println("[E2E-DEBUG] initializeMetadata: creating datasource");
            DataSource ds = MetadataSchemaInitializer.createDataSource(
                    pgJdbcUrl(), pgUsername(), pgPassword());
            System.err.println("[E2E-DEBUG] initializeMetadata: datasource created");
            MetadataSchemaInitializer.createSchema(ds);
            System.err.println("[E2E-DEBUG] initializeMetadata: schema created");
            MetadataSchemaInitializer.createStateStoreTables(ds);
            System.err.println("[E2E-DEBUG] initializeMetadata: state store tables created");
            stateStoreDataSource = ds;
            MetadataStore store = new JdbcMetadataStore(ds);
            System.err.println("[E2E-DEBUG] initializeMetadata: JdbcMetadataStore created");
            TestConnectorHandleFactory connectorFactory = new TestConnectorHandleFactory();
            MetadataBootstrap bootstrap = MetadataBootstrap.initialize(store, connectorFactory);
            System.err.println("[E2E-DEBUG] initializeMetadata: bootstrap initialized");
            LOG.info("MetadataBootstrap initialized with state store tables");
            return bootstrap;
        } catch (Exception e) {
            throw new IllegalStateException(
                    "MetadataBootstrap initialization failed — PostgreSQL may not be ready", e);
        }
    }

    private static void shutdownMetadata() {
        if (metadataBootstrap != null) {
            metadataBootstrap.shutdown();
        }
    }

    /**
     * Reinitializes metadata after PostgreSQL container restart.
     * The restart changes the mapped port, so all DataSource instances holding
     * the old JDBC URL become stale. This method:
     * 1. Shuts down the old MetadataBootstrap (closes stale connections)
     * 2. Creates a new DataSource with the new JDBC URL
     * 3. Recreates schema tables (idempotent via IF NOT EXISTS)
     * 4. Rebuilds MetadataBootstrap and CatalogRegistry
     * 5. Rebuilds the query pipeline with the new metadata
     */
    private static void reinitializeMetadataAfterPgRestart() {
        LOG.warn("event=pg_metadata_reinit newUrl={}", pgJdbcUrl());
        shutdownMetadata();
        metadataBootstrap = initializeMetadata();
        registerNativeWriter();
        lastRegisteredCatalogs.clear();
        rebuildQueryPipeline();
        LOG.info("event=pg_metadata_reinit_complete");
    }

    private static void waitForPostgresReady(Duration timeout) throws Exception {
        SharedTestContainers.probeJdbcReady(POSTGRES, timeout);
        LOG.info("event=pg_ready_check_passed url={}", pgJdbcUrl());
    }

    /**
     * Registers a Java NativeWriteExecutor with the C++ native bridge so that
     * WRITE tasks can call back into Java for connector page sink operations.
     * Must be called after metadataBootstrap is initialized (CatalogRegistry available).
     */
    private static void registerNativeWriter() {
        if (metadataBootstrap == null || !NativeRuntime.isInitialized()) {
            return;
        }
        var catalogRegistry = metadataBootstrap.catalogRegistry();
        org.pangrova.worker.jni.write.CatalogResourceResolver resolver = catalogName -> {
            var catalogHandle = catalogRegistry.getCatalog(catalogName);
            var session = org.pangrova.coordinator.query.statement.TableResolver.buildSession(catalogName);
            var metadata = catalogHandle.getMetadata(session);
            var sinkProvider = catalogHandle.getPageSinkProvider();
            return new org.pangrova.worker.jni.write.CatalogResourceResolver.CatalogResources(
                    metadata, sinkProvider, session);
        };
        var writer = new org.pangrova.worker.jni.write.NativeWriteExecutor(resolver);
        NativeRuntime.nativeRegisterWriter(writer);
        sharedNativeWriteExecutor = writer;
        // If the worker TaskManager has already been created (e.g. metadata reinit
        // after the worker is up), wire the new writer immediately so that
        // TaskStatusCollector can drain commit metadata for write tasks.
        if (workerTaskManagerRef != null) {
            workerTaskManagerRef.setWriteExecutor(writer);
        }
        LOG.info("event=native_writer_registered");
    }

    protected static void registerWorker(int workerHttpPort, int transportPort) {
        WorkerInfo workerInfo = new WorkerInfo(
                WORKER_ID, "localhost", workerHttpPort, transportPort, sharedResultServerPort,
                HealthState.ACTIVE, MemoryPressure.NONE,
                0.0, 0, WORKER_MEMORY_BYTES,
                Instant.now(), 1.0, null, 1, List.of(),
                null, List.of());
        workerManager.registerWorker(workerInfo);
    }

    protected static void configureStatusReporter() {
        if (coordinatorHttpServer == null || statusReporter == null) {
            return;
        }
        statusReporter.updateTarget("http://localhost:" + coordinatorHttpServer.getPort());
    }

    /**
     * Redirects the worker's status reporter to a specific port.
     * Used by {@code MultiWorkerTestBase} to point the shared worker at the
     * multi-worker coordinator without overwriting {@link #coordinatorHttpServer}.
     */
    protected static void configureStatusReporterForPort(int port) {
        if (statusReporter == null) {
            return;
        }
        statusReporter.updateTarget("http://localhost:" + port);
    }

    // -----------------------------------------------------------------------
    // Server lifecycle helpers
    // -----------------------------------------------------------------------

    private static void startServerOrFail(CoordinatorHttpServer server) {
        try {
            server.start();
        } catch (Exception e) {
            throw new RuntimeException("Failed to start CoordinatorHttpServer", e);
        }
        // Structural invariant: the coordinator HTTP port is dynamic (rebuilds
        // may fall back to a random port), and the shared worker's
        // RemoteTaskStatusReporter MUST always point at the live port. A stale
        // target turns every subsequent query into a 300s DAG dispatch timeout
        // (status reports die with Connection refused). Syncing here — on every
        // successful start — covers all bootstrap and rebuild paths.
        configureStatusReporter();
    }

    protected static void startServerOrFail(WorkerHttpServer server) {
        try {
            server.start();
        } catch (Exception e) {
            throw new RuntimeException("Failed to start WorkerHttpServer", e);
        }
    }

    private static void stopQuietly(CoordinatorHttpServer server) {
        if (server == null) {
            return;
        }
        try {
            server.stop();
        } catch (Exception e) {
            // test infrastructure — swallow stop errors
        }
    }

    /** Closes any AutoCloseable resource, logging but swallowing errors. */
    private static void closeQuietly(AutoCloseable resource, String name) {
        if (resource == null) {
            return;
        }
        try {
            resource.close();
        } catch (Exception e) {
            LOG.warn("event=resource_close_error resource={} error={}", name, e.getMessage());
        }
    }

    protected static void stopQuietly(WorkerHttpServer server) {
        if (server == null) {
            return;
        }
        try {
            server.stop();
        } catch (Exception e) {
            // test infrastructure — swallow stop errors
        }
    }

    private static void stopCoordinatorTransportQuietly() {
        if (coordinatorTransportServer == null) {
            return;
        }
        try {
            coordinatorTransportServer.stop();
        } catch (Exception e) {
            // test infrastructure — swallow stop errors
        }
        coordinatorTransportServer = null;
    }

    // -----------------------------------------------------------------------
    // Operations bootstrap helpers
    // -----------------------------------------------------------------------

    private static HealthCheckEndpoint createCoordinatorHealthEndpoint() {
        return new HealthCheckEndpoint(
                HealthCheckEndpoint.NodeType.COORDINATOR,
                new CoordinatorHealthConditions());
    }

    private static HealthCheckEndpoint createWorkerHealthEndpoint() {
        return new HealthCheckEndpoint(
                HealthCheckEndpoint.NodeType.WORKER,
                new WorkerHealthConditions());
    }

    /** Coordinator health: HTTP ready + ≥1 worker + ≥1 catalog. */
    private static final class CoordinatorHealthConditions implements HealthCheckEndpoint.HealthConditions {
        @Override public boolean isHttpReady() { return coordinatorHttpReady.get(); }
        @Override public boolean isTransportReady() { return false; }
        @Override public boolean isNativeReady() { return false; }
        @Override public int activeWorkerCount() {
            return workerManager != null ? workerManager.getActiveWorkers().size() : 0;
        }
        @Override public int activeCatalogCount() {
            return metadataBootstrap != null ? 1 : 0;
        }
    }

    /** Worker health: HTTP ready + Transport ready + native ready. */
    private static final class WorkerHealthConditions implements HealthCheckEndpoint.HealthConditions {
        @Override public boolean isHttpReady() { return workerHttpReady.get(); }
        @Override public boolean isTransportReady() { return workerTransportReady.get(); }
        @Override public boolean isNativeReady() { return workerNativeReady.get(); }
        @Override public int activeWorkerCount() { return 0; }
        @Override public int activeCatalogCount() { return 0; }
    }

    /**
     * Configurable CDC lifecycle manager for test environment.
     * Returns cdcAvailableFlag for isAvailable() — subclasses set the flag
     * to enable CDC mode DT creation in their @BeforeAll.
     */
    private static final class ConfigurableCdcLifecycleManager
            implements org.pangrova.spi.streaming.CdcLifecycleManager {
        @Override public boolean isAvailable(String tableName) { return cdcAvailableFlag; }
        @Override public String resolveOffsetFromSnapshot(String tableName, String snapshotCheckpoint) {
            return "offset-0";
        }
        @Override public CdcRegistrationResult register(String consumerId, String tenantId,
                                                        int maxConcurrentWrites, long maxPendingBytes) {
            return new CdcRegistrationResult("noop", maxConcurrentWrites, maxPendingBytes, 5000, 30000);
        }
        @Override public void heartbeat(String streamId) { }
        @Override public void expireStaleStreams() { }
    }
}
