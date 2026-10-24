package org.pangrova.testing.e2e;

import org.pangrova.coordinator.query.execution.AsyncSubmitConfig;

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
import org.pangrova.coordinator.result.CoordinatorResultBuffer;
import org.pangrova.coordinator.result.CoordinatorResultService;
import org.pangrova.coordinator.result.CoordinatorResultWriter;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
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
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.resource.MemoryPressure;
import org.pangrova.resource.ResourceManager;
import org.pangrova.resource.memory.MemoryArbitrator;
import org.pangrova.resource.memory.NodeMemoryPool;
import org.pangrova.resource.memory.RootMemoryPool;
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
import java.util.concurrent.ExecutorService;
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

    protected static final String WORKER_ID = "e2e-worker-1";
    private static final String IN_PROCESS_WORKER_HOST = resolveWorkerAdvertisedHost();

    protected static String workerAdvertisedHost() {
        return IN_PROCESS_WORKER_HOST;
    }

    private static String resolveWorkerAdvertisedHost() {
        try {
            for (var interfaces = java.net.NetworkInterface.getNetworkInterfaces();
                    interfaces.hasMoreElements();) {
                var network = interfaces.nextElement();
                if (!network.isUp() || network.isLoopback()) {
                    continue;
                }
                for (var addresses = network.getInetAddresses(); addresses.hasMoreElements();) {
                    var address = addresses.nextElement();
                    if (address instanceof java.net.Inet4Address
                            && !address.isLoopbackAddress()
                            && !address.isLinkLocalAddress()) {
                        return address.getHostAddress();
                    }
                }
            }
        } catch (java.net.SocketException failure) {
            LOG.warn("event=e2e_worker_host_lookup_failed error={}", failure.getMessage());
        }
        return "localhost";
    }
    private static final String E2E_COORDINATOR_ID = "e2e-coordinator";
    protected static final String E2E_INTERNAL_AUTH_SECRET =
            "e2e-internal-service-credential-key";
    protected static final String E2E_USER_BEARER_TOKEN = "e2e-user-token";
    private static final String E2E_USER_PRINCIPAL = "e2e-user";
    private static final String E2E_TENANT_ID = "e2e-tenant";
    private static final long WORKER_MEMORY_BYTES = 2L * 1024 * 1024 * 1024; // 2GB — balances Velox memory needs vs OS process limits
    // Finite deployment limits for the authenticated shared fixture, not production defaults.
    private static final org.pangrova.security.tenant.model.ResourceQuota E2E_TENANT_QUOTA =
            new org.pangrova.security.tenant.model.ResourceQuota(
                    WORKER_MEMORY_BYTES, 16, 128, 600_000, 1024L * 1024 * 1024,
                    100_000, 64L * 1024 * 1024 * 1024, 4L * 1024 * 1024 * 1024);
    // A small explicit fixture ceiling, not a production sizing recommendation.
    // This is separate from the shuffle disk service budget and directory.
    private static final long NATIVE_WRITE_STAGE_BYTES = 16L * 1024 * 1024;
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
    private static volatile java.util.function.Consumer<org.pangrova.protocol.rest.task.TaskStatusReport>
            statusReportDelivery;
    private static org.pangrova.coordinator.query.task.write.NativeWriteCommitTracker writeCommitTracker;

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
    private static ScheduledExecutorService terminalCleanupTimer;
    private static ExecutorService terminalCleanupWorker;
    private static ScheduledExecutorService controlEpochObserver;
    private static volatile ExecutorService asyncQueryExecutor;

    private static volatile boolean infrastructureInitialized;
    private static volatile Throwable infrastructureStartupFailure;
    private static Thread infrastructureShutdownHook;

    // Worker-side resources that must be explicitly closed in teardown.
    // Without tracking these, each test class leaks thread pools and native handles.
    private static volatile TaskManager workerTaskManagerRef;
    private static org.pangrova.common.lifecycle.ProcessIncarnationStore workerAdmissionIncarnation;
    // The native write executor registered with the C++ bridge. We keep a reference
    // so that, once the worker TaskManager is created, we can wire it for per-task
    // commit metadata draining (TaskStatusCollector relies on this to surface
    // commit metadata to the coordinator).
    private static volatile org.pangrova.worker.jni.write.NativeWriteExecutor sharedNativeWriteExecutor;
    private static volatile org.pangrova.worker.jni.write.catalog.WorkerCatalogRuntime sharedWorkerCatalogRuntime;
    private static volatile String workerCatalogCoordinatorUrl;
    private static volatile TaskExecutor sharedWorkerTaskExecutor;
    private static volatile int sharedWorkerHttpPort;
    private static volatile org.pangrova.coordinator.metadata.NativeDatasetControlClient sharedDatasetControlClient;
    private static volatile org.pangrova.spi.coordination.WriteCommitJournal sharedWriteCommitJournal;
    private static volatile TaskStatusReceiver sharedTaskStatusReceiver;
    private static final org.pangrova.coordinator.query.task.write.WriteCommitFencer E2E_WRITE_COMMIT_FENCER =
            new E2eWriteCommitFencer();
    // Additional resources that leak if not closed between test classes.
    private static volatile org.apache.arrow.memory.RootAllocator coordinatorArrowAllocator;
    private static volatile org.pangrova.coordinator.bootstrap.IndexingBootstrap sharedIndexingBootstrap;
    private static volatile org.pangrova.metadata.artifact.build.ArtifactBuildStore
            sharedArtifactBuildStore;
    private static volatile org.pangrova.coordinator.artifact.DurableArtifactBuildDispatcher
            sharedArtifactBuildDispatcher;
    private static volatile RemoteTaskDispatcher artifactTaskDispatcher;
    private static volatile org.pangrova.coordinator.artifact.DurableArtifactPublisher
            sharedArtifactPublisher;
    private static volatile ScheduledExecutorService artifactDispatchExecutor;
    private static volatile RemoteTaskStatusReporter sharedStatusReporter;
    // Coordinator-side resources that leak HttpClient thread pools if not closed.
    private static volatile RemoteTaskDispatcher sharedTaskDispatcher;
    private static volatile QueryDispatcher sharedQueryDispatcher;
    private static volatile org.pangrova.coordinator.query.execution.dispatch.StatementDispatcher
            sharedStatementDispatcher;
    private static volatile org.pangrova.coordinator.query.execution.dispatch.StatementDispatcher
            materializationDispatcher;
    private static volatile org.pangrova.metadata.materialization.JdbcMaterializationRepository
            materializationRepository;
    private static volatile org.pangrova.metadata.materialization.JdbcMaterializationCatalog
            materializationCatalog;
    private static volatile org.pangrova.spi.security.AccessControl featureAccessControl;
    private static volatile boolean durableStorageProfileScenario;
    private static volatile org.pangrova.spi.security.AccessControl currentAccessControl;
    private static volatile org.pangrova.coordinator.ai.embedding.GeneratedValueMaterializationCoordinator
            sharedGeneratedValueCoordinator;
    private static volatile org.pangrova.coordinator.query.execution.GeneratedWriteAuthoritySupplier
            sharedWriteAuthoritySupplier;
    private static volatile org.pangrova.coordinator.coordination.HighAvailabilityCoordinationService
            e2eCoordinationService;
    private static volatile org.pangrova.worker.memory.NativeMemoryCoordinator nativeMemoryCoordinatorRef;
    private static volatile MemoryArbitrator sharedWorkerMemoryArbitrator;
    private static volatile NodeMemoryPool sharedWorkerNodePool;
    private static volatile org.pangrova.worker.bootstrap.phase.ShuffleServiceMemoryLease
            sharedShuffleServiceLease;
    private static volatile org.pangrova.worker.result.ResultReadTickets sharedResultReadTickets;
    private static volatile org.pangrova.coordinator.query.statement.DdlExecutor sharedDdlExecutor;
    protected static volatile int sharedResultServerPort;
    private static final SharedNativeFunctionStartup SHARED_NATIVE_FUNCTION_STARTUP =
            new SharedNativeFunctionStartup();
    private static volatile org.pangrova.security.authn.AuthenticationManager
            e2eAuthenticationManager;
    private static volatile org.pangrova.worker.bootstrap.NativeCapabilityAdvertisement.Advertisement
            e2eCapabilityAdvertisement;

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

    /** Fails jqwik properties when required E2E infrastructure is unavailable. */
    @net.jqwik.api.lifecycle.BeforeProperty
    void requirePropertyInfrastructure() {
        requireInfrastructureReady();
    }

    /**
     * Auto-registers connectors on first test execution per subclass.
     * Subclasses only need to implement {@link #connectorConfigs()} — no manual
     * {@code registerTestConnectors()} call required in subclass @BeforeAll.
     */
    @BeforeEach
    void ensureConnectorsRegistered() {
        requireInfrastructureReady();
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
        Throwable retainedFailure = infrastructureStartupFailure;
        if (retainedFailure != null) {
            throw new IllegalStateException("Shared E2E infrastructure startup previously failed", retainedFailure);
        }
        try {
            bootstrapSharedTestEnvironment();
        } catch (RuntimeException | Error failure) {
            if (!infrastructureInitialized) infrastructureStartupFailure = failure;
            throw failure;
        }
    }

    private static void bootstrapSharedTestEnvironment() {
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
        isDockerReady();

        if (tempDir == null) {
            try {
                java.nio.file.Path tmpRoot = java.nio.file.Path.of(System.getProperty("java.io.tmpdir"));
                java.nio.file.Files.createDirectories(tmpRoot);
                tempDir = java.nio.file.Files.createTempDirectory("pangrova-e2e-shared");
            } catch (java.io.IOException e) {
                throw new IllegalStateException("Failed to create shared temp directory", e);
            }
        }

        if (!org.pangrova.worker.jni.NativeLibraryLoader.isAvailable()) {
            throw new IllegalStateException(
                    "Native library is required for pangrova-test-e2e; run `cmake --build` "
                            + "and `mvn process-test-resources -pl pangrova-testing/pangrova-test-e2e`");
        }
        if (NativeRuntime.isInitialized()) {
            throw new IllegalStateException("Shared E2E startup cannot borrow a foreign initialized Native runtime");
        }
        SHARED_NATIVE_FUNCTION_STARTUP.prepare();
        try {
            initializeSharedNativeRuntime();
            e2eCapabilityAdvertisement = SHARED_NATIVE_FUNCTION_STARTUP.initialize();
            registerSharedInfrastructureShutdown();
        } catch (RuntimeException | Error failure) {
            closeFailedNativeStartup(failure);
            throw failure;
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
    }

    private static synchronized void registerSharedInfrastructureShutdown() {
        if (infrastructureShutdownHook != null) return;
        Thread hook = new Thread(SqlE2ETestBase::shutdownSharedInfrastructure, "e2e-infra-cleanup");
        Runtime.getRuntime().addShutdownHook(hook);
        infrastructureShutdownHook = hook;
    }

    /** Actual runtime-owned immutable artifact cache, isolated by the E2E temp root. */
    protected static Path nativeArtifactCacheDirectory() {
        return tempDir.resolve("native-artifact-cache");
    }

    private static void initializeSharedNativeRuntime() {
        Path shuffleSpillDirectory = tempDir.resolve("native-shuffle-spill");
        try {
            java.nio.file.Files.createDirectories(shuffleSpillDirectory);
        } catch (java.io.IOException e) {
            throw new IllegalStateException("Failed to create explicit E2E Native shuffle spill directory", e);
        }
        Path writeStageDirectory = prepareNativeWriteStage();
        MemoryArbitrator arbitrator = new MemoryArbitrator(
                WORKER_MEMORY_BYTES, 128L * 1024 * 1024);
        NodeMemoryPool nodePool = new RootMemoryPool(
                "root-" + WORKER_ID, WORKER_MEMORY_BYTES)
                .createNodePool("node-" + WORKER_ID, WORKER_MEMORY_BYTES);
        var serviceLease = org.pangrova.worker.bootstrap.phase.ShuffleServiceMemoryLease.reserve(
                WORKER_ID, 16L * 1024 * 1024, 16L * 1024 * 1024, nodePool, arbitrator);
        sharedWorkerMemoryArbitrator = arbitrator;
        sharedWorkerNodePool = nodePool;
        sharedShuffleServiceLease = serviceLease;
        byte[] configBytes = org.pangrova.protocol.v2.RuntimeInitConfig.newBuilder()
                .setMaxMemoryBytes(WORKER_MEMORY_BYTES)
                .setIoThreadCount(4)
                // Same immutable-artifact substrate required by production ANN
                // capability advertisement. SSD and parsed-index RAM are bounded;
                // the RAM cache uses the existing Native allocator/governor.
                .setCacheEnabled(true)
                .setRamCacheBytes(32L * 1024 * 1024)
                .setArtifactCachePath(nativeArtifactCacheDirectory().toString())
                .setArtifactCacheBytes(256L * 1024 * 1024)
                .setArtifactIndexMemoryBytes(64L * 1024 * 1024)
                .setSpillEnabled(true)
                .setSpillDirectory(writeStageDirectory.toString())
                .setShuffleRetentionBytes(16L * 1024 * 1024)
                .setShuffleTransitBytes(16L * 1024 * 1024)
                .setShuffleSpillBytes(1024L * 1024 * 1024)
                .setShuffleSpillFiles(256)
                .setShuffleSpillDirectory(shuffleSpillDirectory.toString())
                .build()
                .toByteArray();
        try {
            var coordinator = new org.pangrova.worker.memory.NativeMemoryCoordinator(
                    WORKER_MEMORY_BYTES, arbitrator);
            nativeMemoryCoordinatorRef = coordinator;
            NativeRuntime.initNativeRuntime(configBytes, coordinator);
            // This in-process Worker does not run WorkerTaskBootstrap, which
            // installs a process-local signing key before exposing Native
            // Flight shuffle tickets. Mirror that production security step.
            byte[] shuffleKey = new byte[32];
            new java.security.SecureRandom().nextBytes(shuffleKey);
            NativeRuntime.nativeSetHmacKey(shuffleKey, new byte[0]);
            sharedResultReadTickets = new org.pangrova.worker.result.ResultReadTickets(shuffleKey);
        } catch (UnsatisfiedLinkError e) {
            org.pangrova.worker.jni.NativeLibraryLoader.markUnsupported();
            throw new IllegalStateException(
                    "Native library ABI mismatch; rebuild the native library and test resources",
                    e);
        }
    }

    private static void closeFailedNativeStartup(Throwable failure) {
        try {
            NativeRuntime.shutdownNativeRuntime();
            if (workerAdmissionIncarnation != null) {
                workerAdmissionIncarnation.closeAfterDrain(() -> {});
                workerAdmissionIncarnation = null;
            }
            sharedResultReadTickets = null;
            if (sharedShuffleServiceLease != null) {
                sharedShuffleServiceLease.close();
                sharedShuffleServiceLease = null;
            }
            SHARED_NATIVE_FUNCTION_STARTUP.close();
        } catch (RuntimeException | Error cleanupFailure) {
            if (cleanupFailure != failure) failure.addSuppressed(cleanupFailure);
        }
    }

    private static Path prepareNativeWriteStage() {
        Path directory = tempDir.resolve("native-write-stage");
        try {
            java.nio.file.Files.createDirectories(directory);
            long usableBytes = java.nio.file.Files.getFileStore(directory).getUsableSpace();
            if (usableBytes < NATIVE_WRITE_STAGE_BYTES) {
                throw new IllegalStateException("E2E Native write stage requires " + NATIVE_WRITE_STAGE_BYTES
                        + " available bytes; actual usable bytes=" + usableBytes);
            }
            return directory;
        } catch (java.io.IOException failure) {
            throw new IllegalStateException("Failed to prepare independent E2E Native write stage", failure);
        }
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
        stopArtifactControlLoops();
        if (controlEpochObserver != null) {
            controlEpochObserver.shutdownNow();
            controlEpochObserver = null;
        }
        closeE2ECoordination();
        stopQuietly(workerHttpServer);
        closeWorkerCatalogRuntime();
        closeQuietly(sharedDatasetControlClient, "native_dataset_control_client");
        sharedDatasetControlClient = null;
        stopCoordinatorTransportQuietly();
        stopQuietly(coordinatorHttpServer);
        if (queryManager != null) queryManager.stopPeriodicCleanup();
        if (terminalCleanupTimer != null) terminalCleanupTimer.shutdownNow();
        if (terminalCleanupWorker != null) terminalCleanupWorker.shutdownNow();
        if (resultBufferExecutor != null) resultBufferExecutor.shutdownNow();
        if (asyncQueryExecutor != null) asyncQueryExecutor.shutdownNow();
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
        closeQuietly(e2eAuthenticationManager, "authentication_manager");
        // Flight may retain IPC batches after a task exits. Only release its
        // process-lifetime NodePool/arbitrator lease after Native confirms stop.
        NativeRuntime.shutdownNativeRuntime();
        if (workerAdmissionIncarnation != null) {
            workerAdmissionIncarnation.closeAfterDrain(() -> {});
            workerAdmissionIncarnation = null;
        }
        sharedResultReadTickets = null;
        SHARED_NATIVE_FUNCTION_STARTUP.close();
        if (sharedShuffleServiceLease != null) {
            sharedShuffleServiceLease.close();
            sharedShuffleServiceLease = null;
        }
        if (POSTGRES != null) POSTGRES.stop();
        if (sharedNetwork != null) sharedNetwork.close();
    }

    private static void resetPerClassState() {
        registeredClasses.clear();
        lastRegisteredCatalogs.clear();
        defaultSchemaPath = List.of("");
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
                } catch (org.awaitility.core.ConditionTimeoutException timeout) {
                    throw new IllegalStateException(
                            "Native tasks did not quiesce before inter-class budget reset", timeout);
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException("Native task drain failed; retaining all memory reservations", e);
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

    static void verifyRequiredInfrastructure(
            boolean dockerReady,
            boolean nativeFunctional,
            int resultServerPort,
            boolean runnerReady) {
        if (!dockerReady) {
            throw new IllegalStateException(
                    "Docker is required for pangrova-test-e2e but is unavailable");
        }
        if (!nativeFunctional) {
            throw new IllegalStateException(
                    "The packaged native library is required and must match the Java ABI");
        }
        if (resultServerPort <= 0) {
            throw new IllegalStateException(
                    "The native result server is required for E2E SELECT results");
        }
        if (!runnerReady) {
            throw new IllegalStateException(
                    "The E2E SQL runner was not initialized");
        }
    }

    protected static void requireInfrastructureReady() {
        verifyRequiredInfrastructure(
                isDockerReady(),
                org.pangrova.worker.jni.NativeLibraryLoader.isFunctional(),
                sharedResultServerPort,
                sqlRunner != null);
    }

    /** Borrows the real deployment retained by the shared Native callback owner. */
    protected static org.pangrova.testing.e2e.ai.SharedAiProvider sharedAiProvider() {
        return SHARED_NATIVE_FUNCTION_STARTUP.provider();
    }

    /** Borrows the actual FILE storage deployment retained by the one shared callback owner. */
    protected static SharedGovernedFileStorage sharedFileStorage() {
        return SHARED_NATIVE_FUNCTION_STARTUP.fileStorage();
    }

    /**
     * Registers connector catalogs and rebuilds the query pipeline.
     * Must be called from subclass @BeforeAll after base class initialization completes.
     * Catalogs are registered via CatalogRegistry; Calcite schema and orchestrator are rebuilt.
     */
    protected static void registerTestConnectors(List<ConnectorTestConfig> configs) {
        requireInfrastructureReady();
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
            if (registry.listCatalogNames().contains(config.catalogName())) {
                registry.unregister(config.catalogName());
            }
        }
        TestCatalogRegistrar.registerAll(registry, configs, E2eCatalogSession::new);
        lastRegisteredCatalogs.clear();
        lastRegisteredCatalogs.addAll(catalogNames);
        rebuildQueryPipeline();
        LOG.info("Registered {} test connectors and rebuilt query pipeline", configs.size());
    }

    private record E2eCatalogSession(String catalogName) implements org.pangrova.spi.connector.ConnectorSession {
        @Override
        public java.util.Optional<org.pangrova.spi.connector.ConnectorQueryAuthority> getQueryAuthority() {
            return java.util.Optional.empty();
        }
        @Override public String getUser() { return E2E_USER_PRINCIPAL; }
        @Override public java.util.Optional<String> getCatalog() { return java.util.Optional.of(catalogName); }
        @Override public java.util.Optional<String> getSchema() { return java.util.Optional.empty(); }
        @Override public Map<String, String> getProperties() { return Map.of("tenant_id", E2E_TENANT_ID); }
    }

    /** Returns the catalog session corresponding to this fixture's authenticated HTTP actor. */
    protected static org.pangrova.spi.connector.ConnectorSession authenticatedCatalogSession(String catalog) {
        return new E2eCatalogSession(catalog);
    }

    /** Binds real persistent policy collaborators to the existing authenticated SQL pipeline. */
    protected static void configureFeatureSecurity(org.pangrova.spi.security.AccessControl accessControl) {
        E2eCurrentAuthority.removeDefault(stateStoreDataSource, E2E_USER_PRINCIPAL);
        featureAccessControl = java.util.Objects.requireNonNull(accessControl);
        rebuildQueryPipeline();
    }

    /** Removes the broad fixture role while retaining the live PostgreSQL RBAC authority. */
    protected static void restrictDefaultSqlAuthority() {
        E2eCurrentAuthority.removeDefault(stateStoreDataSource, E2E_USER_PRINCIPAL);
    }

    /** Restores the ordinary fixture role after a scoped-authority E2E scenario. */
    protected static void restoreDefaultSqlAuthority() {
        E2eCurrentAuthority.provisionDefault(stateStoreDataSource, E2E_USER_PRINCIPAL);
    }

    /** Releases a scenario's policy wiring; no alternative compilation path is retained. */
    protected static void clearFeatureSecurity() {
        featureAccessControl = null;
        rebuildQueryPipeline();
    }

    /** Rebuilds only a storage-profile scenario with the production JDBC authority. */
    protected static void configureDurableStorageProfiles(boolean enabled) {
        durableStorageProfileScenario = enabled;
        rebuildQueryPipeline();
    }

    private static void rebuildQueryPipeline() {
        // Dispatchers retain their task tracker: stop the old consumers before replacing it.
        stopArtifactControlLoops();
        if (queryManager != null) {
            queryManager.stopPeriodicCleanup();
        }
        QueryDispatcher dispatcher = buildQueryDispatcher();
        queryManager = new QueryManager(queryManagerContext(dispatcher));
        if (terminalCleanupTimer == null) {
            terminalCleanupTimer = org.pangrova.common.concurrent.ManagedExecutorFactory
                    .newSingleThreadScheduledExecutor("e2e-terminal-cleanup-timer");
            terminalCleanupWorker = org.pangrova.common.concurrent.ManagedExecutorFactory
                    .newBoundedThreadPool("e2e-terminal-cleanup", 2, 32);
        }
        queryManager.startPeriodicCleanup(terminalCleanupTimer, terminalCleanupWorker,
                Duration.ofMinutes(5), Duration.ofSeconds(1));
        if (e2eCoordinationService != null) {
            queryManager.setLocalCoordinatorId(E2E_COORDINATOR_ID);
        }
        TaskStatusReceiver receiver = new TaskStatusReceiver(
                new TaskStatusReceiverContext(queryManager, taskTracker, workerManager,
                        null, null, null,
                        queryManager.getStateJournal(),
                        metadataBootstrap.catalogRegistry(), writeCommitTracker,
                        dispatcher.dagEngine(), null, sharedWriteCommitJournal, artifactWorkReportSink()));
        receiver.setWriteCommitFencer(E2E_WRITE_COMMIT_FENCER);
        sharedTaskStatusReceiver = receiver;
        // Coordinator HTTP server already running — QueryManager is referenced by resources
        // via the existing server instance, so we need to update the reference.
        // Since QueryResource holds a direct reference to QueryManager,
        // we rebuild the HTTP server with the new QueryManager.
        rebuildCoordinatorHttp(receiver);
        if (e2eCoordinationService != null) {
            startArtifactControlLoops();
        }
    }

    private static QueryManagerContext queryManagerContext(QueryDispatcher dispatcher) {
        // Every SQL E2E request traverses the same authenticated tenant path;
        // enabling AI functions changes Native callbacks, not HTTP identity.
        var store = E2eCurrentAuthority.tenants(
                stateStoreDataSource, E2E_TENANT_ID, E2E_USER_PRINCIPAL, E2E_TENANT_QUOTA);
        return new QueryManagerContext(
                dispatcher,
                TRACING_PROVIDER,
                METRICS_PROVIDER,
                new org.pangrova.security.tenant.resolver.TenantResolver(store),
                new org.pangrova.security.tenant.quota.QuotaChecker(),
                null,
                null,
                null,
                null,
                null,
                new org.pangrova.coordinator.coordination.inmemory.InMemoryQueryStateJournal(),
                null,
                null,
                org.pangrova.coordinator.query.execution.AsyncSubmitConfig.defaultEnabled(),
                sharedAsyncQueryExecutor());
    }

    private static synchronized ExecutorService sharedAsyncQueryExecutor() {
        if (asyncQueryExecutor == null || asyncQueryExecutor.isShutdown()) {
            asyncQueryExecutor = org.pangrova.common.concurrent.ManagedExecutorFactory
                    .newBoundedThreadPool("e2e-async-query-submit", 4, 128);
        }
        return asyncQueryExecutor;
    }

    private static CoordinatorHttpServer createCoordinatorHttpServer(
            int port,
            TaskStatusReceiver receiver,
            HealthCheckEndpoint healthEndpoint) {
        return new CoordinatorHttpServer(
                port,
                new org.pangrova.coordinator.http.CoordinatorHttpServerContext(
                        receiver,
                        queryManager,
                        TRACING_HTTP_FILTER,
                        authenticationManager(),
                        healthEndpoint,
                        workerManager,
                        null,
                        null,
                        null,
                        null,
                        metadataBootstrap.metadataService(),
                        E2E_INTERNAL_AUTH_SECRET,
                        () -> requireE2ECoordination().getCurrentEpoch()));
    }

    private static synchronized org.pangrova.security.authn.AuthenticationManager
            authenticationManager() {
        if (e2eAuthenticationManager == null) {
            e2eAuthenticationManager = new org.pangrova.security.authn.AuthenticationManager(
                    List.of(new E2eBearerAuthenticator()));
        }
        return e2eAuthenticationManager;
    }

    private static final class E2eBearerAuthenticator
            implements org.pangrova.security.authn.Authenticator {

        @Override
        public String getName() {
            return "e2e-bearer";
        }

        @Override
        public void init(Map<String, String> config) {}

        @Override
        public org.pangrova.security.authn.AuthenticationResult authenticate(
                org.pangrova.security.authn.AuthenticationRequest request) {
            if (!E2E_USER_BEARER_TOKEN.equals(request.token())) {
                return org.pangrova.security.authn.AuthenticationResult.failure(
                        "invalid E2E bearer credential");
            }
            return org.pangrova.security.authn.AuthenticationResult.success(
                    new org.pangrova.spi.security.UserIdentity(
                            E2E_USER_PRINCIPAL,
                            java.util.Set.of("ai-e2e"),
                            Map.of(),
                            Instant.now()));
        }

        @Override
        public boolean supports(org.pangrova.security.authn.AuthenticationRequest request) {
            return request.token() != null;
        }

        @Override
        public boolean isHealthy() {
            return true;
        }

        @Override
        public void close() {}
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
            coordinatorHttpServer = createCoordinatorHttpServer(
                    previousPort, receiver, healthEndpoint);
            startServerOrFail(coordinatorHttpServer);
        } catch (RuntimeException e) {
            // Re-binding the previous port can race with the old server's socket
            // teardown (BindException: Address already in use). Fall back to a
            // random port — startServerOrFail re-syncs the status reporter, so
            // the shared worker follows the new port.
            LOG.warn("event=coordinator_http_rebind_failed port={} error={} — retrying on random port",
                    previousPort, e.getMessage());
            coordinatorHttpServer = createCoordinatorHttpServer(
                    0, receiver, healthEndpoint);
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

    /** Authenticated policy authority shared by the real and virtual routing harnesses. */
    protected static org.pangrova.spi.security.AccessControl e2eAccessControl() {
        return java.util.Objects.requireNonNull(currentAccessControl,
                "E2E access control must be initialized before routing");
    }

    protected static org.pangrova.spi.security.UserIdentity e2eUserIdentity() {
        return new org.pangrova.spi.security.UserIdentity(E2E_USER_PRINCIPAL,
                java.util.Set.of(), Map.of("tenant_id", E2E_TENANT_ID), Instant.now());
    }

    protected static String e2eTenantId() { return E2E_TENANT_ID; }

    protected static org.pangrova.protocol.v2.CapabilityInventory e2eNativeCapabilityInventory() {
        return java.util.Objects.requireNonNull(e2eCapabilityAdvertisement,
                "E2E Native advertisement must be initialized before Worker registration").inventory();
    }

    // -----------------------------------------------------------------------
    // Real pipeline execution
    // -----------------------------------------------------------------------

    /**
     * Executes SQL through the real HTTP pipeline:
     * POST /v1/query/submit → fetch results via transport → poll terminal state.
     */
    private static List<TypedRow> executeViaRealPipeline(String sql) {
        return executeWithBoundSession(sql, Map.of());
    }

    /**
     * Executes SQL through the HTTP pipeline with client-bound session defaults.
     *
     * <p>Transparent materialized-view rewrite is scoped to the session's default
     * catalog and schema, so tests that exercise rewrite must bind both the same
     * way a client does.
     */
    protected static List<TypedRow> executeWithBoundSession(String sql,
            Map<String, String> sessionProperties) {
        try {
            return consumeSubmittedQuery(submitQueryViaHttp(sql, sessionProperties));
        } catch (TimeoutException e) {
            throw new RuntimeException("Query polling timed out after " + POLL_TIMEOUT_MS + "ms", e);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Pipeline execution failed: " + e.getMessage(), e);
        }
    }

    /** Drains the submitted query's stream, validates its terminal state, and releases its task. */
    protected static List<TypedRow> consumeSubmittedQuery(SubmitQueryResponse submitResponse)
            throws Exception {
        String queryId = submitResponse.queryId();
        ResultEndpoint endpoint = awaitResultEndpoint(queryId, submitResponse.resultEndpoint());
        String canonicalTaskId = hasControlPlaneResult(queryId) ? null
                : requireNativeResultTaskId(endpoint, queryId, taskTracker);
        String workerTaskId = resolveWorkerTaskId(canonicalTaskId);
        List<TypedRow> rows;
        try {
            if ("FAILED".equals(submitResponse.state())
                    || "CANCELLED".equals(submitResponse.state())) {
                var terminal = MAPPER.readTree(pollUntilTerminal(queryId));
                throw new IllegalStateException("Query terminated before result fetch: "
                        + queryId + " state=" + terminal.path("state").asText()
                        + " error=" + extractErrorDetail(terminal));
            }
            // Bounded OUTPUT tasks finish after the client drains their stream.
            try {
                rows = fetchResultsViaTransport(endpoint, queryId);
            } catch (Exception transportFailure) {
                String url = "http://localhost:" + coordinatorHttpServer.getPort()
                        + "/v1/query/" + queryId + "/state";
                String queryFailure = null;
                try {
                    HttpResponse<String> response = HTTP_CLIENT.send(
                            authenticatedHttpRequest(URI.create(url)).GET().build(),
                            HttpResponse.BodyHandlers.ofString());
                    if (response.statusCode() == 200) {
                        var status = MAPPER.readTree(response.body());
                        if ("FAILED".equals(status.path("state").asText())) {
                            queryFailure = extractErrorDetail(status);
                        }
                    }
                } catch (Exception statusFailure) {
                    transportFailure.addSuppressed(statusFailure);
                }
                if (queryFailure != null) {
                    throw new IllegalStateException("Query failed: " + queryId
                            + " — " + queryFailure, transportFailure);
                }
                throw transportFailure;
            }
            var terminal = MAPPER.readTree(pollUntilTerminal(queryId));
            String state = terminal.path("state").asText();
            if (!"FINISHED".equals(state)) {
                throw new IllegalStateException("Query failed: " + queryId + " state="
                        + state + " — " + extractErrorDetail(terminal));
            }
        } catch (Exception failure) {
            cancelNativeTaskAfterFetch(canonicalTaskId, workerTaskId);
            throw failure;
        }
        releaseNativeTaskAfterTerminal(workerTaskId);
        return remapColumnNames(rows, submitResponse.columnNames());
    }

    protected static SubmitQueryResponse submitQueryViaHttp(String sql) throws Exception {
        return submitQueryViaHttp(sql, Map.of());
    }

    protected static SubmitQueryResponse submitQueryViaHttp(String sql,
            Map<String, String> sessionProperties) throws Exception {
        String url = "http://localhost:" + coordinatorHttpServer.getPort() + "/v1/query/submit";
        SubmitQueryRequest request = new SubmitQueryRequest(sql, sessionProperties);
        byte[] body = MAPPER.writeValueAsBytes(request);

        HttpRequest httpReq = authenticatedHttpRequest(URI.create(url))
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

    protected static ResultEndpoint awaitResultEndpoint(
            String queryId, ResultEndpoint submittedEndpoint) throws Exception {
        if (submittedEndpoint != null) {
            return submittedEndpoint;
        }
        String url = "http://localhost:" + coordinatorHttpServer.getPort()
                + "/v1/query/" + queryId + "/state";
        ResultEndpoint[] endpoint = new ResultEndpoint[1];
        org.awaitility.Awaitility.await("query-result-endpoint: " + queryId)
                .atMost(java.time.Duration.ofMillis(POLL_TIMEOUT_MS))
                .pollInterval(java.time.Duration.ofMillis(POLL_INTERVAL_MS))
                .pollDelay(java.time.Duration.ZERO)
                .until(() -> {
                    HttpRequest request = authenticatedHttpRequest(URI.create(url))
                            .GET().build();
                    HttpResponse<String> response = HTTP_CLIENT.send(
                            request, HttpResponse.BodyHandlers.ofString());
                    var status = MAPPER.readTree(response.body());
                    String state = status.path("state").asText();
                    if ("FAILED".equals(state) || "CANCELLED".equals(state)) {
                        throw new IllegalStateException(
                                "Query terminated before result endpoint: " + queryId
                                        + " state=" + state
                                        + " error=" + extractErrorDetail(status));
                    }
                    if (status.hasNonNull("result_endpoint")) {
                        endpoint[0] = MAPPER.treeToValue(
                                status.get("result_endpoint"), ResultEndpoint.class);
                        return true;
                    }
                    if ("FINISHED".equals(state)) {
                        throw new IllegalStateException(
                                "Finished query has no result endpoint: " + queryId);
                    }
                    return false;
                });
        return endpoint[0];
    }

    protected static String pollUntilTerminal(String queryId) throws Exception {
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
                        HttpRequest req = authenticatedHttpRequest(URI.create(url))
                                .GET()
                                .build();
                        HttpResponse<String> resp = HTTP_CLIENT.send(req, HttpResponse.BodyHandlers.ofString());
                        String body = resp.body();
                        String state = MAPPER.readTree(body).get("state").asText();
                        if ("FINISHED".equals(state) || "FAILED".equals(state)
                                || "CANCELLED".equals(state)) {
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

    /** Builds a user request using the same bearer identity as the SQL runner. */
    protected static HttpRequest.Builder authenticatedHttpRequest(URI uri) {
        return HttpRequest.newBuilder()
                .uri(uri)
                .header("Authorization", "Bearer " + E2E_USER_BEARER_TOKEN);
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

    protected static List<TypedRow> fetchResultsViaTransport(
            ResultEndpoint endpoint, String queryId) {
        if (endpoint == null) {
            throw new IllegalStateException(
                    "Missing result endpoint for finished query: queryId=" + queryId);
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
        // The terminal task may run on any physical Worker. Resolve the
        // actual task placement and require the returned endpoint to match
        // that Worker's registered Native result server.
        String nativeTaskId = requireNativeResultTaskId(endpoint, queryId, taskTracker);
        String workerId = taskTracker.getWorkerForTask(nativeTaskId);
        var worker = workerId == null ? null : workerManager.getWorker(workerId);
        if (worker == null || worker.resultServerPort() <= 0
                || !worker.host().equals(endpoint.host())
                || worker.resultServerPort() != endpoint.port()) {
            throw new IllegalStateException(
                    "Native result endpoint does not match task placement: queryId="
                            + queryId + " taskId=" + nativeTaskId);
        }
        return fetchResultsViaFlight(endpoint, queryId, taskTracker);
    }

    /** Only an actual published control-plane acknowledgement may bypass Native result transport. */
    protected static boolean hasControlPlaneResult(String queryId) {
        byte[] buffered = coordinatorResultBuffer == null ? null : coordinatorResultBuffer.get(queryId);
        return buffered != null && buffered.length > 0;
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
     * The opaque producer-issued ticket authorizes the exact Native result stream.
     * The separately retained canonical task ID is used only for control-plane cleanup.
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
            ResultEndpoint endpoint, String queryId, CoordinatorTaskTracker tracker) {
        String nativeTaskId = requireNativeResultTaskId(endpoint, queryId, tracker);
        if (endpoint.host() == null || endpoint.host().isBlank()
                || endpoint.port() <= 0) {
            throw new IllegalStateException(
                    "Native Flight result endpoint is not bound to task: queryId=" + queryId);
        }
        var location = org.apache.arrow.flight.Location.forGrpcInsecure(
                endpoint.host(), endpoint.port());

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
                         new org.apache.arrow.flight.Ticket(endpoint.ticket().getBytes(
                                 java.nio.charset.StandardCharsets.UTF_8)))) {
                List<TypedRow> rows = new java.util.ArrayList<>();
                while (awaitNextBatch(stream)) {
                    appendBatchRows(stream.getRoot(), rows);
                }
                LOG.info("event=flight_fetch_complete queryId={} taskId={} rows={} attempt={}",
                        queryId, nativeTaskId, rows.size(), attempt);
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
        throw new IllegalStateException(
                "Native Flight result fetch failed: queryId=" + queryId
                        + " taskId=" + nativeTaskId, lastException);
    }

    protected static void cancelNativeTaskAfterFetch(String coordinatorTaskId) {
        cancelNativeTaskAfterFetch(coordinatorTaskId, null);
    }

    protected static void cancelNativeTaskAfterFetch(
            String coordinatorTaskId, String knownWorkerTaskId) {
        if (workerTaskManagerRef == null || coordinatorTaskId == null) {
            return;
        }
        try {
            // TaskManager uses worker task IDs (queryId-NNN), so resolve first
            String workerTaskId = knownWorkerTaskId != null
                    ? knownWorkerTaskId
                    : workerTaskManagerRef.resolveWorkerTaskId(coordinatorTaskId);
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

    /** Releases the captured Worker task only after the canonical query terminal state. */
    protected static void releaseNativeTaskAfterTerminal(String workerTaskId) {
        if (workerTaskManagerRef == null || workerTaskId == null) {
            return;
        }
        workerTaskManagerRef.releaseTaskExecutor(workerTaskId);
    }

    /** Captures the Worker-local ID before terminal cleanup removes its canonical index. */
    protected static String resolveWorkerTaskId(String coordinatorTaskId) {
        return workerTaskManagerRef == null || coordinatorTaskId == null
                ? null : workerTaskManagerRef.resolveWorkerTaskId(coordinatorTaskId);
    }

    /** Actual Native split decisions, captured before scan scheduling. */
    protected static TaskManager.IndexSplitTotals nativeIndexSplitTotals() {
        return java.util.Objects.requireNonNull(workerTaskManagerRef,
                "Worker task manager has not started").getNativeIndexSplitTotals();
    }

    /** Waits for asynchronous Worker registration of an issued result task. */
    protected static String awaitWorkerTaskId(String coordinatorTaskId) {
        if (coordinatorTaskId == null || coordinatorTaskId.isBlank()) {
            throw new IllegalArgumentException("Result task ticket is absent");
        }
        String[] workerTaskId = new String[1];
        org.awaitility.Awaitility.await("worker-result-task: " + coordinatorTaskId)
                .atMost(java.time.Duration.ofMillis(POLL_TIMEOUT_MS))
                .pollInterval(java.time.Duration.ofMillis(POLL_INTERVAL_MS))
                .pollDelay(java.time.Duration.ZERO)
                .until(() -> {
                    workerTaskId[0] = resolveWorkerTaskId(coordinatorTaskId);
                    return workerTaskId[0] != null && !workerTaskId[0].isBlank();
                });
        return workerTaskId[0];
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
     * Resolves the sole direct-result task from the Coordinator's original issued contract.
     * The producer-issued endpoint ticket is opaque authority, never a task identity.
     * Ambiguous, absent, or foreign canonical bindings fail closed without unsigned fallback.
     */
    protected static String requireNativeResultTaskId(
            ResultEndpoint endpoint, String queryId, CoordinatorTaskTracker tracker) {
        if (endpoint == null || endpoint.ticket() == null || endpoint.ticket().isBlank()
                || tracker == null) {
            throw new IllegalStateException(
                    "Missing Native task ID for finished query: queryId=" + queryId);
        }
        String resultTaskId = null;
        for (String taskId : tracker.getTaskIds(queryId)) {
            SubmitTaskRequest issued = tracker.getTaskRequest(taskId);
            if (issued == null || !issued.taskRequest().getExecutionOptions().getDirectResultOutput()) {
                continue;
            }
            var attempt = issued.taskRequest().getAttempt();
            if (!queryId.equals(attempt.getQueryOrJobId()) || !taskId.equals(attempt.getTaskId())) {
                throw new IllegalStateException("Foreign Native result task identity: queryId=" + queryId);
            }
            if (resultTaskId != null) {
                throw new IllegalStateException("Ambiguous Native result task identity: queryId=" + queryId);
            }
            resultTaskId = taskId;
        }
        if (resultTaskId == null) {
            throw new IllegalStateException(
                    "Missing Native task ID for finished query: queryId=" + queryId);
        }
        return resultTaskId;
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
            throw new IllegalStateException(
                    "Failed to decode result bytes: length=" + resultBytes.length, e);
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
        if (org.pangrova.transport.ArrowBatchDecoder.isVectorField(vector.getField())) {
            return org.pangrova.transport.ArrowBatchDecoder.decodeVectorField(vector, row);
        }
        Object value = vector.getObject(row);
        // Arrow Text → Java String.
        // Velox pads fixed-length CHAR literals with trailing spaces
        // (e.g. CASE WHEN string constants). Strip at the transport boundary
        // so individual tests don't need per-assertion trimming.
        if (value instanceof org.apache.arrow.vector.util.Text text) {
            return stripTrailing(text.toString());
        }
        // Preserve the SQL DATE type when adapting Arrow epoch days.
        if (vector instanceof org.apache.arrow.vector.DateDayVector) {
            return java.time.LocalDate.ofEpochDay(((Number) value).longValue());
        }
        // Preserve Arrow's decoded timestamp as a typed SQL value.
        if (vector instanceof org.apache.arrow.vector.TimeStampVector) {
            if (!(value instanceof java.time.LocalDateTime)) {
                throw new IllegalStateException("Arrow timestamp has unexpected Java type: "
                        + value.getClass().getName());
            }
            return value;
        }
        return org.pangrova.transport.ArrowBatchDecoder.normalizeComplexValue(
                value, vector.getField());
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
            closeE2ECoordination();
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
        writeCommitTracker = new org.pangrova.coordinator.query.task.write.NativeWriteCommitTracker();
        QueryDispatcher dispatcher = buildQueryDispatcher();
        System.err.println("[E2E-DEBUG] initCoordinatorServices: dispatcher built");
        queryManager = new QueryManager(queryManagerContext(dispatcher));
        System.err.println("[E2E-DEBUG] initCoordinatorServices: queryManager created");
        new CoordinatorMetrics(METRICS_PROVIDER, () -> (double) workerManager.getActiveWorkers().size());
        System.err.println("[E2E-DEBUG] initCoordinatorServices: metrics created");

        TaskStatusReceiver receiver = new TaskStatusReceiver(
                new TaskStatusReceiverContext(queryManager, taskTracker, workerManager,
                        null, null, null,
                        queryManager.getStateJournal(),
                        metadataBootstrap.catalogRegistry(), writeCommitTracker, null, null, sharedWriteCommitJournal,
                        artifactWorkReportSink()));
        System.err.println("[E2E-DEBUG] initCoordinatorServices: receiver created");
        HealthCheckEndpoint healthEndpoint = createCoordinatorHealthEndpoint();
        coordinatorHttpServer = createCoordinatorHttpServer(
                0, receiver, healthEndpoint);
        System.err.println("[E2E-DEBUG] initCoordinatorServices: http server created");
        startServerOrFail(coordinatorHttpServer);
        sharedTaskStatusReceiver = receiver;
        registerNativeWriter();
        coordinatorHttpReady.set(true);
        LOG.info("Coordinator COORDINATORS: metadata + queryManager + HTTP started on port {}",
                coordinatorHttpServer.getPort());
    }

    private static void initCoordinatorReadiness() {
        var coordination = new org.pangrova.coordinator.coordination.HighAvailabilityCoordinationService(
                new org.pangrova.metadata.coordination.JdbcDistributedLease(
                        stateStoreDataSource));
        coordination.setExecutionAnchorStore(
                new org.pangrova.metadata.coordination.JdbcExecutionAnchorStore(
                        stateStoreDataSource));
        coordination.start(E2E_COORDINATOR_ID);
        if (!coordination.isLeader(E2E_COORDINATOR_ID)
                || coordination.getCurrentEpoch() <= 0) {
            coordination.close();
            throw new IllegalStateException("E2E Coordinator did not acquire its durable control lease");
        }
        e2eCoordinationService = coordination;
        registerDurableMaterializationTasks();
        java.util.Objects.requireNonNull(sharedTaskStatusReceiver, "E2E task receiver is not initialized")
                .setWriteCommitFencer(E2E_WRITE_COMMIT_FENCER);
        sharedQueryDispatcher.setOwnershipTracking(coordination, E2E_COORDINATOR_ID);
        queryManager.setLocalCoordinatorId(E2E_COORDINATOR_ID);
        startArtifactControlLoops();
        LOG.info("Coordinator READINESS: durable lease and execution anchors ready epoch={}",
                coordination.getCurrentEpoch());
    }

    private static void closeE2ECoordination() {
        closeQuietly(sharedIndexingBootstrap, "indexing_lifecycle");
        sharedIndexingBootstrap = null;
        stopArtifactControlLoops();
        var coordination = e2eCoordinationService;
        e2eCoordinationService = null;
        if (coordination != null) {
            coordination.close();
        }
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
        sharedWorkerTaskExecutor = taskExecutor;
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
                workerTransportPort, healthEndpoint, 0);
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
        // The in-process Worker does not run WorkerHeartbeatBootstrap. Model its
        // authenticated control heartbeat periodically so a durable lease
        // reacquisition advances the Worker's independently observed epoch.
        // Dispatch itself never advances the epoch on HTTP 425.
        controlEpochObserver = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(
                r -> {
                    Thread thread = new Thread(r, "e2e-worker-control-heartbeat");
                    thread.setDaemon(true);
                    return thread;
                });
        controlEpochObserver.scheduleWithFixedDelay(() -> {
            try {
                var coordination = e2eCoordinationService;
                var manager = workerTaskManagerRef;
                if (coordination != null && manager != null
                        && coordination.isLeader(E2E_COORDINATOR_ID)) {
                    long epoch = coordination.getCurrentEpoch();
                    if (epoch > 0) {
                        manager.observeControlEpoch(epoch);
                    }
                }
            } catch (RuntimeException error) {
                LOG.warn("event=e2e_control_heartbeat_failed error={}", error.toString());
            }
        }, 0, 1, java.util.concurrent.TimeUnit.SECONDS);
        LOG.info("Worker READINESS: cluster registration complete");
    }

    private static TaskExecutor initNativeExecutor() {
        // nativeInit already called in bootstrapTestEnvironment — skip duplicate init
        var executor = new NativeTaskExecutor();
        executor.setCatalogRuntime(java.util.Objects.requireNonNull(sharedWorkerCatalogRuntime));
        return executor;
    }

    private static TaskManager createTaskManager(
            TaskExecutor executor, ResourceManager resourceManager) {
        int coordPort = coordinatorHttpServer != null ? coordinatorHttpServer.getPort() : 0;
        statusReporter = new RemoteTaskStatusReporter(
                "http://localhost:" + coordPort,
                TRACING_PROVIDER,
                E2E_INTERNAL_AUTH_SECRET);
        statusReportDelivery = statusReporter::report;
        sharedStatusReporter = statusReporter;
        TransportRefs transportDeps = createTestTransportRefs(executor);
        TaskManagerContext deps = new TaskManagerContext(
                report -> statusReportDelivery.accept(report), executor,
                new MemoryRefs(resourceManager, sharedWorkerNodePool, null),
                transportDeps,
                java.util.concurrent.Executors.newCachedThreadPool(r -> {
                    Thread t = new Thread(r, "test-completion-monitor");
                    t.setDaemon(true);
                    return t;
                }));
        workerAdmissionIncarnation = org.pangrova.common.lifecycle.ProcessIncarnationStore.open(
                java.nio.file.Path.of(org.pangrova.testing.nativefixture.TestShuffleServiceConfig.admissionDirectory()));
        TaskManager manager = new TaskManager(deps, WORKER_ID, 0.0, workerAdmissionIncarnation);
        manager.requireControlEpochHandshake();
        manager.observeControlEpoch(requireE2ECoordination().getCurrentEpoch());
        return manager;
    }

    static TransportRefs createTestTransportRefs(TaskExecutor executor) {
        org.pangrova.worker.result.NativeResultTransport resultTransport = null;
        try {
            var shuffleTransport = new org.pangrova.worker.shuffle.NativeShuffleTransport(
                    executor, IN_PROCESS_WORKER_HOST, "");
            resultTransport = new org.pangrova.worker.result.NativeResultTransport(
                    executor, IN_PROCESS_WORKER_HOST);
            resultTransport.startServer(0);
            sharedResultServerPort = resultTransport.boundPort();
            var exchangeTransport = new org.pangrova.worker.exchange.NativeExchangeTransport(executor);
            LOG.info("event=e2e_transport_initialized resultPort={}", sharedResultServerPort);
            return new TransportRefs(shuffleTransport, resultTransport, exchangeTransport);
        } catch (Exception e) {
            sharedResultServerPort = 0;
            if (resultTransport != null) {
                try {
                    resultTransport.close();
                } catch (Exception cleanupFailure) {
                    e.addSuppressed(cleanupFailure);
                }
            }
            throw new IllegalStateException("Failed to initialize E2E Native transports", e);
        }
    }

    private static ResourceManager createResourceManager() {
        MemoryArbitrator arbitrator = java.util.Objects.requireNonNull(
                sharedWorkerMemoryArbitrator, "E2E Worker arbitrator must be initialized before task admission");
        SpillManager spillManager = new SpillManager();
        ThreadBudgetTracker threadPoolManager = new ThreadBudgetTracker(8, 4, 2);
        return new ResourceManager(arbitrator, threadPoolManager, spillManager);
    }

    /**
     * Starts a coordinator-side transport server for serving constant-fold and DDL/DML results.
     * Worker-side result serving is handled by the C++ FlightResultServer via NativeResultTransport.
     */
    private static TransportServer startCoordinatorTransportServer() {
        var coordinatorProducer = new org.pangrova.coordinator.result.CoordinatorDataProducer(
                ensureCoordinatorResultBuffer());
        TransportServer server = new TransportServer(
                coordinatorProducer, new org.pangrova.spi.transport.TransportServerConfig(0));
        server.start();
        return server;
    }

    private static WorkerHttpServer startWorkerHttpServer(
            TaskManager taskManager, TaskExecutor taskExecutor,
            int transportPort, HealthCheckEndpoint healthEndpoint, int httpPort) {
        var exchangeTransport = new NativeExchangeTransport(taskExecutor);
        var httpConfig = new WorkerHttpServer.WorkerHttpConfig(
                httpPort, IN_PROCESS_WORKER_HOST, transportPort,
                E2E_INTERNAL_AUTH_SECRET, "grpc");
        WorkerHttpServer server = new WorkerHttpServer(
                httpConfig, taskManager, exchangeTransport, healthEndpoint,
                new org.pangrova.worker.dataset.NativeDatasetControlService(taskManager,
                        java.util.Objects.requireNonNull(sharedWorkerCatalogRuntime, "Worker catalog runtime")),
                java.util.Objects.requireNonNull(sharedResultReadTickets, "Worker result signing authority"));
        startServerOrFail(server);
        sharedWorkerHttpPort = server.getPort();
        return server;
    }

    // -----------------------------------------------------------------------
    // Wiring helpers (each ≤ 20 lines, single responsibility)
    // -----------------------------------------------------------------------

    private static QueryDispatcher buildQueryDispatcher() {
        // Match production DML routing: compile the parsed statement once and
        // preserve its write mode, partition binding, and execution ownership.
        var queryDispatcherRef = new java.util.concurrent.atomic.AtomicReference<QueryDispatcher>();
        org.pangrova.coordinator.query.execution.dispatch.NativeDmlCompileFunction dmlDispatch =
                (sm, session, stmt) -> {
                    QueryDispatcher qd = queryDispatcherRef.get();
                    if (qd == null) {
                        throw new IllegalStateException("QueryDispatcher not initialized");
                    }
                    return qd.executeDmlStatement(sm, stmt, session);
                };
        // Build CoordinatorResultService BEFORE StatementDispatcher so DDL/metadata
        // results (SHOW INDEXES, DESCRIBE, etc.) are buffered and returned to clients.
        CoordinatorResultService resultSvc = buildCoordinatorResultService();
        if (workerManager == null) {
            workerManager = new WorkerManager();
        }
        E2eCurrentAuthority.tenants(stateStoreDataSource, E2E_TENANT_ID, E2E_USER_PRINCIPAL, E2E_TENANT_QUOTA);
        E2eCurrentAuthority.provisionModels(stateStoreDataSource, E2E_USER_PRINCIPAL);
        currentAccessControl = featureAccessControl == null
                ? E2eCurrentAuthority.provisionDefault(stateStoreDataSource, E2E_USER_PRINCIPAL) : featureAccessControl;
        var embeddingRegistry = new org.pangrova.coordinator.ai.embedding.JdbcEmbeddingModelRegistry(
                java.util.Objects.requireNonNull(
                        stateStoreDataSource, "E2E PostgreSQL state store must be initialized"),
                E2eCurrentAuthority.MODEL_ADMIN_ROLE, E2eCurrentAuthority.MODEL_USAGE_ROLE);
        embeddingRegistry.initSchema();
        TaskScheduler scheduler = new TaskScheduler(new WorkerPerformanceScorer());
        sharedGeneratedValueCoordinator = E2eCurrentAuthority.generatedValues(new E2eCurrentAuthority.GeneratedServices(
                stateStoreDataSource, metadataBootstrap.catalogRegistry(), sharedArtifactBuildStore,
                sharedWriteCommitJournal, workerManager, scheduler, embeddingRegistry,
                tempDir.resolve("generated-artifacts").toUri().toString()));
        var generatedCoordinator = sharedGeneratedValueCoordinator;
        generatedCoordinator.bindSourceCompiler((sql, actor) -> {
            QueryDispatcher dispatcher = queryDispatcherRef.get();
            if (dispatcher == null) {
                throw new IllegalStateException("QueryDispatcher not initialized for generated input compilation");
            }
            return dispatcher.compileAuthorizedRelation(sql, actor);
        });
        sharedWriteAuthoritySupplier = generatedCoordinator::writeAuthority;
        var statementDispatcher = buildStatementDispatcher(
                dmlDispatch, queryDispatcherRef, taskTracker, resultSvc,
                embeddingRegistry, workerManager);
        sharedStatementDispatcher = statementDispatcher;
        registerDurableMaterializationTasks();
        var indexAwareSplitFetcher = registerIndexHandlers(statementDispatcher);
        SqlCompiler compiler = buildSqlCompiler();
        statementDispatcher.updateCompiler(compiler);
        closeQuietly(sharedTaskDispatcher, "task_dispatcher");
        RemoteTaskDispatcher httpDispatcher = createE2ETaskDispatcher();
        sharedTaskDispatcher = httpDispatcher;
        var security = new org.pangrova.coordinator.query.execution.SecurityContextBundle(
                new org.pangrova.coordinator.query.security.AccessControlEnforcer(currentAccessControl),
                new org.pangrova.planner.security.ColumnMaskApplier(currentAccessControl),
                new org.pangrova.planner.security.RowFilterApplier(currentAccessControl));
        var scheduling = new org.pangrova.coordinator.query.execution.SchedulingContextBundle(
                httpDispatcher, scheduler, workerManager, taskTracker, null, null,
                new org.pangrova.coordinator.query.execution.dispatch.dag.SpillBudget(
                        NATIVE_WRITE_STAGE_BYTES, ""), null);
        var deps = new QueryDispatcherContext(
                compiler, security, scheduling,
                metadataBootstrap.catalogRegistry(), null, 0L,
                null, null, null, null, null, null, null, indexAwareSplitFetcher, embeddingRegistry);
        var queryDispatcher = new QueryDispatcher(
                deps, statementDispatcher);
        queryDispatcher.setWriteCommitTracker(writeCommitTracker);
        queryDispatcher.setGeneratedWriteAuthoritySupplier(sharedWriteAuthoritySupplier);
        sharedQueryDispatcher = queryDispatcher;
        wireE2EOwnership(queryDispatcher);
        queryDispatcherRef.set(queryDispatcher);
        return queryDispatcher;
    }

    /** Uses the same fenced control identity contract as production HA wiring. */
    protected static void wireE2EOwnership(QueryDispatcher dispatcher) {
        if (e2eCoordinationService != null) {
            dispatcher.setOwnershipTracking(e2eCoordinationService, E2E_COORDINATOR_ID);
        }
    }

    protected static RemoteTaskDispatcher createE2ETaskDispatcher() {
        RemoteTaskDispatcher dispatcher = new RemoteTaskDispatcher(
                CircuitBreakerPolicy.noOp(),
                TRACING_PROVIDER,
                E2E_INTERNAL_AUTH_SECRET);
        dispatcher.setControlEpochSupplier(
                () -> requireE2ECoordination().getCurrentEpoch());
        dispatcher.setExecutionOwnerResolver(executionId -> {
            var anchor = requireE2ECoordination().requireExecutionAnchor(executionId);
            return new org.pangrova.protocol.rest.task.ExecutionOwnerIdentity(
                    anchor.ownerId(), anchor.ownerIncarnation(), anchor.fencingToken());
        });
        return dispatcher;
    }

    private static org.pangrova.coordinator.coordination.HighAvailabilityCoordinationService
            requireE2ECoordination() {
        var coordination = e2eCoordinationService;
        if (coordination == null) {
            throw new IllegalStateException("E2E durable coordination is not initialized");
        }
        return coordination;
    }

    private static org.pangrova.coordinator.query.task.report.ArtifactWorkReportSink
            artifactWorkReportSink() {
        return new org.pangrova.coordinator.query.task.report.DurableArtifactWorkReportSink(
                java.util.Objects.requireNonNull(sharedArtifactBuildStore,
                        "E2E durable artifact store must be initialized"),
                new org.pangrova.metadata.artifact.jdbc.build.JdbcArtifactStageExecutionStore(stateStoreDataSource));
    }

    private static void stopArtifactControlLoops() {
        if (artifactDispatchExecutor != null) {
            artifactDispatchExecutor.shutdownNow();
            artifactDispatchExecutor = null;
        }
        closeQuietly(sharedArtifactBuildDispatcher, "artifact_build_dispatcher");
        sharedArtifactBuildDispatcher = null;
        closeQuietly(artifactTaskDispatcher, "artifact_task_dispatcher");
        artifactTaskDispatcher = null;
        closeQuietly(sharedArtifactPublisher, "artifact_publisher");
        sharedArtifactPublisher = null;
    }

    private static void startArtifactControlLoops() {
        if (sharedArtifactBuildDispatcher == null) {
            artifactTaskDispatcher = createE2ETaskDispatcher();
            sharedArtifactBuildDispatcher = new org.pangrova.coordinator.artifact
                    .DurableArtifactBuildDispatcher(
                            new org.pangrova.coordinator.artifact.DurableArtifactBuildDispatcher.Dependencies(
                                    sharedArtifactBuildStore,
                                    new TaskScheduler(new WorkerPerformanceScorer()),
                                    workerManager, artifactTaskDispatcher, taskTracker,
                                    E2E_COORDINATOR_ID,
                                    () -> requireE2ECoordination().getCurrentEpoch(),
                                    new org.pangrova.coordinator.artifact.ArtifactExecutionOwnership(
                                            requireE2ECoordination(), E2E_COORDINATOR_ID),
                                    new org.pangrova.metadata.artifact.jdbc.build.JdbcArtifactStageExecutionStore(
                                            stateStoreDataSource),
                                    new org.pangrova.metadata.artifact.jdbc.JdbcArtifactStageExchangeStore(stateStoreDataSource),
                                    java.util.Objects.requireNonNull(sharedQueryDispatcher,
                                            "shared QueryDispatcher must be initialized").dagEngine()),
                            new org.pangrova.coordinator.artifact.DurableArtifactBuildDispatcher.Settings(
                                    Duration.ofMinutes(2), Duration.ofSeconds(5), 32));
        }
        if (sharedArtifactPublisher == null) {
            sharedArtifactPublisher = new org.pangrova.coordinator.artifact.DurableArtifactPublisher(
                    sharedArtifactBuildStore, E2eArtifactPublications.stores(stateStoreDataSource),
                    new org.pangrova.coordinator.artifact.ArtifactExecutionOwnership(
                            requireE2ECoordination(), E2E_COORDINATOR_ID));
            sharedArtifactPublisher.start();
        }
        if (artifactDispatchExecutor == null) {
            artifactDispatchExecutor = org.pangrova.common.concurrent.ManagedExecutorFactory
                    .newSingleThreadScheduledExecutor("e2e-artifact-dispatch");
            artifactDispatchExecutor.scheduleWithFixedDelay(() -> {
                try {
                    sharedArtifactBuildDispatcher.pollOnce();
                } catch (RuntimeException failure) {
                    LOG.error("event=e2e_artifact_dispatch_failed error={}",
                            failure.getMessage(), failure);
                }
            }, 0, 1, java.util.concurrent.TimeUnit.SECONDS);
        }
    }

    protected static org.pangrova.coordinator.coordination.HighAvailabilityCoordinationService
            e2eCoordinationForTest() {
        return requireE2ECoordination();
    }

    /**
     * Prepares index DDL infrastructure and query-time index pruning for E2E tests.
     * Uses the same durable Native artifact build service as production,
     * backed by the E2E PostgreSQL metadata store.
     * Returns an IndexAwareSplitFetcher for query-time split pruning.
     */
    private static org.pangrova.coordinator.index.IndexAwareSplitFetcher registerIndexHandlers(
            org.pangrova.coordinator.query.execution.dispatch.StatementDispatcher statementDispatcher) {
        try {
            var indexMetadataStore = new org.pangrova.metadata.index.jdbc.JdbcIndexMetadataStore(stateStoreDataSource);
            closeQuietly(sharedIndexingBootstrap, "indexing_lifecycle_rebuild");
            sharedIndexingBootstrap = new org.pangrova.coordinator.bootstrap.IndexingBootstrap(
                    tempDir.resolve("index-artifacts").toString(),
                    new org.pangrova.metadata.coordination.JdbcDistributedLease(stateStoreDataSource),
                    sharedArtifactBuildStore, E2eArtifactPublications.router(stateStoreDataSource));
            sharedIndexingBootstrap.start(indexMetadataStore, metadataBootstrap.catalogRegistry(),
                    METRICS_PROVIDER, currentAccessControl);
            sharedGeneratedValueCoordinator.activateCatalogs(
                    sharedIndexingBootstrap.registerDurableCommitProcessor(sharedGeneratedValueCoordinator));
            sharedIndexingBootstrap.onLeaderElected();
            var indexExecutor = sharedIndexingBootstrap.getDdlExecutor();
            var accessorResolver = sharedIndexingBootstrap.getAccessorResolver();

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

            var indexAwareSplitFetcher = new org.pangrova.coordinator.index.IndexAwareSplitFetcher(
                    metadataBootstrap.catalogRegistry(), sharedIndexingBootstrap.getSplitFilter(), indexMetadataStore,
                    accessorResolver, sharedArtifactBuildStore);
            indexAwareSplitFetcher.bindGeneratedValueReads(
                    new org.pangrova.coordinator.artifact.GeneratedValueReadResolver(
                            metadataBootstrap.catalogRegistry(), sharedArtifactBuildStore,
                            new org.pangrova.metadata.artifact.jdbc.JdbcGeneratedValueArtifactPublicationStore(
                                    stateStoreDataSource)));

            LOG.info("event=index_infrastructure_initialized_in_e2e executor_type={} pruning=enabled",
                    indexExecutor.getClass().getSimpleName());
            return indexAwareSplitFetcher;
        } catch (Exception e) {
            throw new IllegalStateException("E2E index infrastructure failed to initialize", e);
        }
    }

    private static CoordinatorResultService buildCoordinatorResultService() {
        if (coordinatorTransportServer == null || !coordinatorTransportServer.isRunning()) {
            return null;
        }
        CoordinatorResultBuffer buffer = ensureCoordinatorResultBuffer();
        closeQuietly(coordinatorArrowAllocator, "arrow_allocator");
        coordinatorArrowAllocator = new org.apache.arrow.memory.RootAllocator(128L * 1024 * 1024);
        var writer = new CoordinatorResultWriter(coordinatorArrowAllocator);
        var endpoint = new ResultEndpoint("localhost", coordinatorTransportServer.getPort(), "coordinator", "grpc");
        coordinatorResultService = new CoordinatorResultService(buffer, writer, endpoint);
        return coordinatorResultService;
    }

    private static synchronized CoordinatorResultBuffer ensureCoordinatorResultBuffer() {
        if (coordinatorResultBuffer != null) {
            return coordinatorResultBuffer;
        }
        if (resultBufferExecutor == null) {
            resultBufferExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "result-buffer-expiry");
                t.setDaemon(true);
                return t;
            });
        }
        coordinatorResultBuffer = new CoordinatorResultBuffer(resultBufferExecutor, 300_000L);
        return coordinatorResultBuffer;
    }

    protected static org.pangrova.coordinator.query.execution.dispatch.StatementDispatcher buildStatementDispatcher(
            org.pangrova.coordinator.query.execution.dispatch.NativeDmlCompileFunction dmlDispatch,
            java.util.concurrent.atomic.AtomicReference<QueryDispatcher> queryDispatcherRef,
            org.pangrova.coordinator.task.CoordinatorTaskTracker tt,
            org.pangrova.coordinator.result.CoordinatorResultService resultSvc,
            org.pangrova.coordinator.ai.embedding.EmbeddingModelRegistry embeddingRegistry,
            WorkerManager embeddingWorkers) {
        var catalog = metadataBootstrap.catalogRegistry();
        var ddlExecutor = new org.pangrova.coordinator.query.statement.DdlExecutor(catalog, null,
                org.pangrova.coordinator.metadata.read.MetadataSchemaBuilder::invalidateTableCaches);
        sharedDdlExecutor = ddlExecutor;
        var showDescribeExecutor = new org.pangrova.coordinator.query.statement.ShowDescribeExecutor(catalog);
        var maintenanceExecutor = new org.pangrova.coordinator.query.statement.MaintenanceExecutor(null);

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
        // Incremental read (CHANGES BETWEEN SNAPSHOT x AND y)
        tasks.put(org.pangrova.parser.ast.statement.IncrementalReadStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.IncrementalReadTask(catalog));
        // Storage Profile management
        org.pangrova.spi.storage.StorageProfileRegistry spRegistry = durableStorageProfileScenario
                ? new org.pangrova.metadata.storage.JdbcStorageProfileRegistry(
                        java.util.Objects.requireNonNull(stateStoreDataSource))
                : new org.pangrova.coordinator.storage.InMemoryStorageProfileRegistry();
        tasks.put(org.pangrova.parser.ast.statement.CreateStorageProfileStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.storage.CreateStorageProfileTask(spRegistry));
        tasks.put(org.pangrova.parser.ast.statement.DropStorageProfileStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.storage.DropStorageProfileTask(spRegistry));
        tasks.put(org.pangrova.parser.ast.statement.ShowStorageProfilesStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.storage.ShowStorageProfilesTask(
                        spRegistry, currentAccessControl));
        // Branch/tag mutations use the same metadata tasks as the production bootstrap.
        var branchDeps = new org.pangrova.coordinator.ai.branch.BranchDdlContext(
                metadataBootstrap.catalogRegistry(), null);
        var branchExecutor = new org.pangrova.coordinator.ai.branch.BranchDdlExecutor(branchDeps);
        tasks.put(org.pangrova.parser.ast.statement.CreateBranchStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.branch.CreateBranchTask(branchExecutor));
        tasks.put(org.pangrova.parser.ast.statement.DropBranchStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.branch.DropBranchTask(branchExecutor));
        tasks.put(org.pangrova.parser.ast.statement.CreateTagStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.branch.CreateTagTask(branchExecutor));
        tasks.put(org.pangrova.parser.ast.statement.DropTagStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.branch.DropTagTask(branchExecutor));
        var metadataFactory = new org.pangrova.coordinator.query.execution.dispatch.MetadataExecutionFactory(tasks);
        if (featureAccessControl != null) {
            var exports = new org.pangrova.coordinator.feature.DurableFeatureExportService(
                    sharedArtifactBuildStore,
                    new org.pangrova.coordinator.artifact.SnapshotArtifactSplitPlanner(catalog),
                    tempDir.resolve("feature-exports").toString(), catalog);
            var features = org.pangrova.metadata.ai.AiInfrastructureStores.jdbc(stateStoreDataSource).features();
            var service = new org.pangrova.coordinator.feature.FeatureViewStatementService(
                    features, exports, featureAccessControl,
                    (sql, actor) -> java.util.Objects.requireNonNull(queryDispatcherRef.get(),
                            "QueryDispatcher must be initialized").compileAuthorizedRelation(sql, actor));
            org.pangrova.coordinator.feature.FeatureViewMetadataTasks.register(metadataFactory, service);
        }
        if (embeddingRegistry != null) {
            org.pangrova.coordinator.ai.embedding.EmbeddingModelTaskRegistration.register(
                    metadataFactory, embeddingRegistry, embeddingWorkers);
        }
        var planFactory = new org.pangrova.coordinator.query.execution.dispatch.PlanExecutionFactory(
                new org.pangrova.coordinator.query.execution.dispatch.PlanExecutionFactoryContext(
                        dmlDispatch, null,
                        (sql, sess) -> sql,
                        metadataBootstrap.catalogRegistry(),
                        sharedGeneratedValueCoordinator));

        var factories = new java.util.ArrayList<
                org.pangrova.coordinator.query.execution.dispatch.StatementExecutionFactory<?>>();
        factories.add(metadataFactory);
        factories.add(planFactory);

        var context = new org.pangrova.coordinator.query.execution.dispatch.StatementDispatcherContext(
                null, resultSvc, List.copyOf(factories), List.of(), currentAccessControl);
        return new org.pangrova.coordinator.query.execution.dispatch.StatementDispatcher(context);
    }

    /**
     * Registers the production materialization DDL handlers on the active dispatcher.
     * Idempotent per dispatcher instance; the durable authority is the only DT/MV runtime.
     */
    private static synchronized void registerDurableMaterializationTasks() {
        var dispatcher = sharedStatementDispatcher;
        if (dispatcher == null || e2eCoordinationService == null
                || materializationDispatcher == dispatcher) {
            return;
        }
        var repository = materializationRepository();
        var catalog = materializationCatalog();
        var factory = new org.pangrova.coordinator.query.execution.dispatch.MetadataExecutionFactory(
                new java.util.HashMap<>());
        org.pangrova.coordinator.materialization.MaterializationDdlRegistration.register(
                factory, repository, catalog,
                metadataBootstrap.catalogRegistry(), e2eCoordinationService,
                java.util.Objects.requireNonNull(sharedDdlExecutor),
                (sm, session, stmt) -> java.util.Objects.requireNonNull(sharedQueryDispatcher)
                        .executeDmlStatement(sm, stmt, session),
                stateStoreDataSource, currentAccessControl);
        dispatcher.registerFactory(factory);
        materializationDispatcher = dispatcher;
    }

    /** Returns the durable authority used by the E2E materialization runtime. */
    protected static DurableDynamicTableAuthority installDurableDynamicTableRuntime() {
        registerDurableMaterializationTasks();
        return new DurableDynamicTableAuthority(materializationRepository(), materializationCatalog());
    }

    private static org.pangrova.metadata.materialization.JdbcMaterializationRepository
            materializationRepository() {
        var repository = materializationRepository;
        if (repository == null) {
            repository = new org.pangrova.metadata.materialization.JdbcMaterializationRepository(
                    java.util.Objects.requireNonNull(stateStoreDataSource));
            materializationRepository = repository;
        }
        return repository;
    }

    private static org.pangrova.metadata.materialization.JdbcMaterializationCatalog
            materializationCatalog() {
        var catalog = materializationCatalog;
        if (catalog == null) {
            catalog = new org.pangrova.metadata.materialization.JdbcMaterializationCatalog(
                    java.util.Objects.requireNonNull(stateStoreDataSource));
            materializationCatalog = catalog;
        }
        return catalog;
    }

    /** Reopens the persisted materialization authority as a coordinator restart would. */
    protected static org.pangrova.metadata.materialization.JdbcMaterializationCatalog
            reopenDynamicTablePublicationCatalog() {
        return new org.pangrova.metadata.materialization.JdbcMaterializationCatalog(
                java.util.Objects.requireNonNull(stateStoreDataSource));
    }

    /** Durable authority pair wired into the E2E Dynamic Table runtime. */
    protected record DurableDynamicTableAuthority(
            org.pangrova.metadata.materialization.JdbcMaterializationRepository repository,
            org.pangrova.metadata.materialization.JdbcMaterializationCatalog catalog) {
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

    protected static SqlCompiler buildSqlCompiler() {
        List<String> schemaPath = List.copyOf(defaultSchemaPath);
        PangrovaPlanner template = new PangrovaPlanner(
                CalciteSchema.createRootSchema(false, false), schemaPath);
        return new SqlCompiler(template, new SubstraitConverter(), new SubstraitPlanSerializer())
                .withNativeCapabilities(() ->
                        org.pangrova.planner.validation.NativeCapabilityRegistry.fromInventories(
                                workerManager.getActiveWorkers().stream().map(
                                        org.pangrova.coordinator.worker.WorkerInfo::capabilityInventory).toList()))
                .withSessionPlanner(session -> {
                    var bindings = new org.pangrova.coordinator.metadata.read.PlanningReadBindings(
                            metadataBootstrap.catalogRegistry(), session);
                    var durableRewriter = new org.pangrova.planner.mv.DynamicTableRewriter(
                            new org.pangrova.coordinator.dynamictable.DurableDynamicTableResolver(
                                    materializationCatalog(),
                                    session.getProperties().getOrDefault("tenant_id", E2E_TENANT_ID),
                                    session.getCatalog().orElse(null),
                                    session.getSchema().orElse(null)));
                    var materializationCatalog = materializationCatalog();
                    var mvCatalog = materializationCatalog == null ? null
                            : org.pangrova.coordinator.materialization
                                    .KernelMaterializedViewRewriteCatalog.create(
                                            materializationCatalog,
                                            session.getProperties().getOrDefault(
                                                    "tenant_id", E2E_TENANT_ID),
                                            session, currentAccessControl,
                                            java.time.Instant.now()).orElse(null);
                    var deps = new org.pangrova.planner.PlannerContext(
                            new org.pangrova.planner.stats.StatisticsCollector(
                                    new org.pangrova.coordinator.metadata.read.CatalogStatisticsProvider(
                                            bindings)),
                            mvCatalog, null,
                            org.pangrova.planner.runtimefilter.DynamicPartitionPruningConfig.DEFAULT,
                            null, durableRewriter, null,
                            org.pangrova.planner.cost.CboConfig.DEFAULT,
                            org.pangrova.planner.cost.CostModelConfig.SSD_DEFAULT);
                    return new PangrovaPlanner(metadataBootstrap.buildCalciteSchema(session, bindings), schemaPath, deps);
                });
    }

    private static MetadataBootstrap initializeMetadata() {
        try {
            System.err.println("[E2E-DEBUG] initializeMetadata: creating datasource");
            DataSource ds = MetadataSchemaInitializer.createDataSource(
                    pgJdbcUrl(), pgUsername(), pgPassword());
            System.err.println("[E2E-DEBUG] initializeMetadata: datasource created");
            MetadataSchemaInitializer.createSchema(ds);
            System.err.println("[E2E-DEBUG] initializeMetadata: schema created");
            stateStoreDataSource = ds;
            sharedWriteCommitJournal = new org.pangrova.metadata.coordination.JdbcWriteCommitJournal(ds);
            sharedDatasetControlClient = new org.pangrova.coordinator.metadata.NativeDatasetControlClient(
                    E2E_INTERNAL_AUTH_SECRET);
            sharedArtifactBuildStore = new org.pangrova.metadata.artifact.jdbc.build
                    .JdbcArtifactBuildStore(ds);
            MetadataStore store = new JdbcMetadataStore(ds);
            System.err.println("[E2E-DEBUG] initializeMetadata: JdbcMetadataStore created");
            var dependencies = new org.pangrova.coordinator.metadata.WorkerNativeDatasetControl.Dependencies(
                    () -> java.util.Objects.requireNonNull(workerManager, "E2E Worker registry").getActiveWorkers(),
                    () -> java.util.Objects.requireNonNull(sharedWriteCommitJournal, "E2E write commit journal"),
                    () -> E2E_WRITE_COMMIT_FENCER,
                    () -> requireE2ECoordination().getCurrentEpoch());
            java.util.function.Function<org.pangrova.metadata.catalog.CatalogDescriptor,
                    org.pangrova.spi.connector.metadata.ManagedSourceCatalog> sourceCatalogs = descriptor ->
                    new org.pangrova.metadata.source.jdbc.JdbcManagedSourceCatalog(ds,
                            new org.pangrova.spi.connector.metadata.ManagedSourceCatalog.Deployment(
                                    descriptor.name(), descriptor.incarnationId(), descriptor.revision()),
                            E2E_WRITE_COMMIT_FENCER::currentAnchor);
            TestConnectorHandleFactory connectorFactory = new TestConnectorHandleFactory(descriptor ->
                    java.util.Optional.of(new org.pangrova.coordinator.metadata.WorkerNativeDatasetControl(
                            descriptor, dependencies, sharedDatasetControlClient, sourceCatalogs.apply(descriptor))),
                    descriptor -> java.util.Optional.of(sourceCatalogs.apply(descriptor)));
            MetadataBootstrap bootstrap = MetadataBootstrap.initialize(store, connectorFactory);
            System.err.println("[E2E-DEBUG] initializeMetadata: bootstrap initialized");
            LOG.info("MetadataBootstrap initialized with state store tables");
            return bootstrap;
        } catch (Exception e) {
            throw new IllegalStateException(
                    "MetadataBootstrap initialization failed", e);
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
        closeE2ECoordination();
        stopQuietly(workerHttpServer);
        closeWorkerCatalogRuntime();
        closeQuietly(sharedDatasetControlClient, "native_dataset_control_client");
        sharedDatasetControlClient = null;
        shutdownMetadata();
        metadataBootstrap = initializeMetadata();
        materializationRepository = null;
        materializationCatalog = null;
        materializationDispatcher = null;
        lastRegisteredCatalogs.clear();
        rebuildQueryPipeline();
        initCoordinatorReadiness();
        if (workerTaskManagerRef != null) {
            workerTaskManagerRef.observeControlEpoch(
                    requireE2ECoordination().getCurrentEpoch());
        }
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
        java.util.Objects.requireNonNull(metadataBootstrap, "E2E metadata must be initialized");
        String coordinatorUrl = "http://localhost:" + java.util.Objects.requireNonNull(
                coordinatorHttpServer, "E2E Coordinator HTTP server").getPort();
        var runtime = org.pangrova.worker.jni.write.catalog.WorkerCatalogRuntime.discover(
                coordinatorUrl, E2E_INTERNAL_AUTH_SECRET, Thread.currentThread().getContextClassLoader());
        var writer = new org.pangrova.worker.jni.write.NativeWriteExecutor(runtime);
        try {
            NativeRuntime.nativeRegisterWriter(writer);
        } catch (RuntimeException | Error failure) {
            try {
                writer.close();
            } catch (RuntimeException cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
            throw failure;
        }
        sharedWorkerCatalogRuntime = runtime;
        workerCatalogCoordinatorUrl = coordinatorUrl;
        sharedNativeWriteExecutor = writer;
        if (sharedWorkerTaskExecutor instanceof NativeTaskExecutor executor) executor.setCatalogRuntime(runtime);
        // If the worker TaskManager has already been created (e.g. metadata reinit
        // after the worker is up), wire the new writer immediately so that
        // TaskStatusCollector can drain commit metadata for write tasks.
        if (workerTaskManagerRef != null) {
            workerTaskManagerRef.setWriteExecutor(writer);
        }
        LOG.info("event=native_writer_registered");
    }

    private static void closeWorkerCatalogRuntime() {
        var writer = sharedNativeWriteExecutor;
        sharedNativeWriteExecutor = null;
        var runtime = sharedWorkerCatalogRuntime;
        sharedWorkerCatalogRuntime = null;
        workerCatalogCoordinatorUrl = null;
        if (writer != null) writer.close();
        else if (runtime != null) runtime.close();
    }

    private static void refreshWorkerCatalogEndpoint() {
        String currentUrl = "http://localhost:" + coordinatorHttpServer.getPort();
        if (workerTaskManagerRef == null || currentUrl.equals(workerCatalogCoordinatorUrl)) return;
        int workerPort = sharedWorkerHttpPort;
        if (workerPort <= 0) throw new IllegalStateException("E2E Worker HTTP port is not initialized");
        try {
            workerHttpServer.stop();
        } catch (Exception failure) {
            throw new IllegalStateException("Failed to drain Worker HTTP before rebinding catalog control", failure);
        }
        closeWorkerCatalogRuntime();
        registerNativeWriter();
        workerHttpServer = startWorkerHttpServer(workerTaskManagerRef,
                java.util.Objects.requireNonNull(sharedWorkerTaskExecutor, "Worker task executor"),
                sharedResultServerPort, createWorkerHealthEndpoint(), workerPort);
        registerWorker(workerHttpServer.getPort(), sharedResultServerPort);
    }

    /** Restores only the Worker control endpoint after a simulated HTTP outage.
     * Keeping the same TaskManager and port preserves the issued task's admission
     * authority, so a later cancellation can obtain its actual drain receipt. */
    protected static void restartWorkerHttpServerAfterOutage() {
        int workerPort = sharedWorkerHttpPort;
        if (workerPort <= 0) throw new IllegalStateException("E2E Worker HTTP port is not initialized");
        workerHttpServer = startWorkerHttpServer(
                java.util.Objects.requireNonNull(workerTaskManagerRef, "Worker task manager"),
                java.util.Objects.requireNonNull(sharedWorkerTaskExecutor, "Worker task executor"),
                sharedResultServerPort, createWorkerHealthEndpoint(), workerPort);
    }

    private static final class E2eWriteCommitFencer implements org.pangrova.coordinator.query.task.write.WriteCommitFencer {
        @Override
        public org.pangrova.spi.coordination.ExecutionAnchor currentAnchor(String executionId) {
            return requireE2ECoordination().requireExecutionAnchor(executionId);
        }

        @Override
        public boolean isCurrent(String executionId, long fencingToken) {
            return requireE2ECoordination().validateDurableExecutionToken(executionId, fencingToken);
        }
    }

    protected static void registerWorker(int workerHttpPort, int transportPort) {
        var advertisement = java.util.Objects.requireNonNull(
                e2eCapabilityAdvertisement,
                "Native capability advertisement must be initialized before registration");
        long availableMemoryBytes = java.util.Objects.requireNonNull(
                sharedWorkerMemoryArbitrator,
                "E2E Worker arbitrator must be initialized before registration")
                .getAvailableMemory();
        WorkerInfo workerInfo = new WorkerInfo(
                WORKER_ID, IN_PROCESS_WORKER_HOST, workerHttpPort,
                transportPort, sharedResultServerPort,
                HealthState.ACTIVE, MemoryPressure.NONE,
                0.0, 0, availableMemoryBytes,
                Instant.now(), 1.0, null, 2,
                null, List.of(), null, advertisement.inventory(),
                java.util.Objects.requireNonNull(workerTaskManagerRef).workerExecutionIdentity());
        workerManager.registerWorker(workerInfo);
    }

    protected static void configureStatusReporter() {
        if (coordinatorHttpServer == null || statusReporter == null) {
            return;
        }
        statusReporter.updateTarget("http://localhost:" + coordinatorHttpServer.getPort());
    }

    /** Test-only status transport for routing aliases sharing one physical Worker. */
    protected static void configureVirtualRoutingStatusDelivery(
            java.util.function.Consumer<org.pangrova.protocol.rest.task.TaskStatusReport> delivery) {
        statusReportDelivery = java.util.Objects.requireNonNull(delivery);
    }

    protected static void restoreSharedStatusDelivery() {
        statusReportDelivery = statusReporter::report;
    }

    protected static void reportToSharedCoordinator(
            org.pangrova.protocol.rest.task.TaskStatusReport report) {
        statusReporter.report(report);
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
        refreshWorkerCatalogEndpoint();
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

}
