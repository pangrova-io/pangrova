package org.pangrova.coordinator.bootstrap;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.coordinator.query.AsyncSubmitConfig;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.coordinator.bootstrap.module.CacheModule;
import org.pangrova.coordinator.bootstrap.module.CoordinatorModule;
import org.pangrova.coordinator.bootstrap.module.DefaultModuleContext;
import org.pangrova.coordinator.bootstrap.module.IndexingModule;
import org.pangrova.coordinator.bootstrap.module.QueryExecutionModule;
import org.pangrova.coordinator.bootstrap.module.SecurityModule;
import org.pangrova.coordinator.coordination.HighAvailabilityCoordinationService;
import org.pangrova.coordinator.metadata.MetadataBootstrap;
import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.coordinator.query.QueryManagerContext;
import org.pangrova.coordinator.query.QueryTimeoutEnforcer;
import org.pangrova.coordinator.query.cache.CacheWritebackConfig;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.execution.result.QueryCacheService;
import org.pangrova.coordinator.query.execution.split.CacheAffinityAssigner;
import org.pangrova.coordinator.query.execution.split.SplitAssignmentManager;
import org.pangrova.coordinator.query.execution.dispatch.dag.DAGDispatchEngine;
import org.pangrova.coordinator.query.execution.dispatch.dag.StageCompletionCallback;
import org.pangrova.coordinator.query.task.TaskStatusReceiver;
import org.pangrova.coordinator.query.task.TaskStatusReceiverContext;
import org.pangrova.coordinator.result.CoordinatorDataProducer;
import org.pangrova.coordinator.result.CoordinatorResultBuffer;
import org.pangrova.coordinator.result.CoordinatorResultService;
import org.pangrova.coordinator.result.CoordinatorResultWriter;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.coordinator.speculation.DuplicateTaskResolver;
import org.pangrova.coordinator.speculation.SpeculationConfig;
import org.pangrova.coordinator.speculation.SpeculativeStageHandler;
import org.pangrova.coordinator.speculation.SpeculativeTaskLauncher;
import org.pangrova.coordinator.speculation.StragglerDetector;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.worker.transport.TransportServer;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.metadata.coordination.JdbcWorkerRegistry;
import org.pangrova.observability.audit.LogAuditStore;
import org.pangrova.observability.audit.QueryAuditService;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.protocol.rest.query.ResultEndpoint;
import org.pangrova.protocol.rest.query.ResultProtocol;
import org.pangrova.security.audit.AuditEventEmitter;
import org.pangrova.security.audit.LoggingAuditEventWriter;
import org.pangrova.spi.security.AccessControl;
import org.pangrova.spi.coordination.WorkerRegistry;
import org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry;
import org.pangrova.spi.observability.MetricsProvider;
import org.pangrova.spi.observability.TracingProvider;
import org.pangrova.spi.transport.TransportServerConfig;
import org.pangrova.spi.transport.TransportServerLifecycle;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Bootstraps the query subsystem via modular configuration. */
final class QueryBootstrap {

    private static final Logger LOG = LoggerFactory.getLogger(QueryBootstrap.class);

    private final CoordinatorBootstrapConfig config;
    private final TracingProvider tracingProvider;
    private final MetricsProvider metricsProvider;
    private final AccessControl accessControl;
    private final MetadataBootstrap metadataBootstrap;
    private final ModuleIntegrationBootstrap moduleWiring;
    private final javax.sql.DataSource metadataDataSource;
    private final StreamingBootstrap streamingBootstrap;
    private final HighAvailabilityCoordinationService coordinationService;
    private final org.pangrova.security.secret.SecretResolver secretResolver;

    private WorkerManager workerManager;
    private QueryManager queryManager;
    private TaskStatusReceiver taskStatusReceiver;
    private CoordinatorTaskTracker taskTracker;
    private AuditEventEmitter auditEmitter;
    private QueryTimeoutEnforcer queryTimeoutEnforcer;
    private CircuitBreakerPolicy circuitBreaker;
    private TransportServerLifecycle coordinatorTransportServer;
    private CoordinatorResultService coordinatorResultService;
    private BufferAllocator coordinatorTransportAllocator;
    private ScheduledExecutorService schedulerExecutor;
    private SplitAssignmentManager splitAssignmentManager;
    private CacheAffinityAssigner cacheAffinityAssigner;
    private SpeculativeStageHandler speculativeStageHandler;
    private SpeculativeTaskLauncher speculativeTaskLauncher;
    private org.pangrova.coordinator.speculation.SpeculativeExecutionScheduler speculationScheduler;
    private RemoteTaskDispatcher remoteTaskDispatcher;
    private org.pangrova.observability.metrics.CoordinatorMetrics coordinatorMetrics;
    private QueryDispatcher queryDispatcher;
    private org.pangrova.spi.coordination.QueryStateJournal stateJournal;
    private QueryExecutionBootstrap executionWiring;
    private org.pangrova.coordinator.query.task.NativeWriteCommitTracker writeCommitTracker;
    private SelfIntelligenceBootstrap selfIntelligence;

    QueryBootstrap(CoordinatorBootstrapConfig config, TracingProvider tracingProvider,
                   MetricsProvider metricsProvider, AccessControl accessControl,
                   MetadataBootstrap metadataBootstrap, ModuleIntegrationBootstrap moduleWiring,
                   javax.sql.DataSource metadataDataSource) {
        this(config, tracingProvider, metricsProvider, accessControl,
                metadataBootstrap, moduleWiring, metadataDataSource, null, null, null);
    }

    QueryBootstrap(CoordinatorBootstrapConfig config, TracingProvider tracingProvider,
                   MetricsProvider metricsProvider, AccessControl accessControl,
                   MetadataBootstrap metadataBootstrap, ModuleIntegrationBootstrap moduleWiring,
                   javax.sql.DataSource metadataDataSource, StreamingBootstrap streamingBootstrap) {
        this(config, tracingProvider, metricsProvider, accessControl,
                metadataBootstrap, moduleWiring, metadataDataSource, streamingBootstrap, null, null);
    }

    QueryBootstrap(CoordinatorBootstrapConfig config, TracingProvider tracingProvider,
                   MetricsProvider metricsProvider, AccessControl accessControl,
                   MetadataBootstrap metadataBootstrap, ModuleIntegrationBootstrap moduleWiring,
                   javax.sql.DataSource metadataDataSource, StreamingBootstrap streamingBootstrap,
                   HighAvailabilityCoordinationService coordinationService,
                   org.pangrova.security.secret.SecretResolver secretResolver) {
        this.config = config;
        this.tracingProvider = tracingProvider;
        this.metricsProvider = metricsProvider;
        this.accessControl = accessControl;
        this.metadataBootstrap = metadataBootstrap;
        this.moduleWiring = moduleWiring;
        this.metadataDataSource = metadataDataSource;
        this.streamingBootstrap = streamingBootstrap;
        this.coordinationService = coordinationService;
        this.secretResolver = secretResolver;
    }

    void start() {
        CoordinatorPreflightChecks.validateJdbcConnection(metadataDataSource);
        WorkerRegistry workerRegistry = createWorkerRegistry();
        workerManager = new WorkerManager(
                config.workerSuspectThresholdMs(), config.workerDeadThresholdMs(), workerRegistry);
        // P1-5: aggregate per-tenant usage reported by worker heartbeats into a
        // cluster-wide view (phase 1: reporting + aggregation observability).
        workerManager.setTenantUsageAggregator(
                new org.pangrova.coordinator.worker.ClusterTenantUsageAggregator());
        initCoordinatorMetrics();
        taskTracker = new CoordinatorTaskTracker();
        coordinatorResultService = startCoordinatorTransport();
        initSplitAssignment();
        if (cacheAffinityAssigner != null) {
            workerManager.addTopologyListener(cacheAffinityAssigner);
        }
        initSpeculation();
        stateJournal = createStateJournal();
        CatalogRegistry catalog = metadataBootstrap != null
                ? metadataBootstrap.catalogRegistry() : null;
        this.writeCommitTracker = catalog != null
                ? new org.pangrova.coordinator.query.task.NativeWriteCommitTracker() : null;
        DefaultModuleContext moduleContext = loadModules();
        executionWiring = new QueryExecutionBootstrap(config, tracingProvider, accessControl,
                metadataBootstrap, moduleWiring, streamingBootstrap, coordinationService,
                writeCommitTracker);
        queryDispatcher = executionWiring.buildQueryDispatcher(workerManager, taskTracker,
                moduleContext, resolveCircuitBreaker(), coordinatorResultService,
                splitAssignmentManager, speculativeStageHandler, stateJournal);
        this.remoteTaskDispatcher = executionWiring.remoteTaskDispatcher();
        if (speculativeTaskLauncher != null) {
            speculativeTaskLauncher.setDispatchDependencies(
                    remoteTaskDispatcher, workerManager, taskTracker);
        }
        queryManager = buildQueryManager(queryDispatcher);
        taskStatusReceiver = buildTaskStatusReceiver();
        taskTracker.setTaskFailureCallback((queryId, taskId) ->
                queryManager.onTaskFailed(queryId, new org.pangrova.protocol.rest.error.ErrorDetail(
                        "TASK_FAILED", taskId, "Task " + taskId + " failed")));
        startTimeoutEnforcer();
        queryManager.startPeriodicCleanup(schedulerExecutor(),
                java.time.Duration.ofSeconds(config.queryRetentionSeconds()),
                java.time.Duration.ofSeconds(config.queryCleanupIntervalSeconds()));
        startSpeculationScheduler();
        selfIntelligence = new SelfIntelligenceBootstrap(config, moduleWiring, schedulerExecutor(),
                catalog, metadataDataSource, secretResolver);
        if (config.embeddingEnabled()) {
            selfIntelligence.initEmbeddingPipeline();
            LOG.info("event=embedding_queue_coordinator_started persistent={}",
                    selfIntelligence.hasPersistentQueue());
        }
        selfIntelligence.initialize(executionWiring);
        selfIntelligence.wireIntoQueryDispatcher(queryDispatcher);
        // P1-5b: enforce tenant budget limits across coordinators on the submission path.
        if (moduleWiring != null) {
            var admission = SecurityBootstrap.createClusterAdmissionController(
                    moduleWiring.getTenantStore(), workerManager.tenantUsageView(),
                    metadataDataSource);
            if (admission != null) {
                queryManager.setClusterAdmissionController(admission);
            }
        }
        LOG.info("event=query_bootstrap_complete speculation={} pullMode={} cacheWriteback={} embedding={}",
                config.speculationEnabled(), config.pullModeSplitAssignmentEnabled(),
                config.cacheWritebackEnabled(), config.embeddingEnabled());
    }

    private DefaultModuleContext loadModules() {
        List<CoordinatorModule> modules = buildModuleList();
        modules.sort(Comparator.comparingInt(CoordinatorModule::priority));
        DefaultModuleContext ctx = new DefaultModuleContext();
        seedBindings(ctx);
        for (CoordinatorModule module : modules) {
            module.configure(ctx);
            LOG.debug("event=module_loaded module={} priority={}",
                    module.getClass().getSimpleName(), module.priority());
        }
        return ctx;
    }

    private List<CoordinatorModule> buildModuleList() {
        List<CoordinatorModule> modules = new ArrayList<>();
        boolean tenantAware = moduleWiring != null && moduleWiring.getTenantResolver() != null;
        modules.add(new QueryExecutionModule(tenantAware));
        modules.add(new SecurityModule(accessControl));
        modules.add(new CacheModule());
        modules.add(new IndexingModule());
        return modules;
    }

    private void seedBindings(DefaultModuleContext ctx) {
        var sharedRegistry = new org.pangrova.planner.runtimefilter.RuntimeFilterRegistry();
        SqlCompiler compiler = new PlannerWiring(config, metadataBootstrap)
                .buildSqlCompiler(sharedRegistry);
        ctx.bind(SqlCompiler.class, compiler);
        ctx.bind(org.pangrova.planner.runtimefilter.RuntimeFilterRegistry.class, sharedRegistry);
        CatalogRegistry catalog = metadataBootstrap != null ? metadataBootstrap.catalogRegistry() : null;
        if (catalog != null) {
            ctx.bind(CatalogRegistry.class, catalog);
        }
        bindIndexBootstrap(ctx);
        QueryCacheService cacheService = buildCacheService(compiler);
        if (cacheService != null) {
            ctx.bind(QueryCacheService.class, cacheService);
        }
    }

    private void bindIndexBootstrap(DefaultModuleContext ctx) {
        if (moduleWiring == null) {
            return;
        }
        if (moduleWiring.getIndexingBootstrap() != null) {
            ctx.bind(IndexingBootstrap.class, moduleWiring.getIndexingBootstrap());
        } else if (moduleWiring.getIndexQueryBootstrap() != null) {
            ctx.bind(IndexQueryBootstrap.class, moduleWiring.getIndexQueryBootstrap());
        }
    }

    private QueryCacheService buildCacheService(SqlCompiler compiler) {
        var resolvedCache = QueryCacheService.resolveCache(compiler);
        if (resolvedCache == null) {
            return null;
        }
        var cacheCoordinator = new org.pangrova.coordinator.query.cache.ResultCacheCoordinator(
                resolvedCache, null, null);
        CacheWritebackConfig wbConfig = config.cacheWritebackEnabled()
                ? new CacheWritebackConfig(true) : CacheWritebackConfig.DISABLED;
        return new QueryCacheService(cacheCoordinator, wbConfig, coordinatorResultService);
    }

    private QueryManager buildQueryManager(QueryDispatcher qd) {
        auditEmitter = new AuditEventEmitter(new LoggingAuditEventWriter());
        AsyncSubmitConfig asyncConfig = AsyncSubmitConfig.defaultEnabled();
        ExecutorService asyncExecutor = ManagedExecutorFactory
                .newFixedThreadPool("async-query-submit", 4);
        QueryManagerContext deps = new QueryManagerContext(qd, tracingProvider, metricsProvider,
                moduleWiring.getTenantResolver(), moduleWiring.getQuotaChecker(),
                moduleWiring.getLineageEmitter(), auditEmitter,
                new QueryAuditService(LogAuditStore.shared()),
                moduleWiring.getResilienceRegistry(),
                new org.pangrova.planner.throttle.QueryThrottlingService(1000, 100.0, 50.0),
                stateJournal, null);
        LOG.info("event=async_submit_wired enabled={} submitTimeoutMs={}",
                asyncConfig.enabled(), asyncConfig.submitTimeoutMs());
        QueryManager qm = new QueryManager(deps);
        // SI-6: stamp this coordinator's id on journal events so owner-scoped
        // failover recovery (replayActiveQueriesForOwner) can filter by owner.
        qm.setLocalCoordinatorId(config.coordinatorId());
        return qm;
    }

    private org.pangrova.spi.coordination.QueryStateJournal createStateJournal() {
        if (metadataDataSource == null) {
            return org.pangrova.coordinator.coordination.inmemory.InMemoryQueryStateJournal.INSTANCE;
        }
        try {
            var cfg = new org.pangrova.spi.coordination.StateJournalConfig("JDBC", null, 100, 50, 24);
            return org.pangrova.coordinator.query.state.QueryStateJournalFactory.create(cfg, metadataDataSource);
        } catch (Exception e) {
            LOG.warn("event=jdbc_journal_init_failed error={} fallback=InMemory", e.getMessage());
            return org.pangrova.coordinator.coordination.inmemory.InMemoryQueryStateJournal.INSTANCE;
        }
    }

    private WorkerRegistry createWorkerRegistry() {
        if (metadataDataSource != null) {
            LOG.info("event=worker_registry_selected type=JDBC");
            return new JdbcWorkerRegistry(metadataDataSource);
        }
        LOG.warn("event=worker_registry_selected type=InMemory reason=no_datasource");
        return new InMemoryWorkerRegistry();
    }

    void shutdown() {
        if (speculationScheduler != null) {
            speculationScheduler.close();
        }
        CoordinatorShutdownSequence.stopTimeoutEnforcer(queryTimeoutEnforcer);
        CoordinatorShutdownSequence.closeAuditEmitter(auditEmitter);
        CoordinatorShutdownSequence.stopCoordinatorTransport(coordinatorTransportServer);
        CoordinatorShutdownSequence.closeTransportAllocator(coordinatorTransportAllocator);
        CoordinatorShutdownSequence.shutdownSchedulerExecutor(schedulerExecutor);
        if (splitAssignmentManager != null) {
            splitAssignmentManager.stopLeaseReclamation();
        }
    }

    private CoordinatorResultService startCoordinatorTransport() {
        CoordinatorPreflightChecks.validateAdvertisedHost(config);
        coordinatorTransportAllocator = new RootAllocator(config.coordinatorTransportAllocatorLimitBytes());
        try {
            CoordinatorResultBuffer buffer = new CoordinatorResultBuffer(
                    schedulerExecutor(), CoordinatorResultBuffer.DEFAULT_RETENTION_MS);
            // Attach FlightSpanInterceptor for distributed tracing of inbound Flight RPCs.
            java.util.List<io.grpc.ServerInterceptor> interceptors = tracingProvider != null
                    ? java.util.List.of(new org.pangrova.observability.tracing.FlightSpanInterceptor(tracingProvider))
                    : java.util.List.of();
            coordinatorTransportServer = new TransportServer(
                    new CoordinatorDataProducer(buffer),
                    new TransportServerConfig(config.coordinatorTransportPort()),
                    null,
                    interceptors);
            coordinatorTransportServer.start();
            ResultEndpoint endpoint = new ResultEndpoint(config.coordinatorTransportAdvertisedHost(),
                    coordinatorTransportServer.getPort(), "", ResultProtocol.GRPC.value());
            LOG.info("event=coordinator_transport_started port={} advertisedHost={}",
                    coordinatorTransportServer.getPort(), config.coordinatorTransportAdvertisedHost());
            return new CoordinatorResultService(buffer,
                    new CoordinatorResultWriter(coordinatorTransportAllocator), endpoint);
        } catch (Exception e) {
            coordinatorTransportAllocator.close();
            coordinatorTransportAllocator = null;
            throw e;
        }
    }

    ScheduledExecutorService schedulerExecutor() {
        if (schedulerExecutor == null) {
            schedulerExecutor = ManagedExecutorFactory.newSingleThreadScheduledExecutor(
                    "coordinator-health-scheduler");
        }
        return schedulerExecutor;
    }

    private void startTimeoutEnforcer() {
        java.time.Duration timeout = java.time.Duration.ofSeconds(config.queryTimeoutSeconds());
        java.time.Duration interval = java.time.Duration.ofSeconds(config.timeoutCheckIntervalSeconds());
        queryTimeoutEnforcer = new QueryTimeoutEnforcer(timeout, interval);
        queryTimeoutEnforcer.setTimeoutCallback(queryManager::onQueryTimeout);
        queryTimeoutEnforcer.start(queryManager::getStateMachines);
        LOG.info("event=timeout_enforcer_started timeout={}s interval={}s",
                config.queryTimeoutSeconds(), config.timeoutCheckIntervalSeconds());
    }

    private void initSplitAssignment() {
        cacheAffinityAssigner = new CacheAffinityAssigner();
        if (!config.pullModeSplitAssignmentEnabled()) {
            LOG.info("event=split_assignment_initialized cacheAffinity=enabled pullMode=false");
            return;
        }
        splitAssignmentManager = new SplitAssignmentManager(60_000L, cacheAffinityAssigner);
        splitAssignmentManager.startLeaseReclamation(10_000L);
        LOG.info("event=split_assignment_initialized cacheAffinity=enabled pullMode=true");
    }

    private void initSpeculation() {
        if (!config.speculationEnabled()) {
            return;
        }
        speculativeTaskLauncher = new SpeculativeTaskLauncher();
        speculativeStageHandler = new SpeculativeStageHandler(
                new SpeculationConfig(true, SpeculationConfig.DEFAULT_DETECTION_INTERVAL_MS),
                new StragglerDetector(), speculativeTaskLauncher, taskTracker);
        LOG.info("event=speculation_initialized");
    }

    private void startSpeculationScheduler() {
        if (speculativeStageHandler == null || !speculativeStageHandler.isEnabled()) {
            return;
        }
        speculationScheduler = new org.pangrova.coordinator.speculation.SpeculativeExecutionScheduler(
                speculativeStageHandler, queryManager, taskTracker,
                SpeculationConfig.DEFAULT_DETECTION_INTERVAL_MS);
        speculationScheduler.start();
    }

    private TaskStatusReceiver buildTaskStatusReceiver() {
        CatalogRegistry catalog = metadataBootstrap != null
                ? metadataBootstrap.catalogRegistry() : null;
        DAGDispatchEngine engine = queryDispatcher.dagEngine();
        StageCompletionCallback stageCallback = new StageCompletionCallback() {
            @Override
            public void onTaskTerminal(String queryId, String taskId, String state,
                                       org.pangrova.protocol.rest.error.ErrorDetail error) {
                engine.onTaskTerminal(queryId, taskId, state, error);
            }

            @Override
            public boolean isQueryComplete(String queryId) {
                return engine.isQueryComplete(queryId);
            }
        };
        // A-COMMIT-01: consume any durable PENDING write-commit intents a crashed predecessor
        // left behind before this coordinator accepts new writes.
        org.pangrova.spi.coordination.WriteCommitJournal writeCommitJournal =
                WriteCommitWiring.createJournal(metadataDataSource);
        WriteCommitWiring.recoverPendingIntents(metadataDataSource, writeCommitJournal, catalog);
        TaskStatusReceiver receiver = createTaskStatusReceiver(
                catalog, stageCallback, writeCommitJournal);
        org.pangrova.coordinator.query.task.WriteCommitFencer fencer =
                WriteCommitWiring.createFencer(coordinationService);
        if (fencer != null) {
            receiver.setWriteCommitFencer(fencer);
        }
        return receiver;
    }

    /**
     * Chooses the receiver wiring: the speculation-aware variant additionally carries the
     * duplicate-task resolver and remote dispatcher needed to launch a straggler's duplicate and
     * cancel the loser.
     */
    private TaskStatusReceiver createTaskStatusReceiver(
            CatalogRegistry catalog, StageCompletionCallback stageCallback,
            org.pangrova.spi.coordination.WriteCommitJournal writeCommitJournal) {
        boolean speculative = config.speculationEnabled() && speculativeTaskLauncher != null;
        return new TaskStatusReceiver(new TaskStatusReceiverContext(
                queryManager, taskTracker, workerManager,
                speculative ? speculativeTaskLauncher : null,
                speculative ? new DuplicateTaskResolver() : null,
                speculative ? remoteTaskDispatcher : null,
                queryManager.getStateJournal(), catalog, writeCommitTracker, stageCallback,
                moduleWiring != null ? moduleWiring.getLineageEmitter() : null, writeCommitJournal));
    }

    private CircuitBreakerPolicy resolveCircuitBreaker() {
        CircuitBreakerPolicy cb = moduleWiring != null ? moduleWiring.getCircuitBreakerPolicy() : null;
        this.circuitBreaker = cb;
        if (cb == null) {
            LOG.debug("event=circuit_breaker_skip reason=resilience_disabled");
        }
        return cb;
    }

    private void initCoordinatorMetrics() {
        if (metricsProvider == null) {
            return;
        }
        coordinatorMetrics = new org.pangrova.observability.metrics.CoordinatorMetrics(
                metricsProvider, () -> (double) workerManager.getActiveWorkers().size());
        LOG.info("event=coordinator_metrics_initialized");
    }

    WorkerManager workerManager() { return workerManager; }
    QueryManager queryManager() { return queryManager; }
    org.pangrova.observability.metrics.CoordinatorMetrics coordinatorMetrics() { return coordinatorMetrics; }
    TaskStatusReceiver taskStatusReceiver() { return taskStatusReceiver; }
    CoordinatorTaskTracker taskTracker() { return taskTracker; }
    CircuitBreakerPolicy circuitBreaker() { return circuitBreaker; }
    TransportServerLifecycle coordinatorTransportServer() { return coordinatorTransportServer; }
    SplitAssignmentManager splitAssignmentManager() { return splitAssignmentManager; }
    CacheAffinityAssigner cacheAffinityAssigner() { return cacheAffinityAssigner; }
    QueryDispatcher queryDispatcher() { return queryDispatcher; }
    org.pangrova.coordinator.query.task.NativeWriteCommitTracker writeCommitTracker() { return writeCommitTracker; }
}
