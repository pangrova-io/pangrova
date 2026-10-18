package org.pangrova.coordinator.bootstrap;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ExecutorService;

import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.coordinator.bootstrap.module.DefaultModuleContext;
import org.pangrova.coordinator.coordination.HighAvailabilityCoordinationService;
import org.pangrova.coordinator.metadata.MetadataBootstrap;
import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.coordinator.query.QueryTimeoutEnforcer;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.execution.split.CacheAffinityAssigner;
import org.pangrova.coordinator.query.execution.split.SplitAssignmentManager;
import org.pangrova.coordinator.query.execution.dispatch.dag.DAGDispatchEngine;
import org.pangrova.coordinator.query.execution.dispatch.dag.StageCompletionCallback;
import org.pangrova.coordinator.query.task.TaskStatusReceiver;
import org.pangrova.coordinator.query.task.TaskStatusReceiverContext;
import org.pangrova.coordinator.result.CoordinatorResultService;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.coordinator.scheduler.TaskScheduler;
import org.pangrova.coordinator.speculation.DuplicateTaskResolver;
import org.pangrova.coordinator.speculation.SpeculationConfig;
import org.pangrova.coordinator.speculation.SpeculativeStageHandler;
import org.pangrova.coordinator.speculation.SpeculativeTaskLauncher;
import org.pangrova.coordinator.speculation.StragglerDetector;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.security.audit.AuditEventEmitter;
import org.pangrova.spi.security.AccessControl;
import org.pangrova.spi.coordination.WorkerRegistry;
import org.pangrova.spi.observability.MetricsProvider;
import org.pangrova.spi.observability.TracingProvider;
import org.pangrova.spi.transport.TransportServerLifecycle;
import org.apache.arrow.memory.BufferAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Bootstraps the query subsystem via modular configuration. */
final class QueryBootstrap {

    private static final Logger LOG = LoggerFactory.getLogger(QueryBootstrap.class);
    private static final long EXECUTOR_DRAIN_TIMEOUT_MILLIS = 5_000;
    private static final long SPLIT_ASSIGNMENT_LEASE_MILLIS = 60_000;
    private static final long SPLIT_RECLAMATION_INTERVAL_MILLIS = 10_000;

    private final CoordinatorBootstrapConfig config;
    private final TracingProvider tracingProvider;
    private final MetricsProvider metricsProvider;
    private final AccessControl accessControl;
    private final MetadataBootstrap metadataBootstrap;
    private final ModuleIntegrationBootstrap moduleWiring;
    private final javax.sql.DataSource metadataDataSource;
    private final StreamingBootstrap streamingBootstrap;
    private final HighAvailabilityCoordinationService coordinationService;
    private final QueryDurableRecoveryWiring durableRecovery;
    private final org.pangrova.security.secret.SecretResolver secretResolver;
    private final org.pangrova.metadata.ai.AiInfrastructureStores aiInfrastructureStores;

    private WorkerManager workerManager;
    private QueryManager queryManager;
    private java.util.concurrent.ExecutorService asyncSubmitExecutor;
    private TaskStatusReceiver taskStatusReceiver;
    private CoordinatorTaskTracker taskTracker;
    private AuditEventEmitter auditEmitter;
    private QueryTimeoutEnforcer queryTimeoutEnforcer;
    private CircuitBreakerPolicy circuitBreaker;
    private TransportServerLifecycle controlPlaneResultServer;
    private CoordinatorResultService controlPlaneResultService;
    private BufferAllocator controlPlaneResultAllocator;
    private org.pangrova.coordinator.result.CoordinatorResultBuffer controlPlaneResultBuffer;
    private ScheduledExecutorService schedulerExecutor;
    private ExecutorService terminalCleanupExecutor;
    private ExecutorService shuffleCleanupExecutor;
    private SplitAssignmentManager splitAssignmentManager;
    private CacheAffinityAssigner cacheAffinityAssigner;
    private SpeculativeStageHandler speculativeStageHandler;
    private SpeculativeTaskLauncher speculativeTaskLauncher;
    private org.pangrova.coordinator.speculation.SpeculativeExecutionScheduler speculationScheduler;
    private RemoteTaskDispatcher remoteTaskDispatcher;
    private TaskScheduler taskScheduler;
    private org.pangrova.planner.cache.QueryResultCache queryResultCache;
    private org.pangrova.metadata.artifact.build.ArtifactBuildStore artifactBuildStore;
    private org.pangrova.coordinator.ai.embedding.GeneratedValueMaterializationCoordinator
            generatedValueMaterialization;
    private org.pangrova.coordinator.ai.embedding.EmbeddingModelRegistry
            embeddingModelRegistry;
    private org.pangrova.coordinator.artifact.DurableArtifactBuildDispatcher
            artifactBuildDispatcher;
    private org.pangrova.observability.metrics.CoordinatorMetrics coordinatorMetrics;
    private QueryDispatcher queryDispatcher;
    private org.pangrova.spi.coordination.QueryStateJournal stateJournal;
    private QueryExecutionBootstrap executionWiring;
    private org.pangrova.coordinator.query.task.write.NativeWriteCommitTracker writeCommitTracker;
    private org.pangrova.spi.coordination.WriteCommitJournal writeCommitJournal;
    private org.pangrova.coordinator.query.task.write.WriteCommitRecoveryService
            writeCommitRecoveryService;
    private SelfIntelligenceBootstrap selfIntelligence;
    private volatile java.util.concurrent.ScheduledFuture<?> artifactPoll;
    private final org.pangrova.coordinator.bootstrap.lifecycle.QueryShutdownOwnership shutdownOwnership =
            new org.pangrova.coordinator.bootstrap.lifecycle.QueryShutdownOwnership(
                    () -> queryManager, () -> stateJournal, this::drainSubmissions,
                    this::joinCallbacks, this::closeQueryResources);

    QueryBootstrap(CoordinatorBootstrapConfig config, TracingProvider tracingProvider,
                   MetricsProvider metricsProvider, AccessControl accessControl,
                   MetadataBootstrap metadataBootstrap, ModuleIntegrationBootstrap moduleWiring,
                   javax.sql.DataSource metadataDataSource) {
        this(config, tracingProvider, metricsProvider, accessControl,
                metadataBootstrap, moduleWiring, metadataDataSource, null, null, null, null);
    }

    QueryBootstrap(CoordinatorBootstrapConfig config, TracingProvider tracingProvider,
                   MetricsProvider metricsProvider, AccessControl accessControl,
                   MetadataBootstrap metadataBootstrap, ModuleIntegrationBootstrap moduleWiring,
                   javax.sql.DataSource metadataDataSource, StreamingBootstrap streamingBootstrap) {
        this(config, tracingProvider, metricsProvider, accessControl,
                metadataBootstrap, moduleWiring, metadataDataSource, streamingBootstrap,
                null, null, null);
    }

    QueryBootstrap(CoordinatorBootstrapConfig config, TracingProvider tracingProvider,
                   MetricsProvider metricsProvider, AccessControl accessControl,
                   MetadataBootstrap metadataBootstrap, ModuleIntegrationBootstrap moduleWiring,
                   javax.sql.DataSource metadataDataSource, StreamingBootstrap streamingBootstrap,
                   HighAvailabilityCoordinationService coordinationService,
                   org.pangrova.security.secret.SecretResolver secretResolver) {
        this(config, tracingProvider, metricsProvider, accessControl, metadataBootstrap,
                moduleWiring, metadataDataSource, streamingBootstrap, coordinationService,
                secretResolver, metadataDataSource == null ? null
                        : org.pangrova.metadata.ai.AiInfrastructureStores
                                .jdbc(metadataDataSource));
    }

    // The bootstrap assembles these lifecycle owners exactly once at process startup.
    @SuppressWarnings("checkstyle:ParameterNumber")
    QueryBootstrap(CoordinatorBootstrapConfig config, TracingProvider tracingProvider,
                   MetricsProvider metricsProvider, AccessControl accessControl,
                   MetadataBootstrap metadataBootstrap, ModuleIntegrationBootstrap moduleWiring,
                   javax.sql.DataSource metadataDataSource, StreamingBootstrap streamingBootstrap,
                   HighAvailabilityCoordinationService coordinationService,
                   org.pangrova.security.secret.SecretResolver secretResolver,
                   org.pangrova.metadata.ai.AiInfrastructureStores aiInfrastructureStores) {
        this.config = config;
        this.tracingProvider = tracingProvider;
        this.metricsProvider = metricsProvider;
        this.accessControl = accessControl;
        this.metadataBootstrap = metadataBootstrap;
        this.moduleWiring = moduleWiring;
        this.metadataDataSource = metadataDataSource;
        this.streamingBootstrap = streamingBootstrap;
        this.coordinationService = coordinationService;
        durableRecovery = new QueryDurableRecoveryWiring(
                config, metadataDataSource, coordinationService);
        this.secretResolver = secretResolver;
        this.aiInfrastructureStores = aiInfrastructureStores;
    }

    void start() {
        CoordinatorPreflightChecks.validateJdbcConnection(metadataDataSource);
        var runtimeWiring = new QueryRuntimeWiring(new QueryRuntimeWiring.Dependencies(
                config, tracingProvider, metricsProvider, moduleWiring, metadataDataSource));
        WorkerRegistry workerRegistry = runtimeWiring.createWorkerRegistry();
        workerManager = new WorkerManager(
                config.workerSuspectThresholdMs(), config.workerDeadThresholdMs(), workerRegistry);
        // P1-5: aggregate per-tenant usage reported by worker heartbeats into a
        // cluster-wide view (phase 1: reporting + aggregation observability).
        workerManager.setTenantUsageAggregator(
                new org.pangrova.coordinator.worker.ClusterTenantUsageAggregator());
        initCoordinatorMetrics();
        taskTracker = new CoordinatorTaskTracker();
        controlPlaneResultService = startControlPlaneResultTransport();
        initSplitAssignment();
        if (cacheAffinityAssigner != null) {
            workerManager.addTopologyListener(cacheAffinityAssigner);
        }
        initSpeculation();
        stateJournal = runtimeWiring.createStateJournal();
        CatalogRegistry catalog = metadataBootstrap != null
                ? metadataBootstrap.catalogRegistry() : null;
        this.writeCommitTracker = catalog != null
                ? new org.pangrova.coordinator.query.task.write.NativeWriteCommitTracker() : null;
        DefaultModuleContext moduleContext = new QueryModuleLoader(
                config, metadataBootstrap, moduleWiring, accessControl).load();
        taskScheduler = moduleContext.require(TaskScheduler.class);
        queryResultCache = moduleContext.require(org.pangrova.planner.SqlCompiler.class).queryResultCache();
        if (metadataDataSource != null) {
            artifactBuildStore = new org.pangrova.metadata.artifact.jdbc.build.JdbcArtifactBuildStore(
                    metadataDataSource);
        }
        embeddingModelRegistry = runtimeWiring.createEmbeddingModelRegistry();
        var materializationWiring = new QueryMaterializationWiring(
                new QueryMaterializationWiring.Dependencies(
                        config, moduleWiring, metadataDataSource, artifactBuildStore,
                        workerManager, taskScheduler, splitAssignmentManager,
                        aiInfrastructureStores, accessControl, embeddingModelRegistry));
        generatedValueMaterialization = materializationWiring.createGeneratedValue(catalog);
        materializationWiring.bindGeneratedValueReads(moduleContext, catalog);
        org.pangrova.metadata.semantic.SemanticViewStore semanticViewStore =
                aiInfrastructureStores == null ? null
                        : aiInfrastructureStores.semanticViews();
        var featureViewService =
                materializationWiring.createFeatureView(catalog);
        executionWiring = new QueryExecutionBootstrap(config, tracingProvider, accessControl,
                metadataBootstrap, moduleWiring, streamingBootstrap, coordinationService,
                writeCommitTracker, generatedValueMaterialization, semanticViewStore,
                featureViewService, embeddingModelRegistry,
                durableRecovery::resolveShuffleOwner, metadataDataSource);
        queryDispatcher = executionWiring.buildQueryDispatcher(workerManager, taskTracker,
                moduleContext, resolveCircuitBreaker(), controlPlaneResultService,
                splitAssignmentManager, speculativeStageHandler, stateJournal);
        this.remoteTaskDispatcher = executionWiring.remoteTaskDispatcher();
        if (speculativeTaskLauncher != null) {
            speculativeTaskLauncher.setDispatchDependencies(
                    remoteTaskDispatcher, workerManager, taskTracker);
            speculativeTaskLauncher.setCancelRetrier(queryDispatcher.cancelRetrier());
            taskTracker.addExecutionDrainListener(speculativeTaskLauncher::executionDrained);
        }
        var queryManagerResult = runtimeWiring.buildQueryManager(queryDispatcher, stateJournal);
        queryManager = queryManagerResult.queryManager();
        asyncSubmitExecutor = queryManagerResult.asyncExecutor();
        durableRecovery.installShuffleCleanup(queryManager, queryDispatcher);
        auditEmitter = queryManagerResult.auditEmitter();
        taskStatusReceiver = buildTaskStatusReceiver();
        taskTracker.setTaskFailureCallback((queryId, taskId) -> {
            var request = taskTracker.getTaskRequest(taskId);
            if (request != null && isArtifactTask(request.taskRequest().getTaskKind())) {
                return;
            }
            queryManager.onTaskFailed(queryId, new org.pangrova.protocol.rest.error.ErrorDetail(
                    "TASK_FAILED", taskId, "Task " + taskId + " failed"));
        });
        startTimeoutEnforcer();
        terminalCleanupExecutor = ManagedExecutorFactory.newBoundedThreadPool(
                "coordinator-terminal-cleanup", 2, 32);
        shuffleCleanupExecutor = ManagedExecutorFactory.newBoundedThreadPool(
                "coordinator-shuffle-cleanup", 4, 64);
        queryManager.startPeriodicCleanup(schedulerExecutor(), terminalCleanupExecutor,
                java.time.Duration.ofSeconds(config.queryRetentionSeconds()),
                java.time.Duration.ofSeconds(config.queryCleanupIntervalSeconds()));
        startSpeculationScheduler();
        selfIntelligence = new SelfIntelligenceBootstrap(
                config, moduleWiring, schedulerExecutor(), embeddingModelRegistry);
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
        LOG.info("event=query_bootstrap_complete speculation={} pullMode={} embedding={}",
                config.speculationEnabled(), config.pullModeSplitAssignmentEnabled(),
                config.embeddingEnabled());
    }

    private static boolean isArtifactTask(org.pangrova.protocol.v2.TaskKind kind) {
        return kind == org.pangrova.protocol.v2.TaskKind.TASK_KIND_BACKGROUND_BUILD
                || kind == org.pangrova.protocol.v2.TaskKind.TASK_KIND_BACKGROUND_EXPORT;
    }

    void shutdown() {
        shutdownOwnership.close();
    }

    void sealAdmission() { shutdownOwnership.sealAdmission(); }
    void drainQueries() { shutdownOwnership.drainQueries(); }
    void joinSchedulingOwners() { shutdownOwnership.joinSchedulingOwners(); }
    boolean isDrained() { return shutdownOwnership.isDrained(); }

    private void drainSubmissions() {
        sealAdmission();
        if (artifactPoll != null) artifactPoll.cancel(false);
        if (selfIntelligence != null) selfIntelligence.stopScheduling();
        if (asyncSubmitExecutor != null) {
            org.pangrova.common.concurrent.ExecutorDrain.shutdownAndAwait(
                    asyncSubmitExecutor, EXECUTOR_DRAIN_TIMEOUT_MILLIS);
        }
        if (artifactBuildDispatcher != null) artifactBuildDispatcher.close();
    }

    private void joinCallbacks() {
        durableRecovery.close();
        if (queryManager != null) queryManager.stopPeriodicCleanup();
        CoordinatorResourceRelease.close(null,
                () -> { if (speculationScheduler != null) speculationScheduler.close(); },
                () -> CoordinatorShutdownSequence.stopTimeoutEnforcer(queryTimeoutEnforcer),
                () -> { if (splitAssignmentManager != null) splitAssignmentManager.stopLeaseReclamation(); });
        CoordinatorResourceRelease.close(null,
                () -> {
                    if (terminalCleanupExecutor != null) {
                        org.pangrova.common.concurrent.ExecutorDrain.shutdownAndAwait(
                                terminalCleanupExecutor, EXECUTOR_DRAIN_TIMEOUT_MILLIS);
                    }
                },
                () -> {
                    if (shuffleCleanupExecutor != null) {
                        org.pangrova.common.concurrent.ExecutorDrain.shutdownAndAwait(
                                shuffleCleanupExecutor, EXECUTOR_DRAIN_TIMEOUT_MILLIS);
                    }
                },
                () -> CoordinatorShutdownSequence.shutdownSchedulerExecutor(schedulerExecutor));
    }

    private void closeQueryResources(
            org.pangrova.coordinator.bootstrap.lifecycle.CoordinatorLifecycleAdmission progress) {
        progress.step("audit", () -> CoordinatorShutdownSequence.closeAuditEmitter(auditEmitter));
        progress.step("control-transport", () ->
                CoordinatorShutdownSequence.stopCoordinatorTransport(controlPlaneResultServer));
        progress.step("control-buffer", () -> {
            if (controlPlaneResultBuffer != null) controlPlaneResultBuffer.close();
        });
        progress.step("transport-allocator", () ->
                CoordinatorShutdownSequence.closeTransportAllocator(controlPlaneResultAllocator));
        progress.step("result-cache", () -> CoordinatorShutdownSequence.closeQueryResultCache(queryResultCache));
    }

    /** Completes post-start HA wiring once the elected epoch service exists. */
    void wireHighAvailability(HighAvailabilityCoordinationService service) {
        if (service == null) {
            throw new IllegalArgumentException("HA coordination service must not be null");
        }
        durableRecovery.wireHighAvailability(service, this::schedulerExecutor,
                shuffleCleanupExecutor,
                config.workerHealthCheckIntervalMs());
        // Query and durable artifact scheduling are active-active. The elected
        // coordinator owns only singleton control loops; it is never a gateway
        // for task data or a prerequisite for another coordinator to dispatch.
        // Every coordinator stamps work with the current cluster control term,
        // and Workers fence stale terms at admission.
        java.util.function.LongSupplier activeControlTerm = service::getCurrentEpoch;
        remoteTaskDispatcher.setControlEpochSupplier(activeControlTerm);
        remoteTaskDispatcher.setExecutionOwnerResolver(executionId -> {
            var anchor = service.requireExecutionAnchor(executionId);
            return new org.pangrova.protocol.rest.task.ExecutionOwnerIdentity(
                    anchor.ownerId(), anchor.ownerIncarnation(),
                    anchor.fencingToken());
        });
        var fencer = WriteCommitWiring.createFencer(service);
        if (fencer != null) {
            taskStatusReceiver.setWriteCommitFencer(fencer);
        }
        if (artifactBuildStore == null || artifactBuildDispatcher != null) {
            return;
        }
        artifactBuildDispatcher = new org.pangrova.coordinator.artifact.DurableArtifactBuildDispatcher(
                new org.pangrova.coordinator.artifact.DurableArtifactBuildDispatcher.Dependencies(
                        artifactBuildStore, taskScheduler, workerManager, remoteTaskDispatcher,
                        taskTracker, config.coordinatorId(), activeControlTerm,
                        new org.pangrova.coordinator.artifact.ArtifactExecutionOwnership(
                                service, config.coordinatorId()),
                        new org.pangrova.metadata.artifact.jdbc.build.JdbcArtifactStageExecutionStore(
                                metadataDataSource),
                        new org.pangrova.metadata.artifact.jdbc.JdbcArtifactStageExchangeStore(metadataDataSource),
                        queryDispatcher.dagEngine()),
                new org.pangrova.coordinator.artifact.DurableArtifactBuildDispatcher.Settings(
                        java.time.Duration.ofMinutes(2), java.time.Duration.ofSeconds(5), 32));
        artifactPoll = schedulerExecutor().scheduleWithFixedDelay(() -> {
            try {
                artifactBuildDispatcher.pollOnce();
            } catch (RuntimeException failure) {
                LOG.error("event=artifact_dispatch_poll_failed error={}",
                        failure.getMessage(), failure);
            }
        }, 0, 1, java.util.concurrent.TimeUnit.SECONDS);
        LOG.info("event=artifact_dispatcher_started coordinatorId={} maxInFlight={}",
                config.coordinatorId(), 32);
    }

    /** Starts the bounded DDL/SHOW/DML acknowledgement transport, never query rows. */
    private CoordinatorResultService startControlPlaneResultTransport() {
        var started = ControlPlaneResultTransportBootstrap.start(
                config, tracingProvider, schedulerExecutor());
        controlPlaneResultServer = started.server();
        controlPlaneResultAllocator = started.allocator();
        controlPlaneResultBuffer = started.buffer();
        return started.service();
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
        splitAssignmentManager = new SplitAssignmentManager(
                SPLIT_ASSIGNMENT_LEASE_MILLIS, cacheAffinityAssigner);
        splitAssignmentManager.startLeaseReclamation(SPLIT_RECLAMATION_INTERVAL_MILLIS);
        LOG.info("event=split_assignment_initialized cacheAffinity=enabled pullMode=true");
    }

    private void initSpeculation() {
        if (!config.speculationEnabled()) {
            return;
        }
        speculativeTaskLauncher = new SpeculativeTaskLauncher();
        var speculationConfig = new SpeculationConfig(true,
                SpeculationConfig.DEFAULT_DETECTION_INTERVAL_MS, true, true,
                SpeculationConfig.DEFAULT_DATA_SKEW_MULTIPLIER, java.util.Map.of(),
                SpeculationConfig.DEFAULT_MEDIAN_MULTIPLIER,
                config.speculationSingleTaskDurationThresholdMs());
        speculativeStageHandler = new SpeculativeStageHandler(
                speculationConfig, new StragglerDetector(speculationConfig),
                speculativeTaskLauncher, taskTracker);
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
        writeCommitJournal = WriteCommitWiring.createJournal(metadataDataSource);
        writeCommitRecoveryService = WriteCommitWiring.createRecoveryService(
                writeCommitJournal, catalog);
        TaskStatusReceiver receiver = createTaskStatusReceiver(
                catalog, engine, writeCommitJournal);
        org.pangrova.coordinator.query.task.write.WriteCommitFencer fencer =
                WriteCommitWiring.createFencer(coordinationService);
        if (fencer != null) {
            receiver.setWriteCommitFencer(fencer);
        }
        return receiver;
    }

    java.util.Set<String> recoverWriteCommitIntents(
            java.util.List<org.pangrova.spi.coordination.ExecutionAnchor> claimed,
            HighAvailabilityCoordinationService service) {
        return durableRecovery.recover(claimed, service, writeCommitRecoveryService);
    }

    /**
     * Chooses the receiver wiring: the speculation-aware variant additionally carries the
     * duplicate-task resolver and remote dispatcher needed to launch a straggler's duplicate and
     * cancel the loser.
     */
    private TaskStatusReceiver createTaskStatusReceiver(
            CatalogRegistry catalog, StageCompletionCallback stageCallback,
            org.pangrova.spi.coordination.WriteCommitJournal journal) {
        boolean speculative = config.speculationEnabled() && speculativeTaskLauncher != null;
        org.pangrova.coordinator.query.task.report.ArtifactWorkReportSink artifactSink =
                artifactBuildStore == null
                        ? org.pangrova.coordinator.query.task.report.ArtifactWorkReportSink.rejecting()
                        : new org.pangrova.coordinator.query.task.report.DurableArtifactWorkReportSink(
                                artifactBuildStore,
                                new org.pangrova.metadata.artifact.jdbc.build.JdbcArtifactStageExecutionStore(
                                        metadataDataSource));
        return new TaskStatusReceiver(new TaskStatusReceiverContext(
                queryManager, taskTracker, workerManager,
                speculative ? speculativeTaskLauncher : null,
                speculative ? new DuplicateTaskResolver() : null,
                speculative ? remoteTaskDispatcher : null,
                queryManager.getStateJournal(), catalog, writeCommitTracker, stageCallback,
                moduleWiring != null ? moduleWiring.getLineageEmitter() : null,
                journal, artifactSink));
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
    org.pangrova.spi.coordination.WriteCommitJournal writeCommitJournal() { return writeCommitJournal; }
    QueryManager queryManager() { return queryManager; }
    org.pangrova.observability.metrics.CoordinatorMetrics coordinatorMetrics() { return coordinatorMetrics; }
    TaskStatusReceiver taskStatusReceiver() { return taskStatusReceiver; }
    CoordinatorTaskTracker taskTracker() { return taskTracker; }
    CircuitBreakerPolicy circuitBreaker() { return circuitBreaker; }
    /** Exposed for lifecycle tests; this server only carries control-plane statements. */
    TransportServerLifecycle coordinatorTransportServer() { return controlPlaneResultServer; }
    SplitAssignmentManager splitAssignmentManager() { return splitAssignmentManager; }
    CacheAffinityAssigner cacheAffinityAssigner() { return cacheAffinityAssigner; }
    QueryDispatcher queryDispatcher() { return queryDispatcher; }
    org.pangrova.coordinator.query.task.write.NativeWriteCommitTracker writeCommitTracker() {
        return writeCommitTracker;
    }
}
