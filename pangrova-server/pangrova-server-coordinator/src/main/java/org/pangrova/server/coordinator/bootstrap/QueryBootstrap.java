package org.pangrova.server.coordinator.bootstrap;

import org.pangrova.persistence.coordination.postgresql.admission.JdbcClusterConcurrencySlots;
import org.pangrova.scheduler.admission.TenantQuotaAdmissionController;
import org.pangrova.common.lifecycle.ResourceCloser;
import org.pangrova.generation.api.constants.GenerationOutputTypeIds;

import org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory;

import org.pangrova.coordinator.query.scheduling.CoordinatorSpeculativeWorkerInventory;
import org.pangrova.coordinator.query.scheduling.CoordinatorSpeculativeQueryInventory;
import org.pangrova.coordinator.query.scheduling.CoordinatorSpeculativeTaskRegistry;
import org.pangrova.coordinator.query.scheduling.CoordinatorSpeculativeAttemptCanceller;
import org.pangrova.coordinator.query.scheduling.CoordinatorSpeculativeAttemptDispatcher;
import org.pangrova.persistence.workflow.postgresql.generation.PostgresGenerationStores;
import org.pangrova.persistence.jdbc.api.security.model.CurrentModelAuthority;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ExecutorService;

import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.coordinator.indexing.IndexAwareSplitFetcher;
import org.pangrova.planner.analysis.SqlCompiler;
import org.pangrova.coordinator.coordination.HighAvailabilityCoordinationService;
import org.pangrova.coordinator.metadata.MetadataBootstrap;
import org.pangrova.query.runtime.QueryManager;
import org.pangrova.query.lifecycle.QueryTimeoutScheduler;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.scheduler.placement.CacheAffinityAssigner;
import org.pangrova.coordinator.query.execution.dispatch.dag.DAGDispatchEngine;
import org.pangrova.query.api.stage.StageCompletionCallback;
import org.pangrova.coordinator.query.task.TaskStatusReceiver;
import org.pangrova.coordinator.query.task.TaskStatusReceiverContext;
import org.pangrova.coordinator.result.CoordinatorResultService;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.scheduler.placement.TaskScheduler;
import org.pangrova.scheduler.api.retry.TaskRetryAdmission;
import org.pangrova.scheduler.speculation.DuplicateTaskResolver;
import org.pangrova.scheduler.speculation.SpeculationConfig;
import org.pangrova.scheduler.speculation.SpeculativeStageHandler;
import org.pangrova.scheduler.speculation.SpeculativeTaskLauncher;
import org.pangrova.scheduler.speculation.StragglerDetector;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.security.audit.AuditEventEmitter;
import org.pangrova.server.coordinator.bootstrap.query.audit.QueryAuditServices;
import org.pangrova.security.api.authorization.AccessControl;
import org.pangrova.persistence.coordination.api.membership.WorkerRegistry;
import org.pangrova.observability.api.metrics.MetricsProvider;
import org.pangrova.observability.api.tracing.TracingProvider;
import org.pangrova.protocol.transport.TransportServerLifecycle;
import org.apache.arrow.memory.BufferAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Bootstraps the query subsystem via modular configuration. */
final class QueryBootstrap {

    private static final Logger LOG = LoggerFactory.getLogger(QueryBootstrap.class);
    private static final long EXECUTOR_DRAIN_TIMEOUT_MILLIS = 5_000;

    private final CoordinatorBootstrapConfig config;
    private final TracingProvider tracingProvider;
    private final MetricsProvider metricsProvider;
    private final AccessControl accessControl;
    private final MetadataBootstrap metadataBootstrap;
    private final ModuleIntegrationBootstrap moduleWiring;
    private final javax.sql.DataSource metadataDataSource;
    private final StreamingBootstrap streamingBootstrap;
    private final HighAvailabilityCoordinationService coordinationService;
    private final QueryDurableRecovery durableRecovery;
    private final org.pangrova.security.api.secret.SecretResolver secretResolver;
    private final org.pangrova.persistence.ai.postgresql.assembly.AiInfrastructureStores aiInfrastructureStores;
    private final CurrentModelAuthority modelAuthority;
    private final PostgresGenerationStores artifactStores;

    private WorkerManager workerManager;
    private QueryManager queryManager;
    private final QueryAuditServices queryAuditServices;
    private java.util.concurrent.ExecutorService asyncSubmitExecutor;
    private ScheduledExecutorService submitDeadlines;
    private TaskStatusReceiver taskStatusReceiver;
    private CoordinatorTaskTracker taskTracker;
    private AuditEventEmitter auditEmitter;
    private QueryTimeoutScheduler queryTimeoutScheduler;
    private CircuitBreakerPolicy circuitBreaker;
    private TransportServerLifecycle controlPlaneResultServer;
    private CoordinatorResultService controlPlaneResultService;
    private BufferAllocator controlPlaneResultAllocator;
    private org.pangrova.coordinator.result.CoordinatorResultBuffer controlPlaneResultBuffer;
    private ScheduledExecutorService schedulerExecutor;
    private ExecutorService terminalCleanupExecutor;
    private ExecutorService readerPinRenewalExecutor;
    private ExecutorService shuffleCleanupExecutor;
    private RecoveredTaskDrainPoll recoveredTaskDrains;
    private CacheAffinityAssigner cacheAffinityAssigner;
    private SpeculativeStageHandler speculativeStageHandler;
    private SpeculativeTaskLauncher speculativeTaskLauncher;
    private org.pangrova.scheduler.speculation.SpeculativeExecutionScheduler speculationScheduler;
    private RemoteTaskDispatcher remoteTaskDispatcher;
    private TaskScheduler taskScheduler;
    private QuerySchedulingRuntime queryScheduling;
    private org.pangrova.persistence.generation.api.job.GenerationJobStore artifactBuildStore;
    private org.pangrova.coordinator.ai.embedding.GeneratedValueMaterializationCoordinator
            generatedValueMaterialization;
    private org.pangrova.ai.control.embedding.EmbeddingModelRegistry
            embeddingModelRegistry;
    private org.pangrova.coordinator.generation.DurableGenerationDispatcher
            artifactBuildDispatcher;
    private org.pangrova.coordinator.materialization.MaterializationMaintenance
            materializationMaintenance;
    private org.pangrova.observability.sdk.metrics.CoordinatorMetrics coordinatorMetrics;
    private QueryDispatcher queryDispatcher;
    private org.pangrova.persistence.query.api.state.QueryStateJournal stateJournal;
    private CoordinatorSpeculativeTaskRegistry speculativeTaskRegistry;
    private QueryExecutionBootstrap executionWiring;

    QueryExecutionBootstrap executionWiring() {
        return executionWiring;
    }
    private org.pangrova.coordinator.query.task.write.NativeWriteCommitTracker writeCommitTracker;
    private org.pangrova.persistence.query.api.commit.WriteCommitJournal writeCommitJournal;
    private org.pangrova.coordinator.query.task.write.WriteCommitRecoveryService
            writeCommitRecoveryService;
    private SelfIntelligenceBootstrap selfIntelligence;
    private volatile java.util.concurrent.ScheduledFuture<?> artifactPoll;
    private final org.pangrova.server.coordinator.bootstrap.lifecycle.QueryShutdownOwnership shutdownOwnership =
            new org.pangrova.server.coordinator.bootstrap.lifecycle.QueryShutdownOwnership(
                    () -> queryManager, () -> stateJournal, this::drainSubmissions,
                    this::joinCallbacks, this::closeQueryResources);

    QueryBootstrap(CoordinatorBootstrapConfig config, TracingProvider tracingProvider,
                   MetricsProvider metricsProvider, AccessControl accessControl,
                   MetadataBootstrap metadataBootstrap, ModuleIntegrationBootstrap moduleWiring,
                   javax.sql.DataSource metadataDataSource, CurrentModelAuthority modelAuthority) {
        this(config, tracingProvider, metricsProvider, accessControl,
                metadataBootstrap, moduleWiring, metadataDataSource, null, null, null, null, modelAuthority);
    }

    // The bootstrap assembles these lifecycle owners exactly once at process startup.
    @SuppressWarnings("checkstyle:ParameterNumber")
    QueryBootstrap(CoordinatorBootstrapConfig config, TracingProvider tracingProvider,
                   MetricsProvider metricsProvider, AccessControl accessControl,
                   MetadataBootstrap metadataBootstrap, ModuleIntegrationBootstrap moduleWiring,
                   javax.sql.DataSource metadataDataSource, StreamingBootstrap streamingBootstrap,
                   HighAvailabilityCoordinationService coordinationService,
                   org.pangrova.security.api.secret.SecretResolver secretResolver,
                   org.pangrova.persistence.ai.postgresql.assembly.AiInfrastructureStores aiInfrastructureStores,
                   CurrentModelAuthority modelAuthority) {
        this.config = config;
        this.tracingProvider = tracingProvider;
        this.metricsProvider = metricsProvider;
        this.accessControl = accessControl;
        this.metadataBootstrap = metadataBootstrap;
        this.moduleWiring = moduleWiring;
        this.metadataDataSource = metadataDataSource;
        // Audit must survive restarts; the in-memory store drops entries at its
        // size cap and loses everything on restart. Only dev/test setups without a
        // metadata database fall back to it.
        this.queryAuditServices = new QueryAuditServices(metadataDataSource != null
                ? new org.pangrova.persistence.query.postgresql.audit.JdbcQueryAuditStore(metadataDataSource)
                : new org.pangrova.persistence.query.memory.audit.InMemoryQueryAuditStore());
        this.streamingBootstrap = streamingBootstrap;
        this.coordinationService = coordinationService;
        durableRecovery = new QueryDurableRecovery(
                config, metadataDataSource, coordinationService);
        this.secretResolver = secretResolver;
        this.aiInfrastructureStores = aiInfrastructureStores;
        this.modelAuthority = java.util.Objects.requireNonNull(modelAuthority, "modelAuthority");
        artifactStores = new PostgresGenerationStores(modelAuthority, CoordinatorTableWriteAuthority.create());
    }

    void start() {
        CoordinatorPreflightChecks.validateJdbcConnection(metadataDataSource);
        var runtimeFactory = new QueryRuntimeFactory(new QueryRuntimeFactory.Dependencies(
                config, tracingProvider, metricsProvider, moduleWiring, metadataDataSource,
                queryAuditServices.auditService()));
        WorkerRegistry workerRegistry = runtimeFactory.createWorkerRegistry();
        workerManager = new WorkerManager(
                config.workerSuspectThresholdMs(), config.workerDeadThresholdMs(), workerRegistry,
                new org.pangrova.coordination.api.membership.WorkerShuffleDeployment(config.shuffleMode().name(),
                        config.shuffleMode() == org.pangrova.config.enums.ShuffleMode.REMOTE
                                ? config.featureFlags().shuffleProvider() : null));
        // P1-5: aggregate per-tenant usage reported by worker heartbeats into a
        // cluster-wide view (phase 1: reporting + aggregation observability).
        workerManager.setTenantUsageAggregator(
                new org.pangrova.coordinator.worker.ClusterTenantUsageAggregator());
        initCoordinatorMetrics();
        taskTracker = new CoordinatorTaskTracker();
        controlPlaneResultService = startControlPlaneResultTransport();
        cacheAffinityAssigner = new CacheAffinityAssigner();
        workerManager.addTopologyListener(cacheAffinityAssigner);
        stateJournal = runtimeFactory.createStateJournal();
        speculativeTaskRegistry = new CoordinatorSpeculativeTaskRegistry(taskTracker, stateJournal);
        initSpeculation();
        CatalogRegistry catalog = metadataBootstrap != null
                ? metadataBootstrap.catalogRegistry() : null;
        this.writeCommitTracker = catalog != null
                ? new org.pangrova.coordinator.query.task.write.NativeWriteCommitTracker() : null;
        org.pangrova.materialization.usecase.ReaderPinLeases readerPinLeases =
                createReaderPinLeases();
        SqlCompiler compiler = new SqlCompilerFactory(config, metadataBootstrap, readerPinLeases, accessControl)
                .buildSqlCompiler();
        queryScheduling = new QuerySchedulingRuntime(
                moduleWiring != null ? moduleWiring.getTenantStore() : null, config.query().placementProvider());
        taskScheduler = queryScheduling.scheduler();
        if (metadataDataSource != null) {
            artifactBuildStore = artifactStores.builds(
                    metadataDataSource);
        }
        embeddingModelRegistry = runtimeFactory.createEmbeddingModelRegistry();
        auditEmitter = runtimeFactory.createAuditEmitter();
        wireExecutionContext(compiler, createIndexedReads(moduleWiring, catalog), catalog);
        var queryManagerResult = runtimeFactory.buildQueryManager(
                queryDispatcher, stateJournal, auditEmitter,
                java.util.Optional.ofNullable(readerPinLeases)
                        .map(org.pangrova.coordinator.materialization.QueryReaderPinLifecycle::new));
        queryManager = queryManagerResult.queryManager();
        asyncSubmitExecutor = queryManagerResult.asyncExecutor();
        submitDeadlines = queryManagerResult.submitDeadlines();
        durableRecovery.installShuffleCleanup(queryManager, queryDispatcher);
        taskStatusReceiver = buildTaskStatusReceiver();
        startTimeoutScheduler();
        terminalCleanupExecutor = ManagedExecutorFactory.newBoundedThreadPool(
                "coordinator-terminal-cleanup", 2, 32);
        shuffleCleanupExecutor = ManagedExecutorFactory.newBoundedThreadPool(
                "coordinator-shuffle-cleanup", 4, 64);
        if (stateJournal instanceof org.pangrova.persistence.query.api.state.RecoveredTaskDrainJournal drainJournal) {
            recoveredTaskDrains = new RecoveredTaskDrainPoll(drainJournal, remoteTaskDispatcher::recoverTaskDrain,
                    this::releaseDrainedRecovery, shuffleCleanupExecutor, config.workerHealthCheckIntervalMs());
        }
        if (readerPinLeases != null) {
            readerPinRenewalExecutor = ManagedExecutorFactory.newBoundedThreadPool(
                    "query-reader-pin-renewal", 1, 1);
            queryManager.startReaderPinRenewal(schedulerExecutor(), readerPinRenewalExecutor);
        }
        queryManager.startPeriodicCleanup(schedulerExecutor(), terminalCleanupExecutor,
                java.time.Duration.ofSeconds(config.queryRetentionSeconds()),
                java.time.Duration.ofSeconds(config.queryCleanupIntervalSeconds()));
        startSpeculationScheduler();
        selfIntelligence = new SelfIntelligenceBootstrap(
                config, moduleWiring, schedulerExecutor(), embeddingModelRegistry);
        selfIntelligence.initialize(executionWiring);
        selfIntelligence.wireIntoQueryDispatcher(queryDispatcher);
        // P1-5b: enforce tenant budget limits across coordinators on the submission path.
        if (moduleWiring != null && moduleWiring.getTenantResolver() != null
                && moduleWiring.getQuotaChecker() != null) {
            if (metadataDataSource != null) {
                JdbcClusterConcurrencySlots.verifyAuthority(metadataDataSource);
                queryManager.enableDurableTenantCapacity();
            } else {
                queryManager.setClusterAdmissionController(new TenantQuotaAdmissionController(
                        workerManager.tenantUsageView(), null));
            }
        }
        LOG.info("event=query_bootstrap_complete speculation={} embedding={}",
                config.speculationEnabled(),
                config.embeddingEnabled());
    }

    /** Builds the execution wiring, dispatcher, and speculative dependencies. */
    private void wireExecutionContext(SqlCompiler compiler,
            IndexAwareSplitFetcher indexedReads, CatalogRegistry catalog) {
        var materializationFactory = new QueryMaterializationFactory(
                new QueryMaterializationFactory.Dependencies(
                        config, moduleWiring, metadataDataSource, artifactBuildStore,
                        workerManager, taskScheduler, cacheAffinityAssigner,
                        aiInfrastructureStores, accessControl, embeddingModelRegistry, modelAuthority));
        generatedValueMaterialization = materializationFactory.createGeneratedValue(catalog);
        var generatedValueReads = materializationFactory.createGeneratedValueReads();
        var publishedReads = artifactBuildStore == null ? null
                : new org.pangrova.generation.publication.read.PublishedGenerationBindingResolver(
                        artifactBuildStore);
        org.pangrova.semantic.api.version.SemanticViewStore semanticViewStore =
                metadataDataSource == null ? null
                        : new org.pangrova.persistence.semantic.postgresql.JdbcSemanticViewStore(
                                metadataDataSource);
        var featureViewService =
                materializationFactory.createFeatureView(catalog);
        executionWiring = new QueryExecutionBootstrap(config, tracingProvider, accessControl,
                metadataBootstrap, moduleWiring, streamingBootstrap, coordinationService,
                writeCommitTracker, generatedValueMaterialization, semanticViewStore,
                featureViewService, embeddingModelRegistry,
                durableRecovery::resolveShuffleOwner, metadataDataSource, auditEmitter, modelAuthority);
        queryDispatcher = executionWiring.buildQueryDispatcher(workerManager, taskTracker,
                compiler, taskScheduler, indexedReads, resolveCircuitBreaker(),
                        controlPlaneResultService,
                cacheAffinityAssigner, speculativeStageHandler, stateJournal, publishedReads, generatedValueReads);
        this.remoteTaskDispatcher = executionWiring.remoteTaskDispatcher();
        if (speculativeTaskLauncher != null) {
            speculativeTaskLauncher.setDispatchDependencies(
                    new CoordinatorSpeculativeAttemptDispatcher(remoteTaskDispatcher),
                    new CoordinatorSpeculativeWorkerInventory(workerManager),
                    speculativeTaskRegistry);
            speculativeTaskLauncher.setCancelRetrier(
                    new CoordinatorSpeculativeAttemptCanceller(
                            queryDispatcher.cancelRetrier()));
            taskTracker.addExecutionDrainListener(speculativeTaskLauncher::executionDrained);
        }
    }

    static IndexAwareSplitFetcher createIndexedReads(ModuleIntegrationBootstrap moduleWiring,
            CatalogRegistry catalog) {
        if (moduleWiring == null) return null;
        var indexing = moduleWiring.getIndexingBootstrap();
        if (indexing != null) {
            return new IndexAwareSplitFetcher(catalog, indexing.getSplitFilter(),
                    indexing.getMetadataStore(), indexing.getAccessorResolver());
        }
        var queryIndexing = moduleWiring.getIndexQueryBootstrap();
        return queryIndexing == null ? null : queryIndexing.createSplitFetcher(catalog);
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
        ResourceCloser.closeAll(null, durableRecovery::close,
                () -> { if (recoveredTaskDrains != null) recoveredTaskDrains.close(); });
        if (queryManager != null) queryManager.stopPeriodicCleanup();
        ResourceCloser.closeAll(null,
                () -> { if (materializationMaintenance != null) materializationMaintenance.close(); },
                () -> { if (speculationScheduler != null) speculationScheduler.close(); },
                () -> CoordinatorShutdownSequence.stopTimeoutScheduler(queryTimeoutScheduler));
        if (controlPlaneResultBuffer != null) {
            controlPlaneResultBuffer.stopExpirationScheduling();
        }
        ResourceCloser.closeAll(null,
                () -> {
                    if (readerPinRenewalExecutor != null) {
                        org.pangrova.common.concurrent.ExecutorDrain.shutdownAndAwait(
                                readerPinRenewalExecutor, EXECUTOR_DRAIN_TIMEOUT_MILLIS);
                    }
                },
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
                () -> CoordinatorShutdownSequence.shutdownSchedulerExecutor(submitDeadlines),
                () -> CoordinatorShutdownSequence.shutdownSchedulerExecutor(schedulerExecutor),
                () -> { if (queryDispatcher != null) queryDispatcher.dagEngine().close(); },
                () -> { if (queryScheduling != null) queryScheduling.close(); });
    }

    private void closeQueryResources(
            org.pangrova.server.coordinator.bootstrap.lifecycle.CoordinatorLifecycleAdmission progress) {
        progress.step("shuffle-coordinator", () -> {
            if (executionWiring != null) executionWiring.closeShuffleSession();
        });
        progress.step("audit", () -> CoordinatorShutdownSequence.closeAuditEmitter(auditEmitter));
        progress.step("control-transport", () ->
                CoordinatorShutdownSequence.stopCoordinatorTransport(controlPlaneResultServer));
        progress.step("control-buffer", () -> {
            if (controlPlaneResultBuffer != null) controlPlaneResultBuffer.close();
        });
        progress.step("transport-allocator", () ->
                CoordinatorShutdownSequence.closeTransportAllocator(controlPlaneResultAllocator));
    }

    /** Starts coordination-dependent background work after the shared authority has started. */
    void startCoordinationLoops() {
        var service = java.util.Objects.requireNonNull(coordinationService, "Query coordination authority");
        if (recoveredTaskDrains != null) recoveredTaskDrains.start(schedulerExecutor());
        java.util.function.LongSupplier activeControlTerm = service::getCurrentEpoch;
        startMaterializationMaintenance(service);
        if (artifactBuildStore == null || artifactBuildDispatcher != null) {
            return;
        }
        artifactBuildDispatcher = new org.pangrova.coordinator.generation.DurableGenerationDispatcher(
                new org.pangrova.coordinator.generation.DurableGenerationDispatcher.Dependencies(
                        artifactBuildStore, taskScheduler, workerManager, remoteTaskDispatcher,
                        taskTracker, config.coordinatorId(), activeControlTerm,
                        new org.pangrova.coordinator.generation.GenerationExecutionOwnership(
                                service, config.coordinatorId()),
                        artifactStores.stages(
                                metadataDataSource),
                        artifactStores.exchanges(metadataDataSource),
                        queryDispatcher.dagEngine(), this::validateArtifactSnapshot),
                new org.pangrova.coordinator.generation.DurableGenerationDispatcher.Settings(
                        config.artifactDispatchLease(), config.artifactDispatchRetryDelay(),
                        config.artifactDispatchMaxInFlight()),
                new OtelExecutionTraceContextFactory());
        artifactPoll = schedulerExecutor().scheduleWithFixedDelay(() -> {
            try {
                artifactBuildDispatcher.pollOnce();
            } catch (RuntimeException failure) {
                LOG.error("event=artifact_dispatch_poll_failed error={}",
                        failure.getMessage(), failure);
            }
        }, 0, 1, java.util.concurrent.TimeUnit.SECONDS);
        LOG.info("event=artifact_dispatcher_started coordinatorId={} maxInFlight={}",
                config.coordinatorId(), config.artifactDispatchMaxInFlight());
    }

    private void validateArtifactSnapshot(org.pangrova.generation.api.job.GenerationJob job) {
        var indexing = moduleWiring.getIndexingBootstrap();
        if (indexing != null) {
            indexing.validateSnapshotPin(job);
            return;
        }
        var queryIndexing = moduleWiring.getIndexQueryBootstrap();
        if (queryIndexing != null) {
            queryIndexing.validateSnapshotPin(job);
            return;
        }
        if (GenerationOutputTypeIds.VECTOR_INDEX.equals(job.artifactType())
                || GenerationOutputTypeIds.TEXT_INDEX.equals(job.artifactType())) {
            throw new IllegalStateException("Native index artifact has no snapshot lifecycle owner");
        }
    }

    private void startMaterializationMaintenance(
            HighAvailabilityCoordinationService service) {
        if (executionWiring == null || executionWiring.materializationKernels() == null
                || metadataDataSource == null || queryManager == null) {
            LOG.info("event=materialization_maintenance_unavailable coordinatorId={}",
                    config.coordinatorId());
            return;
        }
        var kernels = executionWiring.materializationKernels();
        var store = new org.pangrova.coordinator.materialization.CoordinatorPhysicalGenerationStore(
                kernels.catalogs(), new OtelExecutionTraceContextFactory());
        var recovery = new org.pangrova.coordinator.materialization.NativeCommitRecovery(
                metadataDataSource, store, kernels.catalogs());
        materializationMaintenance =
                new org.pangrova.coordinator.materialization.MaterializationMaintenance(
                        kernels, service, config.coordinatorId(), queryManager,
                        recovery,
                        new org.pangrova.coordinator.materialization.MaterializationMaintenance
                                .Dependencies(store, kernels.sourcePinning(), schedulerExecutor(),
                                        org.pangrova.common.concurrent.ManagedExecutorFactory
                                                .newSingleThreadExecutor("materialization-control"),
                                        org.pangrova.common.concurrent.ManagedExecutorFactory
                                                .newSingleThreadExecutor("materialization-refresh"),
                                        org.pangrova.coordinator.materialization
                                                .MaterializationMaintenance.Settings.defaults(
                                                        config.coordinatorId())),
                                                                new OtelExecutionTraceContextFactory());
        materializationMaintenance.start();
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

    /** Reader pins need the durable metadata authority; in-memory mode has no generations to pin. */
    private org.pangrova.materialization.usecase.ReaderPinLeases createReaderPinLeases() {
        if (metadataBootstrap == null) {
            return null;
        }
        var repository = metadataBootstrap.materializationRepository();
        return repository == null ? null
                : org.pangrova.materialization.usecase.ReaderPinLeases.forActiveQueries(
                        repository, java.time.Instant::now);
    }

    private void startTimeoutScheduler() {
        java.time.Duration timeout = java.time.Duration.ofSeconds(config.queryTimeoutSeconds());
        java.time.Duration interval = java.time.Duration.ofSeconds(config.timeoutCheckIntervalSeconds());
        queryTimeoutScheduler = new QueryTimeoutScheduler(timeout, interval);
        queryTimeoutScheduler.start(queryManager);
        LOG.info("event=timeout_enforcer_started timeout={}s interval={}s",
                config.queryTimeoutSeconds(), config.timeoutCheckIntervalSeconds());
    }

    private void initSpeculation() {
        // The launcher also dispatches worker-loss task retries, so it exists even
        // when straggler speculation is disabled; only the detector stays gated.
        speculativeTaskLauncher = new SpeculativeTaskLauncher();
        if (!config.speculationEnabled()) {
            return;
        }
        var speculationConfig = new SpeculationConfig(true,
                SpeculationConfig.DEFAULT_DETECTION_INTERVAL_MS, true, true,
                SpeculationConfig.DEFAULT_DATA_SKEW_MULTIPLIER, java.util.Map.of(),
                SpeculationConfig.DEFAULT_MEDIAN_MULTIPLIER,
                config.speculationSingleTaskDurationThresholdMs());
        speculativeStageHandler = new SpeculativeStageHandler(
                speculationConfig, new StragglerDetector(speculationConfig),
                speculativeTaskLauncher,
                speculativeTaskRegistry);
        LOG.info("event=speculation_initialized");
    }

    private void startSpeculationScheduler() {
        if (speculativeStageHandler == null || !speculativeStageHandler.isEnabled()) {
            return;
        }
        speculationScheduler = new org.pangrova.scheduler.speculation.SpeculativeExecutionScheduler(
                speculativeStageHandler,
                new CoordinatorSpeculativeQueryInventory(queryManager),
                speculativeTaskRegistry,
                SpeculationConfig.DEFAULT_DETECTION_INTERVAL_MS);
        speculationScheduler.start();
    }

    private TaskStatusReceiver buildTaskStatusReceiver() {
        CatalogRegistry catalog = metadataBootstrap != null
                ? metadataBootstrap.catalogRegistry() : null;
        DAGDispatchEngine engine = queryDispatcher.dagEngine();
        engine.setOperatorStatsRegistry(queryAuditServices.operatorStatsRegistry());
        writeCommitJournal = WriteCommitComponentFactory.createJournal(metadataDataSource);
        writeCommitRecoveryService = WriteCommitComponentFactory.createRecoveryService(
                writeCommitJournal, catalog,
                WriteCommitComponentFactory.createQuarantineStore(metadataDataSource));
        TaskStatusReceiver receiver = createTaskStatusReceiver(
                catalog, engine, writeCommitJournal);
        if (moduleWiring != null && moduleWiring.getWriteQualityGate() != null) {
            receiver.setWriteQualityGate(moduleWiring.getWriteQualityGate());
        }
        org.pangrova.coordinator.query.task.write.WriteCommitFencer fencer =
                WriteCommitComponentFactory.createFencer(coordinationService);
        if (fencer != null) {
            receiver.setWriteCommitFencer(fencer);
        }
        return receiver;
    }

    JournaledCdcRuntime.WorkerDispatch cdcWorkerDispatch(HighAvailabilityCoordinationService service) {
        return new JournaledCdcRuntime.WorkerDispatch(service, queryDispatcher.taskDispatcher(),
                queryDispatcher.workerSelector(), taskTracker, taskStatusReceiver, stateJournal);
    }

    private boolean releaseDrainedRecovery(org.pangrova.execution.api.ownership.ExecutionAnchor claim) {
        // The same process can claim its own expired grant while local resources still exist.
        var original = queryManager.getStateMachine(claim.executionId());
        if (original != null && !original.physicalCleanupApplied()) return false;
        return durableRecovery.releaseRecoveredQuery(claim);
    }

    java.util.Set<String> observedRecoveryExecutions(java.util.Set<String> executions) {
        if (!(stateJournal instanceof org.pangrova.persistence.query.api.state.RecoveredTaskDrainJournal journal)) {
            throw new IllegalStateException("Durable recovery requires the shared task journal");
        }
        return journal.observedRecoveryExecutions(executions);
    }

    void trackRecoveredTaskDrains(java.util.List<org.pangrova.execution.api.ownership.ExecutionAnchor> claims) {
        if (recoveredTaskDrains != null) recoveredTaskDrains.track(claims);
    }

    java.util.Set<String> recoverWriteCommitIntents(
            java.util.List<org.pangrova.execution.api.ownership.ExecutionAnchor> claimed) {
        return durableRecovery.recover(claimed, writeCommitRecoveryService);
    }

    /**
     * Chooses the receiver wiring: the speculation-aware variant additionally carries the
     * duplicate-task resolver and remote dispatcher needed to launch a straggler's duplicate and
     * cancel the loser.
     */
    private TaskStatusReceiver createTaskStatusReceiver(
            CatalogRegistry catalog, StageCompletionCallback stageCallback,
            org.pangrova.persistence.query.api.commit.WriteCommitJournal journal) {
        boolean speculative = config.speculationEnabled() && speculativeTaskLauncher != null;
        org.pangrova.coordinator.query.task.report.GenerationWorkReportSink artifactSink =
                artifactBuildStore == null
                        ? org.pangrova.coordinator.query.task.report.GenerationWorkReportSink.rejecting()
                        : new org.pangrova.coordinator.query.task.report.DurableGenerationWorkReportSink(
                                artifactBuildStore,
                                artifactStores.stages(
                                        metadataDataSource));
        TaskStatusReceiver receiver = new TaskStatusReceiver(new TaskStatusReceiverContext(
                queryManager, taskTracker, workerManager,
                speculative ? speculativeTaskLauncher : null,
                speculative ? new DuplicateTaskResolver() : null,
                speculative ? remoteTaskDispatcher : null,
                queryManager.getStateJournal(), catalog, writeCommitTracker, stageCallback,
                moduleWiring != null ? moduleWiring.getLineageEmitter() : null,
                journal, artifactSink));
        receiver.setOperatorStatsRegistry(queryAuditServices.operatorStatsRegistry());
        return receiver;
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
        coordinatorMetrics = new org.pangrova.observability.sdk.metrics.CoordinatorMetrics(
                metricsProvider, () -> (double) workerManager.getActiveWorkers().size());
        LOG.info("event=coordinator_metrics_initialized");
    }

    WorkerManager workerManager() { return workerManager; }
    AuditEventEmitter auditEmitter() { return auditEmitter; }
    org.pangrova.persistence.query.api.commit.WriteCommitJournal writeCommitJournal() { return writeCommitJournal; }
    QueryManager queryManager() { return queryManager; }
    QueryAuditServices queryAuditServices() { return queryAuditServices; }
    org.pangrova.observability.sdk.metrics.CoordinatorMetrics coordinatorMetrics() { return coordinatorMetrics; }
    TaskStatusReceiver taskStatusReceiver() { return taskStatusReceiver; }
    CoordinatorTaskTracker taskTracker() { return taskTracker; }
    CircuitBreakerPolicy circuitBreaker() { return circuitBreaker; }
    /** Exposed for lifecycle tests; this server only carries control-plane statements. */
    TransportServerLifecycle coordinatorTransportServer() { return controlPlaneResultServer; }
    CacheAffinityAssigner cacheAffinityAssigner() { return cacheAffinityAssigner; }
    QueryDispatcher queryDispatcher() { return queryDispatcher; }

    /**
     * Retries one task whose worker was declared dead. The replacement identity is
     * admitted onto its Stage before dispatch, so a late report from the dead attempt
     * can neither complete nor fail the Stage.
     *
     * @return true when a replacement was dispatched; false when no retry is possible
     */
    boolean retryTaskOnDeadWorker(String queryId, String taskId, String deadWorkerId) {
        if (speculativeTaskLauncher == null || queryDispatcher == null) {
            return false;
        }
        var engine = queryDispatcher.dagEngine();
        int maxRetries = config.query().workerMaxRetry();
        try {
            String retryId = speculativeTaskLauncher.retryFailedTask(
                    queryId, taskId, deadWorkerId,
                    new TaskRetryAdmission() {
                        @Override
                        public boolean reserve(String candidateId) {
                            return engine.tryPrepareTaskRetry(
                                    queryId, taskId, candidateId, maxRetries);
                        }

                        @Override
                        public void rollback(String candidateId) {
                            engine.rollbackTaskRetry(queryId, taskId, candidateId);
                        }
                    });
            return retryId != null;
        } catch (RuntimeException failure) {
            LOG.warn("event=task_retry_dispatch_failed queryId={} taskId={} workerId={} error={}",
                    queryId, taskId, deadWorkerId, failure.getMessage());
            return false;
        }
    }
    org.pangrova.coordinator.query.task.write.NativeWriteCommitTracker writeCommitTracker() {
        return writeCommitTracker;
    }
}
