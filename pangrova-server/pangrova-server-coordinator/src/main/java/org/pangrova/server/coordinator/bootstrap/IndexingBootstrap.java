package org.pangrova.server.coordinator.bootstrap;

import org.pangrova.persistence.generation.api.publication.GenerationPublicationStore;

import org.pangrova.persistence.generation.api.job.GenerationJobStore;

import org.pangrova.coordinator.indexing.ddl.IndexDdlExecutor;
import org.pangrova.indexing.evaluation.IndexEvalMetrics;
import org.pangrova.indexing.pruning.IndexEvalConfig;
import org.pangrova.indexing.pruning.IndexSplitFilter;
import org.pangrova.coordinator.indexing.connector.ConnectorAccessorResolver;
import org.pangrova.coordinator.indexing.ddl.IndexDdlContext;
import org.pangrova.coordinator.indexing.lifecycle.IndexCommitContext;
import org.pangrova.coordinator.indexing.lifecycle.IndexCommitHandler;
import org.pangrova.coordinator.indexing.lifecycle.IndexConsistencyChecker;
import org.pangrova.coordinator.indexing.lifecycle.IndexConsistencyContext;
import org.pangrova.coordinator.indexing.lifecycle.gc.IndexGarbageCollector;
import org.pangrova.coordinator.indexing.lifecycle.gc.IndexGcContext;
import org.pangrova.coordinator.indexing.lifecycle.IndexLifecycleMetrics;
import org.pangrova.coordinator.indexing.lifecycle.MaintenanceLeaderElection;
import org.pangrova.persistence.indexing.api.build.IndexBuildClaimRepository;
import org.pangrova.coordinator.indexing.lifecycle.MetadataBackedGenerationChecker;
import org.pangrova.coordinator.indexing.lifecycle.StuckBuildRecovery;
import org.pangrova.coordinator.generation.DurableGenerationPublisher;
import org.pangrova.indexing.strategy.IndexEngineRegistry;
import org.pangrova.indexing.build.IndexBuildCoordinator;
import org.pangrova.indexing.build.IndexBuildContext;
import org.pangrova.indexing.config.IndexingConfig;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.evaluation.IndexEvaluationService;
import org.pangrova.indexing.build.IndexBuildMetrics;
import org.pangrova.indexing.codec.data.IndexSerializer;
import org.pangrova.indexing.codec.data.TypeSerializerRegistry;
import org.pangrova.indexing.storage.IndexCache;
import org.pangrova.indexing.storage.IndexStorageManager;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.persistence.indexing.api.metadata.IndexMetadataStore;
import org.pangrova.persistence.indexing.api.metadata.CacheEvent;
import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.common.lifecycle.ResourceCloser;
import org.pangrova.persistence.coordination.api.lease.DistributedLease;
import org.pangrova.coordination.api.leadership.MaintenanceLeaderAware;
import org.pangrova.observability.api.metrics.MetricsProvider;
import org.pangrova.security.api.authorization.AccessControl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.pangrova.generation.api.constants.GenerationOutputTypeIds;
import org.pangrova.generation.publication.GenerationPublicationRouter;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Bootstraps all indexing subsystem components in dependency order.
 *
 * <p>Initialization: IndexEngineRegistry → IndexCache → IndexStorageManager →
 * IndexBuildCoordinator → IndexEvaluationService → IndexSplitFilter →
 * StuckBuildRecovery → GC → ConsistencyChecker → DDL executor registration.</p>
 *
 * <p>The coordinator performs L0 (min/max metadata) and L1 (native connector)
 * pruning only. L2 evaluation (actual index data loading) is delegated to
 * workers via {@link org.pangrova.indexing.api.evaluation.IndexEvalHint} objects
 * attached to splits.</p>
 *
 * <p>Shutdown: LIFO order via {@link #close()}.</p>
 */
public final class IndexingBootstrap implements MaintenanceLeaderAware, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(IndexingBootstrap.class);

    // Default config values
    private static final Duration DEFAULT_BUILD_TIMEOUT = Duration.ofMinutes(30);
    private static final int DEFAULT_STUCK_DETECTION_MULTIPLIER = 2;

    // Components (ordered by initialization, shutdown is reverse)
    private IndexEngineRegistry engineRegistry;
    private IndexCache indexCache;
    private IndexStorageManager storageManager;
    private IndexBuildCoordinator buildCoordinator;
    private IndexEvaluationService evaluationService;
    private IndexSplitFilter splitFilter;
    private IndexCommitHandler commitHandler;
    private IndexGarbageCollector garbageCollector;
    private IndexConsistencyChecker consistencyChecker;
    private IndexDdlExecutor ddlExecutor;
    private org.pangrova.indexing.evolution.IndexSchemaEvolutionHandler schemaEvolutionHandler;
    private ScheduledExecutorService schemaEvolutionScheduler;
    private IndexMetadataStore metadataStore;
    private ConnectorAccessorResolver accessorResolver;
    private volatile java.util.Set<String> commitListenerCatalogs = java.util.Set.of();

    // Stuck build recovery (periodic)
    private StuckBuildRecovery stuckBuildRecovery;
    private ScheduledExecutorService stuckBuildRecoveryScheduler;
    private SnapshotPinRecoveryScheduler snapshotPinRecoveryScheduler;
    private org.pangrova.coordinator.indexing.ddl.DeferredSnapshotPinProvider scalarSnapshotPins;
    private org.pangrova.coordinator.indexing.ddl.DurableIndexBuildService nativeIndexBuildService;

    // Cache invalidation polling
    private ScheduledExecutorService cacheInvalidationScheduler;

    // Metrics (nullable — only created when MetricsProvider available)
    private IndexBuildMetrics buildMetrics;
    private IndexEvalMetrics evalMetrics;
    private IndexLifecycleMetrics lifecycleMetrics;

    private final IndexingConfig indexingConfig;
    private final DistributedLease distributedLease;
    private final GenerationJobStore artifactBuildStore;
    private final GenerationPublicationStore artifactPublicationStore;
    private final MaintenanceLeaderElection configuredLeaderElection;
    private MaintenanceLeaderElection leaderElection;
    private DurableGenerationPublisher artifactPublisher;
    private org.pangrova.coordinator.generation.DurableGenerationPublisher.PollSettings artifactPublicationSettings =
            org.pangrova.coordinator.generation.DurableGenerationPublisher.defaultSettings();

    /**
     * Creates an IndexingBootstrap with an externally-configured base index path
     * and a distributed lease backend for maintenance leader election.
     *
     * @param baseIndexPath    root path for index file storage (must not be null or blank)
     * @param distributedLease lease backend for leader election (must not be null)
     */
    IndexingBootstrap(String baseIndexPath, DistributedLease distributedLease) {
        this(baseIndexPath, distributedLease, null, null,
                (MaintenanceLeaderElection) null, null);
    }

    IndexingBootstrap(String baseIndexPath, DistributedLease distributedLease,
                      GenerationJobStore artifactBuildStore) {
        this(baseIndexPath, distributedLease, artifactBuildStore, null,
                (MaintenanceLeaderElection) null, null);
    }

    public IndexingBootstrap(
            String baseIndexPath,
            DistributedLease distributedLease,
            GenerationJobStore artifactBuildStore,
            GenerationPublicationStore artifactPublicationStore) {
        this(baseIndexPath, distributedLease, artifactBuildStore,
                artifactPublicationStore, (MaintenanceLeaderElection) null, null);
    }

    public IndexingBootstrap(
            String baseIndexPath,
            DistributedLease distributedLease,
            GenerationJobStore artifactBuildStore,
            GenerationPublicationStore artifactPublicationStore,
            IndexingConfig indexingConfig) {
        this(baseIndexPath, distributedLease, artifactBuildStore,
                artifactPublicationStore, (MaintenanceLeaderElection) null, indexingConfig);
    }

    IndexingBootstrap(
            String baseIndexPath,
            DistributedLease distributedLease,
            GenerationJobStore artifactBuildStore,
            GenerationPublicationStore artifactPublicationStore,
            MaintenanceLeaderElection configuredLeaderElection) {
        this(baseIndexPath, distributedLease, artifactBuildStore,
                artifactPublicationStore, configuredLeaderElection, null);
    }

    public IndexingBootstrap(
            String baseIndexPath,
            DistributedLease distributedLease,
            GenerationJobStore artifactBuildStore,
            GenerationPublicationStore artifactPublicationStore,
            IndexingConfig indexingConfig,
            org.pangrova.coordinator.generation.DurableGenerationPublisher.PollSettings publicationSettings) {
        this(baseIndexPath, distributedLease, artifactBuildStore,
                artifactPublicationStore, (MaintenanceLeaderElection) null,
                indexingConfig, publicationSettings);
    }

    IndexingBootstrap(
            String baseIndexPath,
            DistributedLease distributedLease,
            GenerationJobStore artifactBuildStore,
            GenerationPublicationStore artifactPublicationStore,
            MaintenanceLeaderElection configuredLeaderElection,
            IndexingConfig indexingConfig) {
        this(baseIndexPath, distributedLease, artifactBuildStore,
                artifactPublicationStore, configuredLeaderElection, indexingConfig,
                org.pangrova.coordinator.generation.DurableGenerationPublisher.defaultSettings());
    }

    IndexingBootstrap(
            String baseIndexPath,
            DistributedLease distributedLease,
            GenerationJobStore artifactBuildStore,
            GenerationPublicationStore artifactPublicationStore,
            MaintenanceLeaderElection configuredLeaderElection,
            IndexingConfig indexingConfig,
            org.pangrova.coordinator.generation.DurableGenerationPublisher.PollSettings publicationSettings) {
        if (baseIndexPath == null || baseIndexPath.isBlank()) {
            throw new IllegalStateException(
                    "IndexingBootstrap requires baseIndexPath but received null or blank value");
        }
        if (distributedLease == null) {
            throw new IllegalStateException(
                    "IndexingBootstrap requires distributedLease but received null");
        }
        if (indexingConfig != null && !baseIndexPath.equals(indexingConfig.baseIndexPath())) {
            throw new IllegalArgumentException(
                    "IndexingBootstrap requires one consistent indexing.base-path");
        }
        this.indexingConfig = indexingConfig == null
                ? new IndexingConfig(baseIndexPath, true, null, null) : indexingConfig;
        this.distributedLease = distributedLease;
        this.artifactBuildStore = artifactBuildStore;
        this.artifactPublicationStore = artifactPublicationStore;
        this.artifactPublicationSettings = publicationSettings;
        this.configuredLeaderElection = configuredLeaderElection;
    }

    /**
     * Initializes all indexing components using the constructor-injected distributed lease.
     *
     * @param indexMetadata   index metadata persistence
     * @param catalogRegistry catalog registry for connector resolution
     * @param metricsProvider metrics provider (nullable)
     * @param accessControl   access control (nullable)
     */
    public void start(IndexMetadataStore indexMetadata,
               IndexBuildClaimRepository buildClaims,
               CatalogRegistry catalogRegistry,
               MetricsProvider metricsProvider,
               AccessControl accessControl) {

        if (indexMetadata == null) {
            throw new IllegalStateException(
                    "IndexingBootstrap requires metadataStore but received null");
        }
        java.util.Objects.requireNonNull(buildClaims, "buildClaims");
        if (catalogRegistry == null) {
            throw new IllegalStateException(
                    "IndexingBootstrap requires catalogRegistry but received null");
        }

        this.metadataStore = indexMetadata;
        IndexingConfig config = indexingConfig;
        IndexSerializer serializer = initializeEngine(config, metricsProvider);
        var buildServices = initializeBuildServices(config, catalogRegistry, serializer, buildClaims);
        startLifecycle(config, buildServices);
        initializeSchemaEvolutionRecovery(buildServices);
        startRecovery(buildServices);
        garbageCollector.start();
        consistencyChecker.start();
        startCacheInvalidation();
        IndexDdlContext ddlDeps = new IndexDdlContext(
                metadataStore, buildServices, catalogRegistry, accessorResolver);
        ddlExecutor = new IndexDdlExecutor(ddlDeps, indexCache, accessControl);
        // Register external callbacks only after local resources are initialized.
        if (config.commitListenerEnabled()) {
            registerCommitListeners(accessorResolver, catalogRegistry);
        } else {
            LOG.info("event=commit_listener_skipped reason=disabled_by_config");
        }
        LOG.info("event=indexing_bootstrap_completed");
    }

    private IndexSerializer initializeEngine(IndexingConfig config, MetricsProvider metricsProvider) {
        engineRegistry = new IndexEngineRegistry(IndexingBootstrap.class.getClassLoader());
        if (engineRegistry.size() == 0) {
            throw new IllegalStateException("Indexing requires at least one JVM file-pruning strategy");
        }
        LOG.info("event=indexing_phase phase=engine_registry providers={}", engineRegistry.size());
        indexCache = new IndexCache(config.cacheSizeBytes());
        storageManager = new IndexStorageManager();
        TypeSerializerRegistry serdeRegistry = TypeSerializerRegistry.fromStrategies(engineRegistry.providers());
        IndexSerializer serializer = new IndexSerializer(serdeRegistry, config.checksumEnabled());
        if (metricsProvider != null) {
            buildMetrics = new IndexBuildMetrics(metricsProvider);
            evalMetrics = new IndexEvalMetrics(metricsProvider);
            lifecycleMetrics = new IndexLifecycleMetrics(metricsProvider);
            new org.pangrova.indexing.storage.IndexCacheMetrics(metricsProvider, indexCache);
        }
        return serializer;
    }

    private org.pangrova.coordinator.indexing.ddl.IndexBuildServices initializeBuildServices(
            IndexingConfig config, CatalogRegistry catalogRegistry, IndexSerializer serializer,
            IndexBuildClaimRepository buildClaims) {
        accessorResolver = new ConnectorAccessorResolver(catalogRegistry);
        var generationChecker = new MetadataBackedGenerationChecker(metadataStore);
        scalarSnapshotPins = new org.pangrova.coordinator.indexing.ddl.DeferredSnapshotPinProvider(
                metadataStore, artifactBuildStore, accessorResolver);
        IndexBuildContext buildDeps = new IndexBuildContext(
                config, engineRegistry, storageManager, serializer,
                buildClaims, generationChecker, scalarSnapshotPins, null);
        buildCoordinator = new IndexBuildCoordinator(buildDeps, buildMetrics);

        // F-IDX-BREAKER: back the breaker with the shared metadata store so a
        // tripped circuit is observed cluster-wide (multi-coordinator Active-Active),
        // not just locally. Degrades to empty state when the metadata store is not
        // JDBC-backed (default interface methods), matching prior noOp behaviour.
        int circuitFailureThreshold = config.runtime().eval().circuitFailureThreshold();
        java.time.Duration circuitOpenDuration =
                java.time.Duration.ofSeconds(config.runtime().eval().circuitOpenDurationSeconds());
        var breakerStateStore = new org.pangrova.coordinator.indexing.MetadataBreakerStateStore(
                metadataStore, circuitFailureThreshold, circuitOpenDuration);
        var circuitBreaker = new org.pangrova.indexing.evaluation.IndexEvalCircuitBreaker(
                circuitFailureThreshold, circuitOpenDuration, breakerStateStore);
        evaluationService = new IndexEvaluationService(
                engineRegistry, indexCache, storageManager, serializer, circuitBreaker);

        var evalConfig = new IndexEvalConfig(
                evalMetrics, config.evalTimeoutMs(), config.evalParallelism());
        splitFilter = new IndexSplitFilter(metadataStore, evalConfig);

        // Scalar indexes use the Java builder; vector/BM25 artifacts use durable Native jobs.
        var aiNativeBuildService = artifactBuildStore == null ? null
                : new org.pangrova.coordinator.indexing.ddl.DurableIndexBuildService(
                        artifactBuildStore, metadataStore, catalogRegistry, indexingConfig.baseIndexPath(),
                        session -> accessorResolver.forSession(session).resolveSnapshotPinProvider());
        nativeIndexBuildService = aiNativeBuildService;
        return new org.pangrova.coordinator.indexing.ddl.IndexBuildServices(
                buildCoordinator, aiNativeBuildService);
    }

    private void startLifecycle(IndexingConfig config,
                                org.pangrova.coordinator.indexing.ddl.IndexBuildServices buildServices) {
        IndexCommitContext commitDeps = new IndexCommitContext(
                metadataStore, buildServices, accessorResolver);

        IndexGcContext gcDeps = new IndexGcContext(
                metadataStore, storageManager,
                descriptor -> org.pangrova.coordinator.indexing.ddl.IndexSourceAuthority
                        .forDescriptor(metadataStore, buildServices, accessorResolver, descriptor)
                        .resolve(descriptor.catalogName()), config,
                new org.pangrova.coordinator.indexing.ddl.DeferredSnapshotPinProvider(
                        metadataStore, artifactBuildStore, accessorResolver));

        String coordinatorId = configuredLeaderElection != null
                ? configuredLeaderElection.holderId()
                : java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
        leaderElection = configuredLeaderElection != null
                ? configuredLeaderElection
                : new MaintenanceLeaderElection(distributedLease, coordinatorId);

        if ((artifactBuildStore == null) != (artifactPublicationStore == null)) {
            throw new IllegalStateException(
                    "artifact build and publication stores must be configured together");
        }
        // Durable publication starts only after the actual Coordinator HA process is bound.

        if (config.commitListenerEnabled()) {
            int pollBatchSize = config.lifecycle().commitListener().pollBatchSize();
            commitHandler = new IndexCommitHandler(
                    commitDeps, lifecycleMetrics, leaderElection,
                    pollBatchSize);
        }

        garbageCollector = new IndexGarbageCollector(gcDeps, lifecycleMetrics, leaderElection);

        IndexConsistencyContext consistencyDeps = new IndexConsistencyContext(
                metadataStore, buildServices, accessorResolver, config, leaderElection);
        consistencyChecker = new IndexConsistencyChecker(consistencyDeps, lifecycleMetrics);
    }

    private void initializeSchemaEvolutionRecovery(
            org.pangrova.coordinator.indexing.ddl.IndexBuildServices buildServices) {
        schemaEvolutionHandler = new org.pangrova.indexing.evolution.IndexSchemaEvolutionHandler(
                metadataStore, (descriptor, event) -> observeSourceSchema(buildServices, descriptor, event));
    }

    private void startSchemaEvolutionRecovery() {
        if (schemaEvolutionHandler == null || schemaEvolutionScheduler != null) {
            return;
        }
        long epoch = leaderElection.verifiedHeldEpoch().orElseThrow(
                () -> new IllegalStateException("Schema evolution recovery requires verified maintenance leadership"));
        var scheduler = ManagedExecutorFactory.newSingleThreadScheduledExecutor("index-schema-evolution");
        schemaEvolutionScheduler = scheduler;
        scheduler.scheduleWithFixedDelay(
                () -> recoverSchemaEvolutionSafe(scheduler, epoch), 0, 1, TimeUnit.MINUTES);
    }

    private void stopSchemaEvolutionRecovery() {
        if (schemaEvolutionScheduler != null) {
            schemaEvolutionScheduler.shutdown();
            schemaEvolutionScheduler = null;
        }
    }

    private void recoverSchemaEvolutionSafe(ScheduledExecutorService scheduler, long epoch) {
        try {
            java.util.function.BooleanSupplier stillLeader =
                    () -> !scheduler.isShutdown() && leaderElection.holdsEpoch(epoch);
            if (!stillLeader.getAsBoolean()) {
                return;
            }
            int fenced = schemaEvolutionHandler.reconcile(stillLeader);
            int applied = schemaEvolutionHandler.recoverPending(stillLeader);
            if (fenced > 0 || applied > 0) {
                LOG.info("event=index_schema_evolution_recovered fenced={} applied={}", fenced, applied);
            }
        } catch (RuntimeException failure) {
            LOG.warn("event=index_schema_evolution_recovery_failed error={}", failure.getMessage(), failure);
        }
    }

    private void startRecovery(
            org.pangrova.coordinator.indexing.ddl.IndexBuildServices buildServices) {
        Duration buildTimeout = CoordinatorIndexingConfiguration.buildTimeout(indexingConfig);
        stuckBuildRecovery = new StuckBuildRecovery(
                new IndexCommitContext(metadataStore, buildServices, accessorResolver),
                buildTimeout, indexingConfig.lifecycle().consistency().buildStuckDetectionMultiplier(),
                leaderElection);
        int recovered = stuckBuildRecovery.recoverStuckBuilds();
        if (recovered > 0) {
            LOG.info("event=indexing_phase phase=stuck_recovery recovered={}", recovered);
        }

        long recoveryIntervalMinutes = buildTimeout.toMinutes() / 2;
        if (recoveryIntervalMinutes < 1) {
            recoveryIntervalMinutes = 1;
        }
        stuckBuildRecoveryScheduler = ManagedExecutorFactory
                .newSingleThreadScheduledExecutor("pangrova-stuck-build-recovery");
        stuckBuildRecoveryScheduler.scheduleWithFixedDelay(
                this::recoverStuckBuildsSafe,
                recoveryIntervalMinutes, recoveryIntervalMinutes, TimeUnit.MINUTES);
        LOG.info("event=indexing_phase phase=stuck_recovery_scheduled intervalMinutes={}",
                recoveryIntervalMinutes);
    }

    private void startSnapshotPinRecovery() {
        if ((nativeIndexBuildService == null && scalarSnapshotPins == null)
                || snapshotPinRecoveryScheduler != null) {
            return;
        }
        long epoch = leaderElection.verifiedHeldEpoch().orElseThrow(
                () -> new IllegalStateException("Snapshot pin recovery requires verified maintenance leadership"));
        var scheduler = new SnapshotPinRecoveryScheduler(
                () -> leaderElection.holdsEpoch(epoch),
                stillLeader -> {
                    if (scalarSnapshotPins != null && stillLeader.getAsBoolean()) {
                        try {
                            scalarSnapshotPins.cleanupDrainedScalarPins(stillLeader);
                        } catch (RuntimeException failure) {
                            LOG.error("event=scalar_snapshot_pin_maintenance_failed", failure);
                        }
                    }
                    if (nativeIndexBuildService != null && stillLeader.getAsBoolean()) {
                        nativeIndexBuildService.adoptLegacySnapshotPins(stillLeader);
                        if (stillLeader.getAsBoolean()) {
                            nativeIndexBuildService.recoverUnboundSnapshotPins(stillLeader);
                        }
                        if (stillLeader.getAsBoolean()) {
                            nativeIndexBuildService.fenceAbandonedNativeReservations(stillLeader);
                        }
                        if (stillLeader.getAsBoolean()) {
                            nativeIndexBuildService.cleanupSnapshotPins(stillLeader);
                        }
                    }
                });
        snapshotPinRecoveryScheduler = scheduler;
        scheduler.start();
    }

    private void stopSnapshotPinRecovery() {
        if (snapshotPinRecoveryScheduler != null) {
            snapshotPinRecoveryScheduler.close();
            snapshotPinRecoveryScheduler = null;
        }
    }

    /** Revalidates Native source pins at dispatch and publication without synthesizing authority. */
    public void validateSnapshotPin(org.pangrova.generation.api.job.GenerationJob job) {
        if (nativeIndexBuildService == null) {
            throw new IllegalStateException("Native artifact work has no snapshot lifecycle owner");
        }
        nativeIndexBuildService.validateSnapshotPin(job);
    }

    private void recoverStuckBuildsSafe() {
        try {
            stuckBuildRecovery.recoverStuckBuilds();
        } catch (RuntimeException failure) {
            LOG.error("event=stuck_build_recovery_cycle_failed error={}",
                    failure.getMessage(), failure);
        }
    }

    private void startCacheInvalidation() {
        cacheInvalidationScheduler = ManagedExecutorFactory
                .newSingleThreadScheduledExecutor("index-cache-invalidation");
        final AtomicLong lastProcessedEventId = new AtomicLong(0);
        cacheInvalidationScheduler.scheduleWithFixedDelay(() -> {
            try {
                List<CacheEvent> events = metadataStore.pollCacheEvents(
                        lastProcessedEventId.get(), 100);
                for (CacheEvent event : events) {
                    indexCache.invalidateIndex(event.indexId());
                    lastProcessedEventId.set(event.eventId());
                }
            } catch (Exception e) {
                LOG.warn("action=cache_invalidation_poll_failed error={}", e.getMessage());
            }
        }, 5, 5, TimeUnit.SECONDS);
    }

    /** Binds each supported artifact family to the adapter that owns its atomic publication. */
    static GenerationPublicationStore publicationRouter(GenerationPublicationStore indexes,
            GenerationPublicationStore generatedValues, GenerationPublicationStore tabularExports) {
        return GenerationPublicationRouter.route(Map.of(
                GenerationOutputTypeIds.VECTOR_INDEX, indexes,
                GenerationOutputTypeIds.TEXT_INDEX, indexes,
                GenerationOutputTypeIds.GENERATED_COLUMN_VALUES, generatedValues,
                GenerationOutputTypeIds.TABULAR_EXPORT, tabularExports));
    }

    /** Starts metadata publication using the same real durable execution authority as query tasks. */
    void wireExecutionAuthority(org.pangrova.coordinator.coordination.CoordinatorExecutionAuthority authority) {
        if (artifactBuildStore == null || artifactPublisher != null) {
            return;
        }
        artifactPublisher = new DurableGenerationPublisher(artifactBuildStore,
                artifactPublicationStore,
                new org.pangrova.coordinator.generation.GenerationExecutionOwnership(authority),
                this::validateSnapshotPin, artifactPublicationSettings);
        artifactPublisher.start();
    }

    private void registerCommitListeners(
            ConnectorAccessorResolver resolver,
            CatalogRegistry catalogRegistry) {
        java.util.Set<String> registered = new java.util.HashSet<>();
        for (String catalogName : catalogRegistry.listCatalogNames()) {
            try {
                resolver.registerCommitListener(catalogName, commitHandler);
                registered.add(catalogName);
                LOG.info("event=commit_listener_registered catalog={}", catalogName);
            } catch (UnsupportedOperationException unsupported) {
                LOG.debug("event=commit_listener_skip catalog={} reason={}",
                        catalogName, unsupported.getMessage());
            }
        }
        commitListenerCatalogs = java.util.Set.copyOf(registered);
    }

    /** Returns the split filter for query-time integration. */
    public IndexSplitFilter getSplitFilter() { return splitFilter; }

    /** Returns the DDL executor for statement router registration. */
    public IndexDdlExecutor getDdlExecutor() { return ddlExecutor; }

    public org.pangrova.indexing.evolution.IndexSchemaEvolutionHandler getSchemaEvolutionHandler() {
        return schemaEvolutionHandler;
    }

    private org.pangrova.indexing.evolution.IndexSchemaEvolutionHandler.SchemaObservation observeSourceSchema(
                    org.pangrova.coordinator.indexing.ddl.IndexBuildServices buildServices,
                    IndexDescriptor descriptor,
                    org.pangrova.indexing.api.evolution.SchemaChangeEvent event) {
        boolean rename = org.pangrova.indexing.api.enums.SchemaChangeType.RENAME_TABLE
                .equals(event.type());
        String schemaName = rename && event.newSchemaName() != null
                ? event.newSchemaName() : descriptor.schemaName();
        String tableName = rename ? event.tableName() : descriptor.tableName();
        var resolver = org.pangrova.coordinator.indexing.ddl.IndexSourceAuthority.forDescriptor(
                metadataStore, buildServices, accessorResolver, descriptor);
        // Recovery observes only the intended target under the persisted index
        // subject; it never substitutes a different user's authorization.
        return resolver.resolveTableObservation(descriptor.catalogName(), schemaName, tableName);
    }

    /** Returns the index cache for DDL executor. */
    IndexCache getIndexCache() { return indexCache; }

    /** Returns the metadata store for index hint generation. */
    public IndexMetadataStore getMetadataStore() { return metadataStore; }

    /** Returns the connector accessor resolver for split filtering. */
    public ConnectorAccessorResolver getAccessorResolver() { return accessorResolver; }

    /** Returns the generic durable artifact store shared by build and query binding. */
    public GenerationJobStore getArtifactBuildStore() {
        return artifactBuildStore;
    }


    /**
     * Registers an idempotent processor on the existing durable commit-log barrier.
     * Connector callbacks continue to perform one operation only: append the event.
     */
    public java.util.Set<String> registerDurableCommitProcessor(
            org.pangrova.connector.api.index.CommitListener additional) {
        if (commitHandler == null || accessorResolver == null || additional == null) {
            return java.util.Set.of();
        }
        commitHandler.registerDurableCommitProcessor(additional);
        LOG.info("event=durable_commit_processor_registered catalogs={}",
                commitListenerCatalogs);
        return java.util.Set.copyOf(commitListenerCatalogs);
    }

    /** Returns the build coordinator for auto-rebuild triggers. */
    public IndexBuildCoordinator getBuildCoordinator() { return buildCoordinator; }

    /**
     * Called when this node becomes the maintenance leader.
     * Starts commit-log polling, GC scheduling, and resumes build acceptance.
     * Mirrors {@link StreamingBootstrap#onLeaderElected()}.
     */
    @Override
    public synchronized void onLeaderElected() {
        startSnapshotPinRecovery();
        startSchemaEvolutionRecovery();
        if (commitHandler != null) {
            commitHandler.startPolling();
        }
        garbageCollector.startSchedule();
        buildCoordinator.resumePendingBuilds();
        LOG.info("event=indexing_leader_elected");
    }

    /**
     * Called when this node loses maintenance leadership.
     * Stops commit-log polling, GC scheduling, and pauses build acceptance.
     * In-progress builds complete but no new builds are accepted.
     * Mirrors {@link StreamingBootstrap#onLeadershipLost()}.
     */
    @Override
    public synchronized void onLeadershipLost() {
        ResourceCloser.closeAll(null, this::stopSchemaEvolutionRecovery, this::stopSnapshotPinRecovery,
                ResourceCloser.present(commitHandler, IndexCommitHandler::stopPolling),
                ResourceCloser.present(garbageCollector, IndexGarbageCollector::stopSchedule),
                ResourceCloser.present(buildCoordinator, IndexBuildCoordinator::pauseBuilds));
        LOG.info("event=indexing_leadership_lost");
    }

    @Override
    public synchronized void close() {
        ResourceCloser.closeAll(null,
                this::stopSchemaEvolutionRecovery, this::stopSnapshotPinRecovery,
                ResourceCloser.present(ddlExecutor, resource -> closeResource("DdlExecutor", resource)),
                ResourceCloser.present(artifactPublisher, resource -> closeResource("ArtifactPublisher", resource)),
                ResourceCloser.present(consistencyChecker, resource -> closeResource("ConsistencyChecker", resource)),
                ResourceCloser.present(stuckBuildRecoveryScheduler, ScheduledExecutorService::shutdownNow),
                ResourceCloser.present(garbageCollector, resource -> closeResource("GarbageCollector", resource)),
                ResourceCloser.present(cacheInvalidationScheduler, ScheduledExecutorService::shutdownNow),
                ResourceCloser.present(commitHandler, resource -> closeResource("CommitHandler", resource)),
                ResourceCloser.present(splitFilter, resource -> closeResource("SplitFilter", resource)),
                ResourceCloser.present(buildCoordinator, resource -> closeResource("BuildCoordinator", resource)),
                () -> {
                    if (leaderElection != null && configuredLeaderElection == null) {
                        leaderElection.release();
                    }
                });
        indexCache = null;
        LOG.info("event=indexing_shutdown_completed");
    }

    private static void closeResource(String name, AutoCloseable closeable) {
        try {
            closeable.close();
            LOG.info("event=indexing_phase_closed phase={}", name);
        } catch (Exception failure) {
            throw new IllegalStateException("Indexing resource has not closed: " + name, failure);
        }
    }
}
