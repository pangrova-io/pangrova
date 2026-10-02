package org.pangrova.coordinator.bootstrap;

import org.pangrova.coordinator.index.ddl.IndexDdlExecutor;
import org.pangrova.coordinator.index.IndexDescriptorCache;
import org.pangrova.coordinator.index.IndexEvalMetrics;
import org.pangrova.coordinator.index.filter.IndexEvalConfig;
import org.pangrova.coordinator.index.filter.IndexSplitFilter;
import org.pangrova.coordinator.index.ddl.ConnectorAccessorResolver;
import org.pangrova.coordinator.index.ddl.IndexDdlContext;
import org.pangrova.coordinator.index.lifecycle.IndexCommitContext;
import org.pangrova.coordinator.index.lifecycle.IndexCommitHandler;
import org.pangrova.coordinator.index.lifecycle.IndexConsistencyChecker;
import org.pangrova.coordinator.index.lifecycle.IndexConsistencyContext;
import org.pangrova.coordinator.index.lifecycle.gc.IndexGarbageCollector;
import org.pangrova.coordinator.index.lifecycle.gc.IndexGcContext;
import org.pangrova.coordinator.index.lifecycle.IndexLifecycleMetrics;
import org.pangrova.coordinator.index.lifecycle.MaintenanceLeaderElection;
import org.pangrova.coordinator.index.lifecycle.MetadataBackedGenerationChecker;
import org.pangrova.coordinator.index.lifecycle.StuckBuildRecovery;
import org.pangrova.coordinator.artifact.DurableArtifactPublisher;
import org.pangrova.indexing.IndexEngineRegistry;
import org.pangrova.indexing.build.IndexBuildCoordinator;
import org.pangrova.indexing.build.IndexBuildContext;
import org.pangrova.indexing.config.IndexingConfig;
import org.pangrova.indexing.eval.IndexEvaluationService;
import org.pangrova.indexing.metrics.IndexBuildMetrics;
import org.pangrova.indexing.serde.IndexSerializer;
import org.pangrova.indexing.serde.TypeSerializerRegistry;
import org.pangrova.indexing.storage.IndexCache;
import org.pangrova.indexing.storage.IndexStorageManager;
import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.metadata.index.IndexMetadataStore;
import org.pangrova.metadata.index.model.CacheEvent;
import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.spi.coordination.DistributedLease;
import org.pangrova.spi.coordination.MaintenanceLeaderAware;
import org.pangrova.spi.observability.MetricsProvider;
import org.pangrova.spi.security.AccessControl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Bootstraps all indexing subsystem components in dependency order.
 *
 * <p>Initialization: IndexEngineRegistry → IndexCache → IndexStorageManager →
 * IndexBuildCoordinator → IndexEvaluationService → IndexSplitFilter →
 * IndexCommitHandler → GC → ConsistencyChecker → StuckBuildRecovery →
 * DDL executor registration.</p>
 *
 * <p>The coordinator performs L0 (min/max metadata) and L1 (native connector)
 * pruning only. L2 evaluation (actual index data loading) is delegated to
 * workers via {@link org.pangrova.spi.index.eval.IndexEvalHint} objects
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
    private IndexMetadataStore metadataStore;
    private ConnectorAccessorResolver accessorResolver;
    private volatile java.util.Set<String> commitListenerCatalogs = java.util.Set.of();

    // Stuck build recovery (periodic)
    private StuckBuildRecovery stuckBuildRecovery;
    private ScheduledExecutorService stuckBuildRecoveryScheduler;

    // Cache invalidation polling
    private ScheduledExecutorService cacheInvalidationScheduler;

    // Metrics (nullable — only created when MetricsProvider available)
    private IndexBuildMetrics buildMetrics;
    private IndexEvalMetrics evalMetrics;
    private IndexLifecycleMetrics lifecycleMetrics;

    private final String baseIndexPath;
    private final DistributedLease distributedLease;
    private final org.pangrova.metadata.artifact.build.ArtifactBuildStore artifactBuildStore;
    private final org.pangrova.metadata.artifact.ArtifactPublicationStore artifactPublicationStore;
    private final MaintenanceLeaderElection configuredLeaderElection;
    private MaintenanceLeaderElection leaderElection;
    private DurableArtifactPublisher artifactPublisher;

    /**
     * Creates an IndexingBootstrap with an externally-configured base index path
     * and a distributed lease backend for maintenance leader election.
     *
     * @param baseIndexPath    root path for index file storage (must not be null or blank)
     * @param distributedLease lease backend for leader election (must not be null)
     */
    IndexingBootstrap(String baseIndexPath, DistributedLease distributedLease) {
        this(baseIndexPath, distributedLease, null, null, null);
    }

    IndexingBootstrap(
            String baseIndexPath,
            DistributedLease distributedLease,
            org.pangrova.metadata.artifact.build.ArtifactBuildStore artifactBuildStore) {
        this(baseIndexPath, distributedLease, artifactBuildStore, null, null);
    }

    /** Creates the complete production lifecycle with one durable build/publication state machine. */
    public IndexingBootstrap(
            String baseIndexPath,
            DistributedLease distributedLease,
            org.pangrova.metadata.artifact.build.ArtifactBuildStore artifactBuildStore,
            org.pangrova.metadata.artifact.ArtifactPublicationStore artifactPublicationStore) {
        this(baseIndexPath, distributedLease, artifactBuildStore,
                artifactPublicationStore, null);
    }

    IndexingBootstrap(
            String baseIndexPath,
            DistributedLease distributedLease,
            org.pangrova.metadata.artifact.build.ArtifactBuildStore artifactBuildStore,
            org.pangrova.metadata.artifact.ArtifactPublicationStore artifactPublicationStore,
            MaintenanceLeaderElection configuredLeaderElection) {
        if (baseIndexPath == null || baseIndexPath.isBlank()) {
            throw new IllegalStateException(
                    "IndexingBootstrap requires baseIndexPath but received null or blank value");
        }
        if (distributedLease == null) {
            throw new IllegalStateException(
                    "IndexingBootstrap requires distributedLease but received null");
        }
        this.baseIndexPath = baseIndexPath;
        this.distributedLease = distributedLease;
        this.artifactBuildStore = artifactBuildStore;
        this.artifactPublicationStore = artifactPublicationStore;
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
               CatalogRegistry catalogRegistry,
               MetricsProvider metricsProvider,
               AccessControl accessControl) {

        if (indexMetadata == null) {
            throw new IllegalStateException(
                    "IndexingBootstrap requires metadataStore but received null");
        }
        if (catalogRegistry == null) {
            throw new IllegalStateException(
                    "IndexingBootstrap requires catalogRegistry but received null");
        }

        this.metadataStore = indexMetadata;
        IndexingConfig config = new IndexingConfig(
                baseIndexPath, true, null, null);
        IndexSerializer serializer = initializeEngine(config, metricsProvider);
        var buildServices = initializeBuildServices(config, catalogRegistry, serializer);
        startLifecycle(config, buildServices);
        startRecovery();
        if (config.commitListenerEnabled()) {
            registerCommitListeners(accessorResolver, catalogRegistry);
        } else {
            LOG.info("event=commit_listener_skipped reason=disabled_by_config");
        }
        startCacheInvalidation();
        IndexDdlContext ddlDeps = new IndexDdlContext(
                metadataStore, buildServices, catalogRegistry, accessorResolver);
        ddlExecutor = new IndexDdlExecutor(ddlDeps, indexCache, accessControl);
        LOG.info("event=indexing_bootstrap_completed");
    }

    private IndexSerializer initializeEngine(IndexingConfig config, MetricsProvider metricsProvider) {
        engineRegistry = new IndexEngineRegistry();
        LOG.info("event=indexing_phase phase=engine_registry providers={}", engineRegistry.size());
        indexCache = new IndexCache(config.cacheSizeBytes());
        storageManager = new IndexStorageManager();
        TypeSerializerRegistry serdeRegistry = TypeSerializerRegistry.createDefault();
        IndexSerializer serializer = new IndexSerializer(serdeRegistry, config.checksumEnabled());
        if (metricsProvider != null) {
            buildMetrics = new IndexBuildMetrics(metricsProvider);
            evalMetrics = new IndexEvalMetrics(metricsProvider);
            lifecycleMetrics = new IndexLifecycleMetrics(metricsProvider);
            new org.pangrova.indexing.metrics.IndexCacheMetrics(metricsProvider, indexCache);
        }
        return serializer;
    }

    private org.pangrova.coordinator.index.ddl.IndexBuildServices initializeBuildServices(
            IndexingConfig config, CatalogRegistry catalogRegistry, IndexSerializer serializer) {
        accessorResolver = new ConnectorAccessorResolver(catalogRegistry);
        var claimHandler = new org.pangrova.coordinator.index.lifecycle.MetadataBackedBuildClaimHandler(metadataStore);
        var generationChecker = new MetadataBackedGenerationChecker(metadataStore);
        org.pangrova.spi.connector.index.SnapshotPinProvider snapshotPinProvider =
                new org.pangrova.coordinator.index.ddl.DeferredSnapshotPinProvider(
                        metadataStore, artifactBuildStore, accessorResolver);
        IndexBuildContext buildDeps = new IndexBuildContext(
                config, engineRegistry, storageManager, serializer,
                claimHandler, generationChecker, snapshotPinProvider, null);
        buildCoordinator = new IndexBuildCoordinator(buildDeps, buildMetrics);

        // F-IDX-BREAKER: back the breaker with the shared metadata store so a
        // tripped circuit is observed cluster-wide (multi-coordinator Active-Active),
        // not just locally. Degrades to empty state when the metadata store is not
        // JDBC-backed (default interface methods), matching prior noOp behaviour.
        int circuitFailureThreshold = config.runtime().eval().circuitFailureThreshold();
        java.time.Duration circuitOpenDuration =
                java.time.Duration.ofSeconds(config.runtime().eval().circuitOpenDurationSeconds());
        var breakerStateStore = new org.pangrova.coordinator.index.MetadataBreakerStateStore(
                metadataStore, circuitFailureThreshold, circuitOpenDuration);
        var circuitBreaker = new org.pangrova.indexing.eval.IndexEvalCircuitBreaker(
                circuitFailureThreshold, circuitOpenDuration, breakerStateStore);
        evaluationService = new IndexEvaluationService(
                engineRegistry, indexCache, storageManager, serializer, circuitBreaker);

        IndexDescriptorCache descriptorCache = new IndexDescriptorCache(metadataStore);
        var evalConfig = new IndexEvalConfig(
                evalMetrics, config.evalTimeoutMs(), config.evalParallelism());
        splitFilter = new IndexSplitFilter(metadataStore, descriptorCache, evalConfig);

        // Scalar indexes use the Java builder; vector/BM25 artifacts use durable Native jobs.
        var aiNativeBuildService = artifactBuildStore == null ? null
                : new org.pangrova.coordinator.index.ddl.DurableIndexBuildService(
                        artifactBuildStore, catalogRegistry, baseIndexPath);
        return new org.pangrova.coordinator.index.ddl.IndexBuildServices(
                buildCoordinator, aiNativeBuildService);
    }

    private void startLifecycle(IndexingConfig config,
                                org.pangrova.coordinator.index.ddl.IndexBuildServices buildServices) {
        IndexCommitContext commitDeps = new IndexCommitContext(
                metadataStore, buildServices, accessorResolver);

        IndexGcContext gcDeps = new IndexGcContext(
                metadataStore, storageManager, accessorResolver, config);

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
        garbageCollector.start();

        IndexConsistencyContext consistencyDeps = new IndexConsistencyContext(
                metadataStore, buildServices, accessorResolver, config, leaderElection);
        consistencyChecker = new IndexConsistencyChecker(consistencyDeps, lifecycleMetrics);
        consistencyChecker.start();
    }

    private void startRecovery() {
        stuckBuildRecovery = new StuckBuildRecovery(
                metadataStore, DEFAULT_BUILD_TIMEOUT, DEFAULT_STUCK_DETECTION_MULTIPLIER,
                leaderElection);
        int recovered = stuckBuildRecovery.recoverStuckBuilds();
        if (recovered > 0) {
            LOG.info("event=indexing_phase phase=stuck_recovery recovered={}", recovered);
        }

        long recoveryIntervalMinutes = DEFAULT_BUILD_TIMEOUT.toMinutes() / 2;
        if (recoveryIntervalMinutes < 1) {
            recoveryIntervalMinutes = 1;
        }
        stuckBuildRecoveryScheduler = ManagedExecutorFactory
                .newSingleThreadScheduledExecutor("pangrova-stuck-build-recovery");
        stuckBuildRecoveryScheduler.scheduleWithFixedDelay(
                stuckBuildRecovery::recoverStuckBuilds,
                recoveryIntervalMinutes, recoveryIntervalMinutes, TimeUnit.MINUTES);
        LOG.info("event=indexing_phase phase=stuck_recovery_scheduled intervalMinutes={}",
                recoveryIntervalMinutes);
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

    /** Starts metadata publication using the same real durable execution authority as query tasks. */
    void wireExecutionAuthority(org.pangrova.coordinator.coordination.CoordinatorExecutionAuthority authority) {
        if (artifactBuildStore == null || artifactPublisher != null) {
            return;
        }
        artifactPublisher = new DurableArtifactPublisher(artifactBuildStore,
                artifactPublicationStores(artifactPublicationStore),
                new org.pangrova.coordinator.artifact.ArtifactExecutionOwnership(authority));
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

    /** Returns the index cache for DDL executor. */
    IndexCache getIndexCache() { return indexCache; }

    /** Returns the metadata store for index hint generation. */
    public IndexMetadataStore getMetadataStore() { return metadataStore; }

    /** Returns the connector accessor resolver for split filtering. */
    public ConnectorAccessorResolver getAccessorResolver() { return accessorResolver; }

    /** Returns the generic durable artifact store shared by build and query binding. */
    public org.pangrova.metadata.artifact.build.ArtifactBuildStore getArtifactBuildStore() {
        return artifactBuildStore;
    }

    static Map<String, org.pangrova.metadata.artifact.ArtifactPublicationStore>
            artifactPublicationStores(
                    org.pangrova.metadata.artifact.ArtifactPublicationStore store) {
        return Map.of(
                org.pangrova.metadata.artifact.contract.ArtifactTypeIds.VECTOR_INDEX, store,
                org.pangrova.metadata.artifact.contract.ArtifactTypeIds.TEXT_INDEX, store,
                org.pangrova.metadata.artifact.contract.ArtifactTypeIds.TABULAR_EXPORT, store,
                org.pangrova.metadata.artifact.contract.ArtifactTypeIds.GENERATED_COLUMN_VALUES, store);
    }

    /**
     * Registers an idempotent processor on the existing durable commit-log barrier.
     * Connector callbacks continue to perform one operation only: append the event.
     */
    public java.util.Set<String> registerDurableCommitProcessor(
            org.pangrova.spi.connector.index.CommitListener additional) {
        if (commitHandler == null || accessorResolver == null || additional == null) {
            return java.util.Set.of();
        }
        commitHandler.registerDurableCommitProcessor(additional);
        LOG.info("event=durable_commit_processor_registered catalogs={}",
                commitListenerCatalogs);
        return commitListenerCatalogs;
    }

    /** Returns the build coordinator for auto-rebuild triggers. */
    public IndexBuildCoordinator getBuildCoordinator() { return buildCoordinator; }

    /**
     * Called when this node becomes the maintenance leader.
     * Starts commit-log polling, GC scheduling, and resumes build acceptance.
     * Mirrors {@link StreamingBootstrap#onLeaderElected()}.
     */
    @Override
    public void onLeaderElected() {
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
    public void onLeadershipLost() {
        if (commitHandler != null) {
            commitHandler.stopPolling();
        }
        garbageCollector.stopSchedule();
        buildCoordinator.pauseBuilds();
        LOG.info("event=indexing_leadership_lost");
    }

    @Override
    public void close() {
        // LIFO shutdown order
        safeClose("DdlExecutor", ddlExecutor);
        safeClose("ArtifactPublisher", artifactPublisher);
        safeClose("ConsistencyChecker", consistencyChecker);
        if (stuckBuildRecoveryScheduler != null) {
            stuckBuildRecoveryScheduler.shutdownNow();
            LOG.info("event=indexing_phase_closed phase=StuckBuildRecoveryScheduler");
        }
        safeClose("GarbageCollector", garbageCollector);
        if (cacheInvalidationScheduler != null) {
            cacheInvalidationScheduler.shutdownNow();
            LOG.info("event=indexing_phase_closed phase=CacheInvalidationScheduler");
        }
        safeClose("CommitHandler", commitHandler);
        safeClose("SplitFilter", splitFilter);
        safeClose("BuildCoordinator", buildCoordinator);
        if (leaderElection != null && configuredLeaderElection == null) {
            leaderElection.release();
        }
        if (indexCache != null) {
            indexCache = null;
        }
        LOG.info("event=indexing_shutdown_completed");
    }

    private static void safeClose(String name, AutoCloseable closeable) {
        if (closeable == null) return;
        try {
            closeable.close();
            LOG.info("event=indexing_phase_closed phase={}", name);
        } catch (Exception e) {
            LOG.warn("event=indexing_close_failed phase={} error={}", name, e.getMessage());
        }
    }
}
