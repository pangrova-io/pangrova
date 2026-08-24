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
import org.pangrova.coordinator.index.lifecycle.IndexGarbageCollector;
import org.pangrova.coordinator.index.lifecycle.IndexGcContext;
import org.pangrova.coordinator.index.lifecycle.IndexLifecycleMetrics;
import org.pangrova.coordinator.index.lifecycle.MaintenanceLeaderElection;
import org.pangrova.coordinator.index.lifecycle.MetadataBackedGenerationChecker;
import org.pangrova.coordinator.index.lifecycle.StuckBuildRecovery;
import org.pangrova.indexing.IndexEngineRegistry;
import org.pangrova.indexing.build.IndexBuildCoordinator;
import org.pangrova.indexing.build.IndexBuildContext;
import org.pangrova.indexing.config.IndexingConfig;
import org.pangrova.indexing.eval.IndexEvaluationService;
import org.pangrova.indexing.metrics.IndexBuildMetrics;
import org.pangrova.indexing.serde.IndexSerializer;
import org.pangrova.indexing.serde.TypeSerializerRegistry;
import org.pangrova.indexing.storage.IndexCache;
import org.pangrova.indexing.storage.IndexStorageLayout;
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

    // Stuck build recovery (periodic)
    private StuckBuildRecovery stuckBuildRecovery;
    private ScheduledExecutorService stuckBuildRecoveryScheduler;

    // Cache invalidation polling
    private ScheduledExecutorService cacheInvalidationScheduler;

    // Lease stale cleanup (periodic)
    private ScheduledExecutorService leaseCleanupScheduler;

    // Metrics (nullable — only created when MetricsProvider available)
    private IndexBuildMetrics buildMetrics;
    private IndexEvalMetrics evalMetrics;
    private IndexLifecycleMetrics lifecycleMetrics;

    private final String baseIndexPath;
    private final DistributedLease distributedLease;

    /**
     * Creates an IndexingBootstrap with an externally-configured base index path
     * and a distributed lease backend for maintenance leader election.
     *
     * @param baseIndexPath    root path for index file storage (must not be null or blank)
     * @param distributedLease lease backend for leader election (must not be null)
     */
    IndexingBootstrap(String baseIndexPath, DistributedLease distributedLease) {
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
    }

    /**
     * Initializes all indexing components using the constructor-injected distributed lease.
     *
     * @param metadataStore   index metadata persistence
     * @param catalogRegistry catalog registry for connector resolution
     * @param metricsProvider metrics provider (nullable)
     * @param accessControl   access control (nullable)
     */
    void start(IndexMetadataStore metadataStore,
               CatalogRegistry catalogRegistry,
               MetricsProvider metricsProvider,
               AccessControl accessControl) {

        if (metadataStore == null) {
            throw new IllegalStateException(
                    "IndexingBootstrap requires metadataStore but received null");
        }
        if (catalogRegistry == null) {
            throw new IllegalStateException(
                    "IndexingBootstrap requires catalogRegistry but received null");
        }

        this.metadataStore = metadataStore;

        IndexingConfig config = new IndexingConfig(
                baseIndexPath, true, null, null);

        // 1. Engine registry (ServiceLoader discovery)
        engineRegistry = new IndexEngineRegistry();
        LOG.info("event=indexing_phase phase=engine_registry providers={}", engineRegistry.size());

        // 2. Cache
        indexCache = new IndexCache(config.cacheSizeBytes());

        // 3. Storage
        IndexStorageLayout layout = new IndexStorageLayout(config.baseIndexPath());
        storageManager = new IndexStorageManager();

        // 4. Serializer
        TypeSerializerRegistry serdeRegistry = TypeSerializerRegistry.createDefault();
        IndexSerializer serializer = new IndexSerializer(serdeRegistry, config.checksumEnabled());

        // 5. Metrics (if available)
        if (metricsProvider != null) {
            buildMetrics = new IndexBuildMetrics(metricsProvider);
            evalMetrics = new IndexEvalMetrics(metricsProvider);
            lifecycleMetrics = new IndexLifecycleMetrics(metricsProvider);
            new org.pangrova.indexing.metrics.IndexCacheMetrics(metricsProvider, indexCache);
        }

        // 6. Connector accessor resolver (moved before build coordinator — requires accessorResolver)
        accessorResolver = new ConnectorAccessorResolver(catalogRegistry);

        // 7. Build coordinator with distributed coordination
        var claimHandler = new org.pangrova.coordinator.index.lifecycle.MetadataBackedBuildClaimHandler(metadataStore);
        var generationChecker = new MetadataBackedGenerationChecker(metadataStore);
        org.pangrova.spi.connector.index.SnapshotPinProvider snapshotPinProvider =
                accessorResolver.resolveSnapshotPinProvider();
        IndexBuildContext buildDeps = new IndexBuildContext(
                config, engineRegistry, storageManager, serializer,
                claimHandler, generationChecker, snapshotPinProvider, null);
        buildCoordinator = new IndexBuildCoordinator(buildDeps, buildMetrics);

        // 8. Evaluation service with circuit breaker from config.
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

        // 9. Split filter (L0 + L1 only; L2 delegated to workers via hints)
        IndexDescriptorCache descriptorCache = new IndexDescriptorCache(metadataStore);
        var evalConfig = new IndexEvalConfig(
                evalMetrics, config.evalTimeoutMs(), config.evalParallelism());
        splitFilter = new IndexSplitFilter(metadataStore, descriptorCache, evalConfig);

        // 10. Commit handler (conditionally created based on commitListener config)
        IndexCommitContext commitDeps = new IndexCommitContext(
                metadataStore, buildCoordinator, accessorResolver);

        // 11. Garbage collector (created below after leader election at step 12)
        IndexGcContext gcDeps = new IndexGcContext(
                metadataStore, storageManager, accessorResolver, config);

        // 12. Leader election (shared by consistency checker, commit handler, and GC)
        String coordinatorId = java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
        MaintenanceLeaderElection leaderElection = new MaintenanceLeaderElection(
                distributedLease, coordinatorId);

        // 10b. Wire leader election into commit handler (skip when commitListener disabled)
        if (config.commitListenerEnabled()) {
            int queueCapacity = config.lifecycle().commitListener().queueCapacity();
            int batchMax = config.lifecycle().commitListener().batchMax();
            commitHandler = new IndexCommitHandler(
                    commitDeps, lifecycleMetrics, leaderElection,
                    queueCapacity, batchMax);
        }

        // 11b. Wire leader election into garbage collector
        garbageCollector = new IndexGarbageCollector(gcDeps, lifecycleMetrics, leaderElection);
        garbageCollector.start();

        // 13. Consistency checker
        IndexConsistencyContext consistencyDeps = new IndexConsistencyContext(
                metadataStore, buildCoordinator, accessorResolver, config, leaderElection);
        consistencyChecker = new IndexConsistencyChecker(consistencyDeps, lifecycleMetrics);
        consistencyChecker.start();

        // 14. Stuck build recovery with leader election
        stuckBuildRecovery = new StuckBuildRecovery(
                metadataStore, DEFAULT_BUILD_TIMEOUT, DEFAULT_STUCK_DETECTION_MULTIPLIER,
                leaderElection);
        int recovered = stuckBuildRecovery.recoverStuckBuilds();
        if (recovered > 0) {
            LOG.info("event=indexing_phase phase=stuck_recovery recovered={}", recovered);
        }

        // 14.1 Schedule periodic stuck build recovery (interval = buildTimeout / 2)
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

        // 14.2 Schedule periodic lease stale cleanup (every 5 minutes)
        leaseCleanupScheduler = ManagedExecutorFactory
                .newSingleThreadScheduledExecutor("pangrova-lease-cleanup");
        leaseCleanupScheduler.scheduleWithFixedDelay(
                distributedLease::expireStale, 5, 5, TimeUnit.MINUTES);
        LOG.info("event=indexing_phase phase=lease_cleanup_scheduled intervalMinutes=5");

        // 15. Register commit listener with all catalogs (skip when commitListener disabled)
        if (config.commitListenerEnabled()) {
            registerCommitListeners(accessorResolver, catalogRegistry);
        } else {
            LOG.info("event=commit_listener_skipped reason=disabled_by_config");
        }

        // 16. Cache invalidation polling
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

        // 17. DDL executor
        IndexDdlContext ddlDeps = new IndexDdlContext(
                metadataStore, buildCoordinator, catalogRegistry, accessorResolver);
        ddlExecutor = new IndexDdlExecutor(ddlDeps, indexCache, accessControl);

        LOG.info("event=indexing_bootstrap_completed");
    }

    private void registerCommitListeners(
            ConnectorAccessorResolver accessorResolver,
            CatalogRegistry catalogRegistry) {
        for (String catalogName : catalogRegistry.listCatalogNames()) {
            try {
                accessorResolver.registerCommitListener(catalogName, commitHandler);
                LOG.info("event=commit_listener_registered catalog={}", catalogName);
            } catch (Exception e) {
                LOG.debug("event=commit_listener_skip catalog={} reason={}",
                        catalogName, e.getMessage());
            }
        }
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

    /**
     * Registers an additional commit listener alongside the existing index handler.
     * The connector only supports a single listener, so this composes both into one.
     */
    public void registerAdditionalCommitListener(
            org.pangrova.spi.connector.index.CommitListener additional,
            CatalogRegistry catalogRegistry) {
        if (commitHandler == null || accessorResolver == null || additional == null) {
            return;
        }
        org.pangrova.spi.connector.index.CommitListener composed = event -> {
            commitHandler.onCommit(event);
            try {
                additional.onCommit(event);
            } catch (Exception e) {
                LOG.warn("event=additional_commit_listener_failed error={}", e.getMessage());
            }
        };
        for (String catalogName : catalogRegistry.listCatalogNames()) {
            try {
                accessorResolver.registerCommitListener(catalogName, composed);
            } catch (Exception e) {
                LOG.debug("event=additional_commit_listener_skip catalog={} reason={}",
                        catalogName, e.getMessage());
            }
        }
        LOG.info("event=additional_commit_listener_registered");
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
        commitHandler.startPolling();
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
        commitHandler.stopPolling();
        garbageCollector.stopSchedule();
        buildCoordinator.pauseBuilds();
        LOG.info("event=indexing_leadership_lost");
    }

    @Override
    public void close() {
        // LIFO shutdown order
        safeClose("DdlExecutor", ddlExecutor);
        safeClose("ConsistencyChecker", consistencyChecker);
        if (leaseCleanupScheduler != null) {
            leaseCleanupScheduler.shutdownNow();
            LOG.info("event=indexing_phase_closed phase=LeaseCleanupScheduler");
        }
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
