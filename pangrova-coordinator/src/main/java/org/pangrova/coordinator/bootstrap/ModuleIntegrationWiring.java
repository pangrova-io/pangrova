package org.pangrova.coordinator.bootstrap;

import org.pangrova.governance.GovernanceContext;
import org.pangrova.governance.GovernanceWiring;
import org.pangrova.governance.lineage.LineageEventEmitter;
import org.pangrova.governance.lineage.LogLineageBackend;
import org.pangrova.governance.quality.schedule.CronScheduler;
import org.pangrova.governance.quality.alert.AlertChannelRegistry;
import org.pangrova.governance.quality.alert.DataQualityAlerter;
import org.pangrova.governance.quality.alert.LogAlertChannel;
import org.pangrova.governance.quality.rule.DefaultQualityRuleExecutor;
import org.pangrova.governance.quality.rule.GovernanceStatementHandler;
import org.pangrova.governance.quality.rule.InMemoryQualityRuleStore;
import org.pangrova.governance.quality.rule.QualityRuleStore;

import org.pangrova.resilience.breaker.CircuitBreaker;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.metadata.streaming.JdbcCdcRegistrationStore;
import org.pangrova.spi.streaming.CdcLifecycleManager;
import org.pangrova.spi.streaming.CdcRegistrationStore;
import org.pangrova.streaming.StreamingWiring;
import org.pangrova.streaming.cdc.CdcRegistrationHandler;
import org.pangrova.streaming.cdc.InMemoryCdcRegistrationStore;
import org.pangrova.streaming.maintenance.MaintenanceScheduler;
import org.pangrova.metadata.streaming.JdbcWatermarkStore;
import org.pangrova.spi.streaming.WatermarkStore;
import org.pangrova.streaming.watermark.InMemoryWatermarkStore;
import org.pangrova.streaming.watermark.WatermarkTracker;
import org.pangrova.security.tenant.quota.QuotaChecker;
import org.pangrova.security.tenant.resolver.TenantResolver;
import org.pangrova.security.tenant.store.InMemoryTenantStore;
import org.pangrova.spi.coordination.MaintenanceLeaderAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;

/**
 * Wires optional modules (streaming, governance, resilience, tenant, operations)
 * into the Coordinator lifecycle. Each module is guarded by a feature flag.
 *
 * <p>Implements {@link MaintenanceLeaderAware} to provide a single entry point
 * for leader lifecycle transitions. On leader elected/lost, delegates to
 * {@link IndexingBootstrap}, {@link StreamingBootstrap}, and any other
 * leader-aware subsystems.</p>
 */
final class ModuleIntegrationWiring implements MaintenanceLeaderAware, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ModuleIntegrationWiring.class);

    private volatile boolean closed;

    // Streaming
    private StreamingWiring streamingModule;
    private CdcRegistrationHandler cdcRegistrationHandler;
    private MaintenanceScheduler maintenanceScheduler;

    // Governance
    private GovernanceWiring governanceModule;
    private LineageEventEmitter lineageEmitter;

    // Resilience
    private CircuitBreaker circuitBreaker;
    private org.pangrova.resilience.degradation.DegradationManager degradationManager;
    private org.pangrova.resilience.retry.StageRetryManager stageRetryManager;

    // Tenant
    private TenantResolver tenantResolver;
    private QuotaChecker quotaChecker;

    // Operations
    private org.pangrova.operations.recovery.RecoveryPlaybookFramework recoveryPlaybook;

    // Indexing
    private IndexingBootstrap indexingBootstrap;
    private IndexQueryBootstrap indexQueryBootstrap;

    // Streaming bootstrap (set externally after build)
    private StreamingBootstrap streamingBootstrap;

    // Branch TTL cleanup (leader-gated, set externally)
    private org.pangrova.coordinator.ai.branch.BranchTtlCleanupTask branchTtlCleanupTask;

    // Metadata sync (leader-gated periodic sync for federation catalogs)
    private org.pangrova.metadata.sync.MetadataSyncService metadataSyncService;
    private org.pangrova.metadata.catalog.CatalogRegistry catalogRegistry;
    private ScheduledExecutorService metadataSyncScheduler;
    private volatile ScheduledFuture<?> metadataSyncTask;

    // Invalidation log cleanup (leader-gated)
    private org.pangrova.metadata.cache.JdbcInvalidationPoller invalidationPoller;
    private volatile ScheduledFuture<?> invalidationCleanupTask;

    private static final long METADATA_SYNC_INTERVAL_SECONDS = 60;

    // ── Unified leader lifecycle (MaintenanceLeaderAware) ───────────

    /**
     * Called when this node becomes the maintenance leader.
     * Delegates to all leader-aware subsystems: indexing, streaming, branch TTL,
     * metadata sync.
     */
    @Override
    public void onLeaderElected() {
        if (indexingBootstrap != null) {
            indexingBootstrap.onLeaderElected();
        }
        if (streamingBootstrap != null) {
            streamingBootstrap.onLeaderElected();
        }
        if (branchTtlCleanupTask != null) {
            branchTtlCleanupTask.start();
        }
        startMetadataSyncSchedule();
        startInvalidationCleanupSchedule();
        LOG.info("event=unified_leader_elected");
    }

    /**
     * Called when this node loses maintenance leadership.
     * Delegates to all leader-aware subsystems: indexing, streaming, branch TTL,
     * metadata sync.
     */
    @Override
    public void onLeadershipLost() {
        if (indexingBootstrap != null) {
            indexingBootstrap.onLeadershipLost();
        }
        if (streamingBootstrap != null) {
            streamingBootstrap.onLeadershipLost();
        }
        if (branchTtlCleanupTask != null) {
            branchTtlCleanupTask.stop();
        }
        stopMetadataSyncSchedule();
        stopInvalidationCleanupSchedule();
        LOG.info("event=unified_leadership_lost");
    }

    /**
     * Configures the metadata sync service and catalog registry for leader-gated periodic sync.
     * Must be called during bootstrap after MetadataBootstrap is initialized.
     */
    void setMetadataSyncDeps(org.pangrova.metadata.sync.MetadataSyncService syncService,
                             org.pangrova.metadata.catalog.CatalogRegistry catalogRegistry) {
        this.metadataSyncService = syncService;
        this.catalogRegistry = catalogRegistry;
    }

    void setInvalidationPoller(org.pangrova.metadata.cache.JdbcInvalidationPoller poller) {
        this.invalidationPoller = poller;
    }

    org.pangrova.metadata.cache.JdbcInvalidationPoller getInvalidationPoller() {
        return invalidationPoller;
    }

    private void startMetadataSyncSchedule() {
        if (metadataSyncService == null || catalogRegistry == null) {
            return;
        }
        if (metadataSyncScheduler == null) {
            metadataSyncScheduler = org.pangrova.common.concurrent.ManagedExecutorFactory
                    .newSingleThreadScheduledExecutor("metadata-sync");
        }
        metadataSyncTask = metadataSyncScheduler.scheduleWithFixedDelay(
                this::runMetadataSync,
                METADATA_SYNC_INTERVAL_SECONDS, METADATA_SYNC_INTERVAL_SECONDS, TimeUnit.SECONDS);
        LOG.info("event=metadata_sync_started intervalSeconds={}", METADATA_SYNC_INTERVAL_SECONDS);
    }

    private void stopMetadataSyncSchedule() {
        if (metadataSyncTask != null) {
            metadataSyncTask.cancel(false);
            metadataSyncTask = null;
        }
        LOG.info("event=metadata_sync_stopped");
    }

    private void runMetadataSync() {
        for (String catalogName : catalogRegistry.listCatalogNames()) {
            try {
                org.pangrova.metadata.catalog.CatalogHandle handle = catalogRegistry.getCatalog(catalogName);
                if (!(handle instanceof org.pangrova.metadata.catalog.FederationCatalogHandle fedHandle)) {
                    continue;
                }
                org.pangrova.metadata.adapter.FederatedCatalog adapter =
                        fedHandle.getFederatedCatalog();
                metadataSyncService.syncWithRetry(catalogName, adapter);
            } catch (Exception e) {
                LOG.warn("event=metadata_sync_catalog_failed catalog={} error={}",
                        catalogName, e.getMessage());
            }
        }
    }

    private static final long INVALIDATION_CLEANUP_INTERVAL_HOURS = 24;

    private ScheduledExecutorService invalidationCleanupScheduler;

    private void startInvalidationCleanupSchedule() {
        if (invalidationPoller == null) {
            return;
        }
        invalidationCleanupScheduler = org.pangrova.common.concurrent.ManagedExecutorFactory
                .newSingleThreadScheduledExecutor("invalidation-cleanup");
        invalidationCleanupTask = invalidationCleanupScheduler.scheduleWithFixedDelay(
                () -> {
                    try {
                        invalidationPoller.cleanup();
                    } catch (Exception e) {
                        LOG.warn("event=invalidation_cleanup_failed error={}", e.getMessage());
                    }
                },
                INVALIDATION_CLEANUP_INTERVAL_HOURS, INVALIDATION_CLEANUP_INTERVAL_HOURS, TimeUnit.HOURS);
        LOG.info("event=invalidation_cleanup_started intervalHours={}", INVALIDATION_CLEANUP_INTERVAL_HOURS);
    }

    private void stopInvalidationCleanupSchedule() {
        if (invalidationCleanupTask != null) {
            invalidationCleanupTask.cancel(false);
            invalidationCleanupTask = null;
        }
    }

    void setStreamingBootstrap(StreamingBootstrap streamingBootstrap) {
        this.streamingBootstrap = streamingBootstrap;
    }

    StreamingBootstrap getStreamingBootstrap() {
        return streamingBootstrap;
    }

    void setBranchTtlCleanupTask(org.pangrova.coordinator.ai.branch.BranchTtlCleanupTask task) {
        this.branchTtlCleanupTask = task;
    }

    org.pangrova.coordinator.ai.branch.BranchTtlCleanupTask getBranchTtlCleanupTask() {
        return branchTtlCleanupTask;
    }

    void startStreaming(CoordinatorBootstrapConfig config, DataSource dataSource) {
        if (!config.streamingEnabled()) {
            LOG.info("Phase skipped: streaming not enabled");
            return;
        }
        MaintenanceScheduler scheduler = buildMaintenanceScheduler();
        this.maintenanceScheduler = scheduler;
        WatermarkStore watermarkStore = dataSource != null
                ? new JdbcWatermarkStore(dataSource) : new InMemoryWatermarkStore();
        WatermarkTracker tracker = new WatermarkTracker(watermarkStore);
        streamingModule = new StreamingWiring(scheduler, tracker);
        CdcRegistrationStore cdcStore = createCdcRegistrationStore(dataSource);
        cdcRegistrationHandler = new CdcRegistrationHandler(cdcStore);
        LOG.info("Phase: streaming — initialized (watermarkStore={} cdcRegistrationStore={})",
                watermarkStore.getClass().getSimpleName(), cdcStore.getClass().getSimpleName());
    }

    private static CdcRegistrationStore createCdcRegistrationStore(DataSource dataSource) {
        if (dataSource == null) {
            return new InMemoryCdcRegistrationStore();
        }
        JdbcCdcRegistrationStore jdbcStore = new JdbcCdcRegistrationStore(dataSource);
        jdbcStore.initSchema();
        return jdbcStore;
    }

    void startGovernance(CoordinatorBootstrapConfig config) {
        if (!config.governanceEnabled()) {
            LOG.info("Phase skipped: governance not enabled");
            return;
        }
        lineageEmitter = new LineageEventEmitter(new LogLineageBackend());
        QualityRuleStore ruleStore = new InMemoryQualityRuleStore();
        AlertChannelRegistry channelRegistry = new AlertChannelRegistry();
        channelRegistry.register(new LogAlertChannel());
        DataQualityAlerter alerter = new DataQualityAlerter(channelRegistry);
        GovernanceStatementHandler handler = new GovernanceStatementHandler(
                ruleStore, new DefaultQualityRuleExecutor(), alerter);
        CronScheduler cronScheduler = new CronScheduler(handler);
        governanceModule = new GovernanceWiring(new GovernanceContext(
                new org.pangrova.governance.GovernanceDeps(
                        lineageEmitter, handler, null, cronScheduler, ruleStore)));
        governanceModule.initialize();
        LOG.info("Phase: governance — initialized");
    }

    // Late-binding suppliers for DegradationManager (set after WorkerManager is available)
    private volatile java.util.function.DoubleSupplier workerRatioSupplier = () -> 1.0;
    private volatile java.util.function.DoubleSupplier memoryPressureSupplier = () -> 0.0;

    void startResilience(CoordinatorBootstrapConfig config) {
        if (!config.resilienceEnabled()) {
            LOG.info("Phase skipped: resilience not enabled");
            return;
        }
        circuitBreaker = new CircuitBreaker();
        // DegradationManager uses pull-mode suppliers. At this point WorkerManager
        // doesn't exist yet, so we use volatile-backed delegates that are
        // repointed by bindDegradationMetrics() after QueryBootstrap completes.
        degradationManager = new org.pangrova.resilience.degradation.DegradationManager(
                org.pangrova.resilience.degradation.DegradationThresholds.defaults(),
                () -> workerRatioSupplier.getAsDouble(),
                () -> memoryPressureSupplier.getAsDouble());
        stageRetryManager = new org.pangrova.resilience.retry.StageRetryManager();
        LOG.info("Phase: resilience — initialized (CircuitBreaker, DegradationManager, StageRetryManager)");
    }

    /**
     * Late-binds real worker ratio and memory pressure suppliers to the DegradationManager.
     * Must be called after WorkerManager is available (post-QueryBootstrap).
     */
    void bindDegradationMetrics(java.util.function.DoubleSupplier workerRatio,
                                java.util.function.DoubleSupplier memoryPressure) {
        if (workerRatio != null) {
            this.workerRatioSupplier = workerRatio;
        }
        if (memoryPressure != null) {
            this.memoryPressureSupplier = memoryPressure;
        }
        LOG.info("Phase: resilience — degradation metrics bound to live suppliers");
    }

    void startTenant(CoordinatorBootstrapConfig config, javax.sql.DataSource dataSource) {
        if (!config.tenantEnabled()) {
            LOG.info("Phase skipped: tenant not enabled");
            return;
        }
        org.pangrova.security.tenant.store.TenantStore tenantStore;
        if (dataSource != null) {
            tenantStore = new org.pangrova.metadata.tenant.JdbcTenantStore(dataSource);
            LOG.info("Phase: tenant — using JDBC-backed tenant store");
        } else {
            LOG.warn("Phase: tenant — no datasource configured; using InMemoryTenantStore.");
            tenantStore = new InMemoryTenantStore();
        }
        tenantResolver = new TenantResolver(tenantStore);
        quotaChecker = new QuotaChecker();
        LOG.info("Phase: tenant — initialized");
    }

    void startOperations(CoordinatorBootstrapConfig config) {
        if (!config.operationsEnabled()) {
            LOG.info("Phase skipped: operations not enabled");
            return;
        }
        recoveryPlaybook = new org.pangrova.operations.recovery.RecoveryPlaybookFramework();
        RecoveryActions.registerAll(recoveryPlaybook);
        LOG.info("Phase: operations — initialized (RecoveryPlaybookFramework with 3 actions)");
    }

    // -- Accessors --

    CdcLifecycleManager getCdcLifecycleManager() { return cdcRegistrationHandler; }
    MaintenanceScheduler getMaintenanceScheduler() { return maintenanceScheduler; }
    StreamingWiring getStreamingWiring() { return streamingModule; }
    LineageEventEmitter getLineageEmitter() { return lineageEmitter; }
    CircuitBreakerPolicy getCircuitBreakerPolicy() { return circuitBreaker; }
    org.pangrova.resilience.degradation.DegradationManager getDegradationManager() { return degradationManager; }
    org.pangrova.resilience.retry.StageRetryManager getStageRetryManager() { return stageRetryManager; }
    TenantResolver getTenantResolver() { return tenantResolver; }
    QuotaChecker getQuotaChecker() { return quotaChecker; }
    org.pangrova.operations.recovery.RecoveryPlaybookFramework getRecoveryPlaybook() { return recoveryPlaybook; }
    IndexingBootstrap getIndexingBootstrap() { return indexingBootstrap; }
    IndexQueryBootstrap getIndexQueryBootstrap() { return indexQueryBootstrap; }

    void startIndexQuery(org.pangrova.metadata.index.IndexMetadataStore metadataStore,
                         org.pangrova.spi.observability.MetricsProvider metricsProvider) {
        try {
            indexQueryBootstrap = new IndexQueryBootstrap(metadataStore, metricsProvider, 5000, 0);
            LOG.info("Phase: index-query — initialized (query-only split filter)");
        } catch (Exception e) {
            LOG.warn("event=index_query_start_failed error={} action=skip_index_filtering",
                    e.getMessage(), e);
            indexQueryBootstrap = null;
        }
    }

    void startIndexing(org.pangrova.metadata.index.IndexMetadataStore metadataStore,
                       org.pangrova.metadata.catalog.CatalogRegistry catalogRegistry,
                       org.pangrova.spi.observability.MetricsProvider metricsProvider,
                       org.pangrova.spi.security.AccessControl accessControl,
                       String baseIndexPath,
                       org.pangrova.spi.coordination.DistributedLease distributedLease) {
        try {
            indexingBootstrap = new IndexingBootstrap(baseIndexPath, distributedLease);
            indexingBootstrap.start(metadataStore, catalogRegistry, metricsProvider, accessControl);
            LOG.info("Phase: indexing — initialized");
        } catch (Exception e) {
            LOG.warn("event=indexing_start_failed error={} action=skip_index_filtering",
                    e.getMessage(), e);
            indexingBootstrap = null;
        }
    }

    /**
     * Closes all modules in LIFO order (reverse of startup):
     * operations → tenant → resilience → governance → streaming → indexing.
     */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        stopMetadataSyncSchedule();
        stopInvalidationCleanupSchedule();
        if (metadataSyncScheduler != null) {
            metadataSyncScheduler.shutdownNow();
        }
        if (invalidationCleanupScheduler != null) {
            invalidationCleanupScheduler.shutdownNow();
        }
        closeOperations();
        closeTenant();
        closeResilience();
        closeGovernance();
        closeStreaming();
        closeIndexing();
        closeIndexQuery();
    }

    private void closeModule(AutoCloseable resource, String moduleName) {
        if (resource == null) return;
        try {
            resource.close();
            LOG.info("event=module_closed module={}", moduleName);
        } catch (Exception e) {
            LOG.warn("event=module_close_failed module={} error={}", moduleName, e.getMessage(), e);
        }
    }

    private void closeIndexing() { closeModule(indexingBootstrap, "indexing"); }

    private void closeIndexQuery() { closeModule(indexQueryBootstrap, "index_query"); }

    private void closeStreaming() { closeModule(streamingModule, "streaming"); }

    private void closeGovernance() { closeModule(governanceModule, "governance"); }

    private void closeResilience() {
        if (degradationManager == null && stageRetryManager == null) return;
        degradationManager = null;
        stageRetryManager = null;
        circuitBreaker = null;
        LOG.info("event=module_closed module=resilience");
    }

    private void closeTenant() {
        if (tenantResolver == null && quotaChecker == null) return;
        tenantResolver = null;
        quotaChecker = null;
        LOG.info("event=module_closed module=tenant");
    }

    private void closeOperations() {
        if (recoveryPlaybook == null) return;
        recoveryPlaybook = null;
        LOG.info("event=module_closed module=operations");
    }

    private static MaintenanceScheduler buildMaintenanceScheduler() {
        return AbsentTableMaintenance.createDefaultScheduler();
    }
}
