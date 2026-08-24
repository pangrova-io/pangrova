package org.pangrova.coordinator.bootstrap;

import org.pangrova.governance.GovernanceDeps;
import org.pangrova.governance.GovernanceContext;
import org.pangrova.governance.classification.InMemoryAuditConfigStore;
import org.pangrova.governance.classification.InMemoryColumnMaskStore;
import org.pangrova.governance.classification.TagBasedSecurityPolicy;
import org.pangrova.governance.lineage.LineageEventEmitter;
import org.pangrova.governance.lineage.LogLineageBackend;
import org.pangrova.governance.quality.schedule.CronScheduler;
import org.pangrova.governance.quality.alert.AlertChannelRegistry;
import org.pangrova.governance.quality.alert.DataQualityAlerter;
import org.pangrova.governance.quality.alert.LogAlertChannel;
import org.pangrova.governance.quality.rule.DefaultQualityRuleExecutor;
import org.pangrova.governance.quality.rule.GovernanceStatementHandler;
import org.pangrova.governance.quality.rule.InMemoryQualityRuleStore;
import org.pangrova.governance.quality.rule.JdbcQualityRuleStore;
import org.pangrova.governance.quality.rule.QualityRuleStore;

import org.pangrova.resilience.ResilienceRegistry;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.metadata.streaming.JdbcCdcRegistrationStore;
import org.pangrova.spi.streaming.CdcLifecycleManager;
import org.pangrova.spi.streaming.CdcRegistrationStore;
import org.pangrova.streaming.StreamingContext;
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
final class ModuleIntegrationBootstrap implements MaintenanceLeaderAware, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ModuleIntegrationBootstrap.class);

    private volatile boolean closed;

    /**
     * Live maintenance-leader flag driven by {@link #onLeaderElected()} /
     * {@link #onLeadershipLost()}. Leader-gated schedulers (e.g. governance quality
     * cron) read this so exactly one coordinator runs periodic maintenance in an
     * Active-Active cluster. Single-node deployments elect themselves leader at
     * startup via the InMemory lease, so this becomes true immediately (F-GOV-CRON).
     */
    private volatile boolean maintenanceLeader;

    // Streaming
    private StreamingContext streamingModule;
    private CdcRegistrationHandler cdcRegistrationHandler;
    private MaintenanceScheduler maintenanceScheduler;

    // Governance
    private GovernanceContext governanceModule;
    private LineageEventEmitter lineageEmitter;

    // Resilience
    private ResilienceRegistry resilienceRegistry;

    // Tenant
    private TenantResolver tenantResolver;
    private QuotaChecker quotaChecker;
    private org.pangrova.security.tenant.store.TenantStore tenantStore;

    // Operations
    private org.pangrova.operations.recovery.RecoveryPlaybookFramework recoveryPlaybook;

    // Indexing
    private IndexingBootstrap indexingBootstrap;
    private IndexQueryBootstrap indexQueryBootstrap;

    // Streaming bootstrap (set externally after build)
    private StreamingBootstrap streamingBootstrap;

    // Metadata sync (leader-gated periodic sync for federation catalogs)
    private org.pangrova.metadata.sync.MetadataSyncService metadataSyncService;
    private org.pangrova.metadata.catalog.CatalogRegistry catalogRegistry;
    private ScheduledExecutorService metadataSyncScheduler;
    private volatile ScheduledFuture<?> metadataSyncTask;

    // Invalidation log cleanup (leader-gated)
    private org.pangrova.metadata.cache.JdbcInvalidationPoller invalidationPoller;
    private volatile ScheduledFuture<?> invalidationCleanupTask;

    // Distributed lease (shared across indexing and DT refresh)
    private org.pangrova.spi.coordination.DistributedLease distributedLease;

    private static final long METADATA_SYNC_INTERVAL_SECONDS = 60;

    // ── Unified leader lifecycle (MaintenanceLeaderAware) ───────────

    /**
     * Called when this node becomes the maintenance leader.
     * Delegates to all leader-aware subsystems: indexing, streaming, metadata sync.
     */
    @Override
    public void onLeaderElected() {
        maintenanceLeader = true;
        if (indexingBootstrap != null) {
            indexingBootstrap.onLeaderElected();
        }
        if (streamingBootstrap != null) {
            streamingBootstrap.onLeaderElected();
        }
        startMetadataSyncSchedule();
        startInvalidationCleanupSchedule();
        LOG.info("event=unified_leader_elected");
    }

    /**
     * Called when this node loses maintenance leadership.
     * Delegates to all leader-aware subsystems: indexing, streaming, metadata sync.
     */
    @Override
    public void onLeadershipLost() {
        maintenanceLeader = false;
        if (indexingBootstrap != null) {
            indexingBootstrap.onLeadershipLost();
        }
        if (streamingBootstrap != null) {
            streamingBootstrap.onLeadershipLost();
        }
        stopMetadataSyncSchedule();
        stopInvalidationCleanupSchedule();
        LOG.info("event=unified_leadership_lost");
    }

    /** Returns whether this coordinator currently holds maintenance leadership. */
    private boolean isMaintenanceLeader() {
        return maintenanceLeader;
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
        // F-META-01: Periodic catalog membership reconciliation ensures catalogs
        // added/dropped by other coordinators are detected even without failover.
        try {
            org.pangrova.metadata.catalog.CatalogRegistry.DivergenceReport report =
                    catalogRegistry.reconcileFromStore();
            if (!report.isEmpty()) {
                LOG.info("event=periodic_catalog_reconcile loaded={} removed={} loadFailed={}",
                        report.loaded().size(), report.removed().size(), report.loadFailed().size());
            }
        } catch (Exception e) {
            LOG.warn("event=periodic_catalog_reconcile_failed error={}", e.getMessage());
        }

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
        streamingModule = new StreamingContext(scheduler, tracker);
        CdcRegistrationStore cdcStore = createCdcRegistrationStore(dataSource);
        cdcRegistrationHandler = new CdcRegistrationHandler(cdcStore);
        // Activate periodic maintenance scan. The table source returns an empty list
        // initially; connectors register their managed tables as catalogs are mounted.
        scheduler.startPeriodicScan(java.util.Collections::emptyList);
        LOG.info("Phase: streaming — initialized (watermarkStore={} cdcRegistrationStore={} periodicScanStarted=true)",
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

    void startGovernance(CoordinatorBootstrapConfig config, DataSource dataSource) {
        if (!config.governanceEnabled()) {
            LOG.info("Phase skipped: governance not enabled");
            return;
        }
        lineageEmitter = new LineageEventEmitter(new LogLineageBackend());
        // F-GOV-CRON: persist quality rules so they survive restart and are visible
        // across all coordinators. Without a datasource we fall back to in-memory
        // (single-node/test only) and warn, mirroring the tenant store policy.
        QualityRuleStore ruleStore;
        if (dataSource != null) {
            ruleStore = new JdbcQualityRuleStore(dataSource);
            LOG.info("Phase: governance — using JDBC-backed quality rule store");
        } else {
            LOG.warn("Phase: governance — no datasource configured; using InMemoryQualityRuleStore. "
                    + "Quality rules are NOT persisted and will not survive restart. "
                    + "Configure a datasource for production governance.");
            ruleStore = new InMemoryQualityRuleStore();
        }
        AlertChannelRegistry channelRegistry = new AlertChannelRegistry();
        channelRegistry.register(new LogAlertChannel());
        DataQualityAlerter alerter = new DataQualityAlerter(channelRegistry);
        GovernanceStatementHandler handler = new GovernanceStatementHandler(
                ruleStore, new DefaultQualityRuleExecutor(), alerter);
        // F-GOV-CRON: leader-gated cron so scheduled quality checks run on exactly
        // one coordinator per interval (prevents duplicate execution/alerts in a
        // multi-coordinator Active-Active cluster). leaderCheck reads the live
        // maintenance-leader flag maintained by onLeaderElected/onLeadershipLost.
        CronScheduler cronScheduler = new CronScheduler(handler, this::isMaintenanceLeader, null);
        TagBasedSecurityPolicy securityPolicy = new TagBasedSecurityPolicy(
                new InMemoryColumnMaskStore(), new InMemoryAuditConfigStore());
        governanceModule = new GovernanceContext(new GovernanceDeps(
                lineageEmitter, handler, securityPolicy, cronScheduler, ruleStore));
        governanceModule.initialize();
        LOG.info("Phase: governance — initialized (ruleStore={} leaderGatedCron=true)",
                ruleStore.getClass().getSimpleName());
    }

    void startResilience(CoordinatorBootstrapConfig config) {
        if (!config.resilienceEnabled()) {
            LOG.info("Phase skipped: resilience not enabled");
            return;
        }
        java.io.InputStream configStream = getClass().getClassLoader()
                .getResourceAsStream("resilience-defaults.yml");
        java.util.Map<String, org.pangrova.resilience.config.ResiliencePolicyConfig> policies =
                org.pangrova.resilience.config.ResilienceConfigLoader.load(configStream);
        resilienceRegistry = new ResilienceRegistry(policies);
        LOG.info("Phase: resilience — initialized (ResilienceRegistry with {} services)", policies.size());
    }

    void startTenant(CoordinatorBootstrapConfig config, DataSource dataSource) {
        if (!config.tenantEnabled()) {
            LOG.info("Phase skipped: tenant not enabled");
            return;
        }
        // F-SEC-02: Wire JdbcTenantStore when a datasource is available for
        // production persistence. InMemoryTenantStore is only acceptable for
        // single-node/test mode where cross-coordinator consistency is not needed.
        if (dataSource != null) {
            tenantStore = new org.pangrova.metadata.tenant.JdbcTenantStore(dataSource);
            LOG.info("Phase: tenant — using JDBC-backed tenant store");
        } else {
            LOG.warn("Phase: tenant — no datasource configured; using InMemoryTenantStore. "
                    + "Tenant state is NOT persisted and will not survive restart. "
                    + "Configure a datasource for production multi-tenancy.");
            tenantStore = new InMemoryTenantStore();
        }
        tenantResolver = new TenantResolver(tenantStore);
        quotaChecker = new QuotaChecker();
        LOG.info("Phase: tenant — initialized");
    }

    /** Returns the tenant store (JDBC or in-memory) for cluster admission wiring. */
    public org.pangrova.security.tenant.store.TenantStore getTenantStore() {
        return tenantStore;
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
    StreamingContext getStreamingContext() { return streamingModule; }
    LineageEventEmitter getLineageEmitter() { return lineageEmitter; }
    CircuitBreakerPolicy getCircuitBreakerPolicy() { return resilienceRegistry; }
    ResilienceRegistry getResilienceRegistry() { return resilienceRegistry; }
    TenantResolver getTenantResolver() { return tenantResolver; }
    QuotaChecker getQuotaChecker() { return quotaChecker; }
    org.pangrova.operations.recovery.RecoveryPlaybookFramework getRecoveryPlaybook() { return recoveryPlaybook; }
    IndexingBootstrap getIndexingBootstrap() { return indexingBootstrap; }
    IndexQueryBootstrap getIndexQueryBootstrap() { return indexQueryBootstrap; }
    org.pangrova.spi.coordination.DistributedLease getDistributedLease() { return distributedLease; }

    void setDistributedLease(org.pangrova.spi.coordination.DistributedLease distributedLease) {
        this.distributedLease = distributedLease;
    }

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
        if (resilienceRegistry == null) return;
        resilienceRegistry = null;
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
