package org.pangrova.server.coordinator.bootstrap;
import org.pangrova.persistence.catalog.postgresql.invalidation.PostgresInvalidationPoller;

import org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory;

import org.pangrova.persistence.table.postgresql.maintenance.JdbcTableControlMaintenanceStore;
import org.pangrova.persistence.table.memory.maintenance.InMemoryTableControlMaintenanceStore;

import org.pangrova.persistence.generation.api.publication.GenerationPublicationStore;

import org.pangrova.persistence.generation.api.job.GenerationJobStore;

import org.pangrova.governance.core.GovernanceDeps;
import org.pangrova.governance.core.GovernanceContext;
import org.pangrova.persistence.governance.postgresql.JdbcGovernanceAuditTrail;
import org.pangrova.governance.core.classification.ColumnTagService;
import org.pangrova.governance.core.lineage.LineageEventEmitter;
import org.pangrova.governance.lineage.LineageBackendConfiguration;
import org.pangrova.governance.core.quality.CronScheduler;
import org.pangrova.governance.core.quality.AlertChannelRegistry;
import org.pangrova.governance.core.quality.DataQualityAlerter;
import org.pangrova.governance.core.quality.LogAlertChannel;
import org.pangrova.governance.quality.executor.unconfigured.UnconfiguredQualityRuleExecutor;
import org.pangrova.governance.core.quality.GovernanceStatementHandler;
import org.pangrova.governance.api.quality.QualityRuleStore;
import org.pangrova.coordinator.resilience.resilience4j.ResilienceRegistry;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.persistence.streaming.postgresql.JdbcCdcRegistrationStore;
import org.pangrova.streaming.api.cdc.CdcLifecycleManager;
import org.pangrova.streaming.api.cdc.CdcRegistrationStore;
import org.pangrova.streaming.pipeline.StreamingContext;
import org.pangrova.streaming.cdc.CdcRegistrationHandler;
import org.pangrova.streaming.store.memory.InMemoryCdcRegistrationStore;
import org.pangrova.streaming.maintenance.MaintenanceScheduler;
import org.pangrova.persistence.streaming.postgresql.JdbcWatermarkPositionStore;
import org.pangrova.streaming.api.watermark.WatermarkStore;
import org.pangrova.streaming.store.memory.InMemoryWatermarkStore;
import org.pangrova.streaming.watermark.WatermarkTracker;
import org.pangrova.security.core.tenant.QuotaChecker;
import org.pangrova.security.core.tenant.TenantResolver;
import org.pangrova.persistence.security.memory.tenant.InMemoryTenantStore;
import org.pangrova.coordination.spi.leadership.MaintenanceLeaderAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;

/** Wires optional modules into the coordinator lifecycle. */
final class ModuleIntegrationBootstrap implements MaintenanceLeaderAware, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ModuleIntegrationBootstrap.class);
    private static final long INDEX_EVAL_TIMEOUT_MS = 5_000;

    private volatile boolean closed;

    private volatile boolean maintenanceLeader;

    // Streaming
    private StreamingContext streamingModule;
    private CdcRegistrationHandler cdcRegistrationHandler;
    private MaintenanceScheduler maintenanceScheduler;
    private org.pangrova.streaming.maintenance.MaintenanceTableSource maintenanceTableSource;

    // Governance
    private GovernanceContext governanceModule;
    private org.pangrova.governance.api.quality.WriteQualityGate writeQualityGate;
    private LineageEventEmitter lineageEmitter;

    // Resilience
    private ResilienceRegistry resilienceRegistry;

    // Tenant
    private TenantResolver tenantResolver;
    private QuotaChecker quotaChecker;
    private org.pangrova.persistence.security.api.tenant.TenantStore tenantStore;

    // Operations
    private org.pangrova.operations.recovery.RecoveryPlaybookFramework recoveryPlaybook;

    // Indexing
    private IndexingBootstrap indexingBootstrap;
    private IndexQueryBootstrap indexQueryBootstrap;

    // Streaming bootstrap (set externally after build)
    private StreamingBootstrap streamingBootstrap;

    // AI infrastructure maintenance
    private org.pangrova.coordinator.ai.branch.BranchTtlCleanupTask branchTtlCleanupTask;

    // Metadata sync (leader-gated periodic sync for federation catalogs)
    private org.pangrova.metadata.outbox.MetadataSyncService metadataSyncService;
    private org.pangrova.metadata.outbox.TableVersionObserver versionObserver;
    private org.pangrova.metadata.catalog.registry.CatalogRegistry catalogRegistry;
    private ScheduledExecutorService metadataSyncScheduler;
    private volatile ScheduledFuture<?> metadataSyncTask;

    // Invalidation log cleanup (leader-gated)
    private org.pangrova.persistence.catalog.postgresql.invalidation.PostgresInvalidationPoller invalidationPoller;
    private volatile ScheduledFuture<?> invalidationCleanupTask;

    // Distributed lease (shared across indexing and DT refresh)
    private org.pangrova.persistence.coordination.api.lease.DistributedLease distributedLease;

    private static final long METADATA_SYNC_INTERVAL_SECONDS = 60;

    @Override
    public void onLeaderElected() {
        maintenanceLeader = true;
        startMaintenanceSchedule();
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

    @Override
    public void onLeadershipLost() {
        maintenanceLeader = false;
        stopMaintenanceSchedule();
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

    private boolean isMaintenanceLeader() {
        return maintenanceLeader;
    }

    void setMetadataSyncDeps(org.pangrova.metadata.outbox.MetadataSyncService syncService,
                             org.pangrova.metadata.catalog.registry.CatalogRegistry catalog) {
        this.metadataSyncService = syncService;
        this.catalogRegistry = catalog;
    }

    /** Observes LINKED/MANAGED versions on the same leader-gated cadence as sync. */
    void setVersionObserver(org.pangrova.metadata.outbox.TableVersionObserver observer) {
        this.versionObserver = observer;
    }

    void setInvalidationPoller(PostgresInvalidationPoller poller) {
        this.invalidationPoller = poller;
    }

    org.pangrova.persistence.catalog.postgresql.invalidation.PostgresInvalidationPoller getInvalidationPoller() {
        return invalidationPoller;
    }

    private void startMetadataSyncSchedule() {
        if (catalogRegistry == null
                || (metadataSyncService == null && versionObserver == null)) {
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
        try {
            org.pangrova.metadata.catalog.registry.CatalogRegistry.DivergenceReport report =
                    catalogRegistry.reconcileFromStore();
            if (!report.isEmpty()) {
                LOG.info("event=periodic_catalog_reconcile loaded={} removed={} loadFailed={}",
                        report.loaded().size(), report.removed().size(), report.loadFailed().size());
            }
        } catch (Exception e) {
            LOG.warn("event=periodic_catalog_reconcile_failed error={}", e.getMessage(), e);
        }

        if (metadataSyncService != null) {
            for (String catalogName : catalogRegistry.listCatalogNames()) {
                try {
                    org.pangrova.metadata.api.catalog.CatalogHandle handle = catalogRegistry.getCatalog(catalogName);
                    if (!(handle
                            instanceof org.pangrova.metadata.federation.FederationCatalogHandle fedHandle)) {
                        continue;
                    }
                    org.pangrova.metadata.api.catalog.FederatedCatalog adapter =
                            fedHandle.getFederatedCatalog();
                    metadataSyncService.syncWithRetry(fedHandle.getDescriptor(), adapter);
                } catch (Exception e) {
                    LOG.warn("event=metadata_sync_catalog_failed catalog={} error={}",
                            catalogName, e.getMessage(), e);
                }
            }
        }
        if (versionObserver != null) {
            for (org.pangrova.metadata.outbox.TableVersionObservationReport report
                    : versionObserver.observeAll()) {
                if (report.versionChanges() > 0 || report.identityChanges() > 0
                        || report.missing() > 0 || report.hasFailures()) {
                    LOG.info("event=table_version_observed catalog={} checked={} "
                                    + "versionChanges={} identityChanges={} missing={} "
                                    + "unsupported={} errors={}",
                            report.catalogName(), report.checked(), report.versionChanges(),
                            report.identityChanges(), report.missing(), report.unsupported(),
                            report.errors().size());
                }
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

    /** Supplies the catalog authority required by governance assembly and streaming. */
    void setCatalogRegistry(org.pangrova.metadata.catalog.registry.CatalogRegistry catalogs) {
        this.catalogRegistry = catalogs;
    }

    void setStreamingBootstrap(StreamingBootstrap streamingBootstrap) {
        this.streamingBootstrap = streamingBootstrap;
    }

    void setBranchTtlCleanupTask(
            org.pangrova.coordinator.ai.branch.BranchTtlCleanupTask cleanupTask) {
        this.branchTtlCleanupTask = cleanupTask;
    }

    StreamingBootstrap getStreamingBootstrap() {
        return streamingBootstrap;
    }

    void startStreaming(CoordinatorBootstrapConfig config, DataSource dataSource) {
        startStreaming(config, dataSource, catalogRegistry);
    }

    void startStreaming(CoordinatorBootstrapConfig config, DataSource dataSource,
                        org.pangrova.metadata.catalog.registry.CatalogRegistry catalogs) {
        if (!config.streamingEnabled()) {
            LOG.info("Phase skipped: streaming not enabled");
            return;
        }
        this.catalogRegistry = catalogs;
        MaintenanceScheduler scheduler = buildMaintenanceScheduler(catalogs, dataSource, config);
        this.maintenanceScheduler = scheduler;
        WatermarkStore watermarkStore = dataSource != null
                ? new JdbcWatermarkPositionStore(dataSource) : new InMemoryWatermarkStore();
        WatermarkTracker tracker = new WatermarkTracker(watermarkStore);
        streamingModule = new StreamingContext(scheduler, tracker);
        CdcRegistrationStore cdcStore = createCdcRegistrationStore(dataSource);
        cdcRegistrationHandler = new CdcRegistrationHandler(cdcStore);
        LOG.info("Phase: streaming — initialized (watermarkStore={} cdcRegistrationStore={} "
                        + "maintenanceSource={} periodicScanLeaderGated=true)",
                watermarkStore.getClass().getSimpleName(), cdcStore.getClass().getSimpleName(),
                maintenanceTableSource.getClass().getSimpleName());
    }

    private void startMaintenanceSchedule() {
        if (maintenanceScheduler == null || maintenanceTableSource == null) {
            return;
        }
        maintenanceScheduler.startPeriodicScan(maintenanceTableSource);
        LOG.info("event=table_maintenance_schedule_started");
    }

    private void stopMaintenanceSchedule() {
        if (maintenanceScheduler != null) {
            maintenanceScheduler.stopPeriodicScan();
        }
    }

    private static CdcRegistrationStore createCdcRegistrationStore(DataSource dataSource) {
        if (dataSource == null) {
            return new InMemoryCdcRegistrationStore();
        }
        JdbcCdcRegistrationStore jdbcStore = new JdbcCdcRegistrationStore(dataSource);
        return jdbcStore;
    }

    void startGovernance(CoordinatorBootstrapConfig config, DataSource dataSource) {
        if (!config.governanceEnabled()) {
            LOG.info("Phase skipped: governance not enabled");
            return;
        }
        if (dataSource == null) {
            throw new IllegalStateException("Enabled governance requires the durable metadata datasource");
        }
        QualityRuleStore ruleStore = org.pangrova.metadata.governance.QualityRuleStoreWiring
                .tableControl(
                        org.pangrova.persistence.governance.postgresql.QualityRuleStores.jdbc(dataSource),
                        java.util.Objects.requireNonNull(catalogRegistry, "catalogRegistry"));
        lineageEmitter = LineageBackendConfiguration.createEmitter(
                new org.pangrova.persistence.query.postgresql.lineage.JdbcQueryLineageOutbox(dataSource));
        AlertChannelRegistry channelRegistry = new AlertChannelRegistry();
        channelRegistry.register(new LogAlertChannel());
        DataQualityAlerter alerter = new DataQualityAlerter(channelRegistry);
        org.pangrova.governance.api.quality.QualityRuleExecutor qualityExecutor =
                new UnconfiguredQualityRuleExecutor();
        GovernanceStatementHandler handler = new GovernanceStatementHandler(
                ruleStore, qualityExecutor, alerter);
        CronScheduler cronScheduler = new CronScheduler(handler, this::isMaintenanceLeader,
                new JdbcGovernanceAuditTrail(dataSource));
        ColumnTagService columnTagService = new ColumnTagService(table ->
                java.util.Objects.requireNonNull(catalogRegistry, "catalogRegistry").tableGovernance()
                        .requireBindable(table.catalog(), table.schema(), table.table()).tableId());
        governanceModule = new GovernanceContext(new GovernanceDeps(
                lineageEmitter, handler, null, cronScheduler, ruleStore, columnTagService));
        try {
            governanceModule.initialize();
        } catch (RuntimeException failure) {
            // A failed assembly must not leave the just-created lineage consumer unowned.
            LineageEventEmitter emitter = lineageEmitter;
            lineageEmitter = null;
            if (emitter != null) {
                try {
                    emitter.close();
                } catch (RuntimeException cleanup) {
                    failure.addSuppressed(cleanup);
                }
            }
            throw failure;
        }
        writeQualityGate = writeQualityGateEnabled()
                ? new org.pangrova.governance.core.quality.RuleBasedWriteQualityGate(
                        ruleStore, qualityExecutor)
                : null;
        LOG.info("event=governance_initialized ruleStore={} stableTagAuthority=true writeQualityGate={}",
                ruleStore.getClass().getSimpleName(), writeQualityGate != null);
    }

    /** Pre-commit quality gate; null when governance is disabled or the gate is turned off. */
    org.pangrova.governance.api.quality.WriteQualityGate getWriteQualityGate() {
        return writeQualityGate;
    }

    private static boolean writeQualityGateEnabled() {
        return Boolean.parseBoolean(System.getProperty(
                "pangrova.governance.write-quality-gate.enabled", "true"));
    }

    void startResilience(CoordinatorBootstrapConfig config) {
        if (!config.resilienceEnabled()) {
            LOG.info("Phase skipped: resilience not enabled");
            return;
        }
        java.io.InputStream configStream = getClass().getClassLoader()
                .getResourceAsStream("resilience-defaults.yml");
        java.util.Map<String, org.pangrova.common.resilience.policy.ResiliencePolicyConfig> policies =
                org.pangrova.coordinator.resilience.config.ResilienceConfigLoader.load(configStream);
        resilienceRegistry = new ResilienceRegistry(policies);
        LOG.info("Phase: resilience — initialized (ResilienceRegistry with {} services)", policies.size());
    }

    void startTenant(CoordinatorBootstrapConfig config, DataSource dataSource) {
        if (!config.tenantEnabled()) {
            LOG.info("Phase skipped: tenant not enabled");
            return;
        }
        if (dataSource != null) {
            tenantStore = new org.pangrova.persistence.security.postgresql.tenant.JdbcTenantStore(dataSource);
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

    public org.pangrova.persistence.security.api.tenant.TenantStore getTenantStore() {
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

    CdcLifecycleManager getCdcLifecycleManager() { return cdcRegistrationHandler; }
    MaintenanceScheduler getMaintenanceScheduler() { return maintenanceScheduler; }
    StreamingContext getStreamingContext() { return streamingModule; }
    LineageEventEmitter getLineageEmitter() { return lineageEmitter; }

    /** Shared tag event source; null when governance is disabled. */
    ColumnTagService getColumnTagService() {
        return governanceModule != null ? governanceModule.columnTagService() : null;
    }
    CircuitBreakerPolicy getCircuitBreakerPolicy() { return resilienceRegistry; }
    ResilienceRegistry getResilienceRegistry() { return resilienceRegistry; }
    TenantResolver getTenantResolver() { return tenantResolver; }
    QuotaChecker getQuotaChecker() { return quotaChecker; }
    org.pangrova.operations.recovery.RecoveryPlaybookFramework getRecoveryPlaybook() { return recoveryPlaybook; }
    IndexingBootstrap getIndexingBootstrap() { return indexingBootstrap; }
    IndexQueryBootstrap getIndexQueryBootstrap() { return indexQueryBootstrap; }
    org.pangrova.persistence.coordination.api.lease.DistributedLease getDistributedLease() { return distributedLease; }

    void setDistributedLease(org.pangrova.persistence.coordination.api.lease.DistributedLease lease) {
        this.distributedLease = lease;
    }

    void startIndexQuery(org.pangrova.persistence.indexing.api.metadata.IndexMetadataStore metadataStore,
                         org.pangrova.observability.spi.metrics.MetricsProvider metricsProvider) {
        try {
            indexQueryBootstrap = new IndexQueryBootstrap(
                    metadataStore, metricsProvider, INDEX_EVAL_TIMEOUT_MS, 0);
            LOG.info("Phase: index-query — initialized (query-only split filter)");
        } catch (Exception e) {
            LOG.warn("event=index_query_start_failed error={} action=skip_index_filtering",
                    e.getMessage(), e);
            indexQueryBootstrap = null;
        }
    }

    void startIndexing(org.pangrova.persistence.indexing.api.metadata.IndexMetadataStore metadataStore,
                       org.pangrova.persistence.indexing.api.build.IndexBuildClaimRepository buildClaims,
                       org.pangrova.metadata.catalog.registry.CatalogRegistry catalog,
                       org.pangrova.observability.spi.metrics.MetricsProvider metricsProvider,
                       org.pangrova.security.spi.authorization.AccessControl accessControl,
                       String baseIndexPath,
                       org.pangrova.persistence.coordination.api.lease.DistributedLease lease) {
        startIndexing(metadataStore, buildClaims, catalog, metricsProvider, accessControl,
                baseIndexPath, lease, null);
    }

    void startIndexing(org.pangrova.persistence.indexing.api.metadata.IndexMetadataStore metadataStore,
                       org.pangrova.persistence.indexing.api.build.IndexBuildClaimRepository buildClaims,
                       org.pangrova.metadata.catalog.registry.CatalogRegistry catalog,
                       org.pangrova.observability.spi.metrics.MetricsProvider metricsProvider,
                       org.pangrova.security.spi.authorization.AccessControl accessControl,
                       String baseIndexPath,
                       org.pangrova.persistence.coordination.api.lease.DistributedLease lease,
                       GenerationJobStore artifactBuildStore) {
        startIndexing(metadataStore, buildClaims, catalog, metricsProvider, accessControl,
                baseIndexPath, lease, artifactBuildStore, null);
    }

    void startIndexing(org.pangrova.persistence.indexing.api.metadata.IndexMetadataStore metadataStore,
                       org.pangrova.persistence.indexing.api.build.IndexBuildClaimRepository buildClaims,
                       org.pangrova.metadata.catalog.registry.CatalogRegistry catalog,
                       org.pangrova.observability.spi.metrics.MetricsProvider metricsProvider,
                       org.pangrova.security.spi.authorization.AccessControl accessControl,
                       String baseIndexPath,
                       org.pangrova.persistence.coordination.api.lease.DistributedLease lease,
                       GenerationJobStore artifactBuildStore,
                       GenerationPublicationStore artifactPublicationStore) {
        startIndexing(metadataStore, buildClaims, catalog, metricsProvider, accessControl,
                baseIndexPath, lease, artifactBuildStore,
                artifactPublicationStore, null);
    }

    void startIndexing(org.pangrova.persistence.indexing.api.metadata.IndexMetadataStore metadataStore,
                       org.pangrova.persistence.indexing.api.build.IndexBuildClaimRepository buildClaims,
                       org.pangrova.metadata.catalog.registry.CatalogRegistry catalog,
                       org.pangrova.observability.spi.metrics.MetricsProvider metricsProvider,
                       org.pangrova.security.spi.authorization.AccessControl accessControl,
                       String baseIndexPath,
                       org.pangrova.persistence.coordination.api.lease.DistributedLease lease,
                       GenerationJobStore artifactBuildStore,
                       GenerationPublicationStore artifactPublicationStore,
                       org.pangrova.coordinator.indexing.lifecycle.MaintenanceLeaderElection leaderElection) {
        try {
            indexingBootstrap = new IndexingBootstrap(
                    baseIndexPath, lease, artifactBuildStore,
                    artifactPublicationStore, leaderElection);
            indexingBootstrap.start(metadataStore, buildClaims, catalog, metricsProvider, accessControl);
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
    public synchronized void close() {
        if (closed) {
            return;
        }
        stopMetadataSyncSchedule();
        stopInvalidationCleanupSchedule();
        if (metadataSyncScheduler != null) {
            metadataSyncScheduler.shutdownNow();
        }
        if (invalidationCleanupScheduler != null) {
            invalidationCleanupScheduler.shutdownNow();
        }
        closeModule(invalidationPoller, "metadata_invalidation");
        closeOperations();
        closeTenant();
        closeResilience();
        closeGovernance();
        closeStreaming();
        closeIndexing();
        closeIndexQuery();
        closed = true;
    }

    private void closeModule(AutoCloseable resource, String moduleName) {
        if (resource == null) return;
        try {
            resource.close();
            LOG.info("event=module_closed module={}", moduleName);
        } catch (Exception e) {
            throw new IllegalStateException("Module owner has not actually closed: " + moduleName, e);
        }
    }

    private void closeIndexing() { closeModule(indexingBootstrap, "indexing"); }

    private void closeIndexQuery() { closeModule(indexQueryBootstrap, "index_query"); }

    private void closeStreaming() { closeModule(streamingModule, "streaming"); }

    private void closeGovernance() {
        closeModule(governanceModule, "governance");
        writeQualityGate = null;
    }

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

    private MaintenanceScheduler buildMaintenanceScheduler(
            org.pangrova.metadata.catalog.registry.CatalogRegistry catalogs, DataSource dataSource,
            CoordinatorBootstrapConfig config) {
        if (catalogs == null) {
            // Metadata-less mode has no ownership records. Keep the scheduler idle
            // rather than maintaining tables nobody has proved Pangrova may touch.
            this.maintenanceTableSource = java.util.List::of;
            return AbsentTableMaintenance.createDefaultScheduler();
        }
        org.pangrova.table.core.maintenance.TableControlMaintenanceService policies =
                new org.pangrova.table.core.maintenance.TableControlMaintenanceService(
                        dataSource != null
                                ? new JdbcTableControlMaintenanceStore(dataSource)
                                : new InMemoryTableControlMaintenanceStore());
        org.pangrova.streaming.maintenance.MaintenanceConfig maintenanceConfig =
                org.pangrova.streaming.maintenance.MaintenanceConfig.defaults();
        var executor = new org.pangrova.coordinator.query.statement.ControlledTableMaintenance(
                catalogs, policies, distributedLease, config.coordinatorId(), new OtelExecutionTraceContextFactory());
        this.maintenanceTableSource =
                new org.pangrova.coordinator.maintenance.ControlledTableSource(
                        catalogs.tableControl(), policies);
        return new MaintenanceScheduler(
                new org.pangrova.coordinator.maintenance.ControlledTableMaintenanceSpi(
                        catalogs, policies, executor, maintenanceConfig, new OtelExecutionTraceContextFactory()),
                maintenanceConfig);
    }
}
