package org.pangrova.server.coordinator.bootstrap;

import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.coordination.api.leadership.MaintenanceLeaderAware;
import org.pangrova.coordinator.resilience.resilience4j.ResilienceRegistry;
import org.pangrova.governance.api.quality.QualityRuleStore;
import org.pangrova.governance.runtime.GovernanceContext;
import org.pangrova.governance.runtime.GovernanceDeps;
import org.pangrova.governance.runtime.classification.ColumnTagService;
import org.pangrova.governance.runtime.lineage.LineageEventEmitter;
import org.pangrova.governance.runtime.quality.AlertChannelRegistry;
import org.pangrova.governance.runtime.quality.CronScheduler;
import org.pangrova.governance.runtime.quality.DataQualityAlerter;
import org.pangrova.governance.runtime.quality.GovernanceStatementHandler;
import org.pangrova.governance.runtime.quality.LogAlertChannel;
import org.pangrova.governance.runtime.quality.UnconfiguredQualityRuleExecutor;
import org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory;
import org.pangrova.persistence.generation.api.job.GenerationJobStore;
import org.pangrova.persistence.generation.api.publication.GenerationPublicationStore;
import org.pangrova.persistence.governance.postgresql.JdbcGovernanceAuditTrail;
import org.pangrova.persistence.security.memory.tenant.InMemoryTenantStore;
import org.pangrova.persistence.streaming.memory.InMemoryCdcRegistrationStore;
import org.pangrova.persistence.streaming.memory.InMemoryWatermarkStore;
import org.pangrova.persistence.streaming.postgresql.JdbcCdcRegistrationStore;
import org.pangrova.persistence.streaming.postgresql.JdbcWatermarkPositionStore;
import org.pangrova.security.tenant.QuotaChecker;
import org.pangrova.security.tenant.TenantResolver;
import org.pangrova.common.lifecycle.ResourceCloser;
import org.pangrova.streaming.api.cdc.CdcLifecycleManager;
import org.pangrova.streaming.api.cdc.CdcRegistrationStore;
import org.pangrova.streaming.api.watermark.WatermarkStore;
import org.pangrova.streaming.cdc.CdcRegistrationHandler;
import org.pangrova.streaming.maintenance.MaintenanceScheduler;
import org.pangrova.streaming.pipeline.StreamingContext;
import org.pangrova.streaming.watermark.WatermarkTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import javax.sql.DataSource;

/** Wires optional modules into the coordinator lifecycle. */
final class ModuleIntegrationBootstrap implements MaintenanceLeaderAware, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ModuleIntegrationBootstrap.class);
    private static final long INDEX_EVAL_TIMEOUT_MS = 5_000;

    private boolean closing;
    private boolean closed;

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
    private org.pangrova.operations.recovery.RecoveryPlaybook recoveryPlaybook;

    // Indexing
    private IndexingBootstrap indexingBootstrap;
    private IndexQueryBootstrap indexQueryBootstrap;

    // Streaming bootstrap (set externally after build)
    private StreamingBootstrap streamingBootstrap;

    // AI infrastructure maintenance
    private org.pangrova.coordinator.ai.branch.BranchTtlCleanupTask branchTtlCleanupTask;

    private org.pangrova.metadata.catalog.registry.CatalogRegistry catalogRegistry;
    private final CatalogMaintenance catalogMaintenance;

    ModuleIntegrationBootstrap() {
        catalogMaintenance = new CatalogMaintenance();
    }

    ModuleIntegrationBootstrap(ScheduledExecutorService cleanupExecutor) {
        catalogMaintenance = new CatalogMaintenance(cleanupExecutor);
    }

    // Distributed lease (shared across indexing and DT refresh)
    private org.pangrova.persistence.coordination.api.lease.DistributedLease distributedLease;

    @Override
    public synchronized void onLeaderElected() {
        if (closing || maintenanceLeader) {
            return;
        }
        maintenanceLeader = true;
        try {
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
            catalogMaintenance.start();
        } catch (RuntimeException | Error failure) {
            maintenanceLeader = false;
            stopLeaderServices(failure);
            throw failure;
        }
        LOG.info("event=unified_leader_elected");
    }

    @Override
    public synchronized void onLeadershipLost() {
        maintenanceLeader = false;
        stopLeaderServices(null);
        LOG.info("event=unified_leadership_lost");
    }

    private void stopLeaderServices(Throwable failure) {
        ResourceCloser.closeAll(failure, catalogMaintenance::stop,
                ResourceCloser.present(branchTtlCleanupTask, task -> task.stop()),
                ResourceCloser.present(streamingBootstrap, StreamingBootstrap::onLeadershipLost),
                ResourceCloser.present(indexingBootstrap, IndexingBootstrap::onLeadershipLost),
                this::stopMaintenanceSchedule);
    }

    private boolean isMaintenanceLeader() {
        return maintenanceLeader;
    }

    CatalogMaintenance catalogMaintenance() {
        return catalogMaintenance;
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
        MaintenanceScheduler scheduler = buildMaintenanceScheduler(catalogs, config);
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

    void startGovernance(CoordinatorBootstrapConfig config, DataSource dataSource, ColumnTagService columnTagService) {
        if (!config.governanceEnabled()) {
            LOG.info("Phase skipped: governance not enabled");
            return;
        }
        if (dataSource == null) {
            throw new IllegalStateException("Enabled governance requires the durable metadata datasource");
        }
        QualityRuleStore ruleStore = new org.pangrova.metadata.governance.TableControlQualityRuleStore(
                java.util.Objects.requireNonNull(catalogRegistry, "catalogRegistry").tableGovernance(),
                org.pangrova.persistence.governance.postgresql.QualityRuleStores.jdbc(dataSource));
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
        governanceModule = new GovernanceContext(new GovernanceDeps(
                lineageEmitter, handler, null, cronScheduler, ruleStore, columnTagService));
        try {
            governanceModule.initialize();
        } catch (RuntimeException | Error failure) {
            ResourceCloser.closeAll(failure, this::closeGovernance);
            throw failure;
        }
        writeQualityGate = writeQualityGateEnabled()
                ? new org.pangrova.governance.runtime.quality.RuleBasedWriteQualityGate(
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
        recoveryPlaybook = new org.pangrova.operations.recovery.RecoveryPlaybook();
        LOG.info("Phase: operations — initialized (unhandled incidents are escalated)");
    }

    CdcLifecycleManager getCdcLifecycleManager() { return cdcRegistrationHandler; }
    MaintenanceScheduler getMaintenanceScheduler() { return maintenanceScheduler; }
    StreamingContext getStreamingContext() { return streamingModule; }
    LineageEventEmitter getLineageEmitter() { return lineageEmitter; }

    CircuitBreakerPolicy getCircuitBreakerPolicy() { return resilienceRegistry; }
    ResilienceRegistry getResilienceRegistry() { return resilienceRegistry; }
    TenantResolver getTenantResolver() { return tenantResolver; }
    QuotaChecker getQuotaChecker() { return quotaChecker; }
    org.pangrova.operations.recovery.RecoveryPlaybook getRecoveryPlaybook() { return recoveryPlaybook; }
    IndexingBootstrap getIndexingBootstrap() { return indexingBootstrap; }
    IndexQueryBootstrap getIndexQueryBootstrap() { return indexQueryBootstrap; }
    org.pangrova.persistence.coordination.api.lease.DistributedLease getDistributedLease() { return distributedLease; }

    void setDistributedLease(org.pangrova.persistence.coordination.api.lease.DistributedLease lease) {
        this.distributedLease = lease;
    }

    void startIndexQuery(org.pangrova.persistence.indexing.api.metadata.IndexMetadataStore metadataStore,
                         org.pangrova.observability.api.metrics.MetricsProvider metricsProvider) {
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
                       org.pangrova.observability.api.metrics.MetricsProvider metricsProvider,
                       org.pangrova.security.api.authorization.AccessControl accessControl,
                       String baseIndexPath,
                       org.pangrova.persistence.coordination.api.lease.DistributedLease lease) {
        startIndexing(metadataStore, buildClaims, catalog, metricsProvider, accessControl,
                baseIndexPath, lease, null);
    }

    void startIndexing(org.pangrova.persistence.indexing.api.metadata.IndexMetadataStore metadataStore,
                       org.pangrova.persistence.indexing.api.build.IndexBuildClaimRepository buildClaims,
                       org.pangrova.metadata.catalog.registry.CatalogRegistry catalog,
                       org.pangrova.observability.api.metrics.MetricsProvider metricsProvider,
                       org.pangrova.security.api.authorization.AccessControl accessControl,
                       String baseIndexPath,
                       org.pangrova.persistence.coordination.api.lease.DistributedLease lease,
                       GenerationJobStore artifactBuildStore) {
        startIndexing(metadataStore, buildClaims, catalog, metricsProvider, accessControl,
                baseIndexPath, lease, artifactBuildStore, null);
    }

    void startIndexing(org.pangrova.persistence.indexing.api.metadata.IndexMetadataStore metadataStore,
                       org.pangrova.persistence.indexing.api.build.IndexBuildClaimRepository buildClaims,
                       org.pangrova.metadata.catalog.registry.CatalogRegistry catalog,
                       org.pangrova.observability.api.metrics.MetricsProvider metricsProvider,
                       org.pangrova.security.api.authorization.AccessControl accessControl,
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
                       org.pangrova.observability.api.metrics.MetricsProvider metricsProvider,
                       org.pangrova.security.api.authorization.AccessControl accessControl,
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
        closing = true;
        maintenanceLeader = false;
        ResourceCloser.closeAll(null,
                catalogMaintenance::close, this::closeOperations, this::closeTenant,
                this::closeResilience, this::closeGovernance, this::closeStreaming,
                this::closeIndexing, this::closeIndexQuery);
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

    private void closeIndexing() {
        closeModule(indexingBootstrap, "indexing");
        indexingBootstrap = null;
    }

    private void closeIndexQuery() {
        closeModule(indexQueryBootstrap, "index_query");
        indexQueryBootstrap = null;
    }

    private void closeStreaming() {
        closeModule(streamingModule, "streaming");
        streamingModule = null;
        maintenanceScheduler = null;
        maintenanceTableSource = null;
        cdcRegistrationHandler = null;
    }

    private void closeGovernance() {
        closeModule(governanceModule, "governance");
        governanceModule = null;
        lineageEmitter = null;
        writeQualityGate = null;
    }

    private void closeResilience() {
        if (resilienceRegistry == null) return;
        resilienceRegistry = null;
        LOG.info("event=module_closed module=resilience");
    }

    private void closeTenant() {
        if (tenantResolver == null && quotaChecker == null) return;
        tenantStore = null;
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
            org.pangrova.metadata.catalog.registry.CatalogRegistry catalogs, CoordinatorBootstrapConfig config) {
        if (catalogs == null) {
            // Metadata-less mode has no ownership records. Keep the scheduler idle
            // rather than maintaining tables nobody has proved Pangrova may touch.
            this.maintenanceTableSource = java.util.List::of;
            return AbsentTableMaintenance.createDefaultScheduler();
        }
        org.pangrova.table.maintenance.TableControlMaintenanceService policies =
                catalogs.tableMaintenance();
        org.pangrova.streaming.maintenance.MaintenanceConfig maintenanceConfig =
                org.pangrova.streaming.maintenance.MaintenanceConfig.defaults();
        var executor = new org.pangrova.coordinator.query.statement.ControlledTableMaintenance(
                catalogs, policies, distributedLease, config.coordinatorId(), new OtelExecutionTraceContextFactory());
        this.maintenanceTableSource =
                new org.pangrova.coordinator.maintenance.ControlledTableSource(
                        catalogs.tableControl(), policies);
        return new MaintenanceScheduler(
                new org.pangrova.coordinator.maintenance.CoordinatorTableMaintenance(
                        catalogs, policies, executor, maintenanceConfig, new OtelExecutionTraceContextFactory()),
                maintenanceConfig);
    }
}
