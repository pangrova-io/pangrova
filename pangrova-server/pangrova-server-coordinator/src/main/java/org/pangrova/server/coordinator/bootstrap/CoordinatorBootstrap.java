package org.pangrova.server.coordinator.bootstrap;

import org.pangrova.persistence.security.postgresql.transaction.AuthorizationTransactionGate;
import org.pangrova.coordinator.ai.embedding.GeneratedValueMaterializationCoordinator;
import org.pangrova.persistence.table.api.provider.TableStoreFactory;
import org.pangrova.persistence.table.api.provider.TableStores;
import org.pangrova.persistence.materialization.postgresql.JdbcMaterializationRepository;
import org.pangrova.persistence.materialization.postgresql.JdbcMaterializationCatalog;
import org.pangrova.persistence.indexing.postgresql.build.JdbcIndexBuildClaimRepository;

import org.pangrova.persistence.workflow.postgresql.generation.PostgresGenerationStores;
import org.pangrova.persistence.ai.postgresql.embedding.JdbcCurrentModelAuthority;
import org.pangrova.persistence.jdbc.api.security.model.CurrentModelAuthority;
import org.pangrova.persistence.security.postgresql.policy.JdbcCurrentDirectRoleReader;
import org.pangrova.coordinator.ai.branch.BranchTtlCleanupTask;
import org.pangrova.coordinator.metadata.MetadataBootstrap;
import org.pangrova.common.exception.BootstrapFailedException;
import org.pangrova.common.lifecycle.ProcessIncarnationStore;
import org.pangrova.persistence.query.postgresql.coordination.JdbcCoordinatorWorkRecovery;
import org.pangrova.coordinator.metadata.PluginConnectorHandleFactory;
import org.pangrova.metadata.catalog.federation.PluginFederationHandleFactory;
import org.pangrova.query.runtime.QueryManager;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.persistence.ai.postgresql.assembly.AiInfrastructureStores;
import org.pangrova.persistence.catalog.postgresql.catalog.PostgresCatalogDeploymentRepository;
import org.pangrova.persistence.catalog.api.catalog.CatalogDeploymentRepository;
import org.pangrova.persistence.jdbc.support.PooledDataSourceFactory;
import org.pangrova.persistence.streaming.postgresql.JdbcRunningStreamFeed;
import org.pangrova.persistence.jdbc.support.SchemaMigrationRunner;
import org.pangrova.security.api.secret.SecretResolver;
import org.pangrova.security.secret.resolution.SecretResolverRegistry;
import org.pangrova.observability.api.tracing.TracingProvider;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.protocol.transport.TransportServerLifecycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.sql.DataSource;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import static org.pangrova.common.util.Preconditions.checkNotNull;
import org.pangrova.persistence.catalog.postgresql.projection.PostgresTableProjectionRepository;
import org.pangrova.persistence.workflow.postgresql.projection.PostgresMetadataProjectionTransactions;
import org.pangrova.persistence.catalog.postgresql.synchronization.PostgresMetadataSyncRepository;

/**
 * Orchestrates the full Coordinator startup/shutdown sequence.
 * Delegates to sub-bootstraps: Observability → Metadata → Security → Modules → Query → HTTP → HA.
 * Shutdown runs in reverse order.
 */
public final class CoordinatorBootstrap {

    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorBootstrap.class);
    private static final java.time.Duration EXECUTION_OWNER_LEASE_TTL = java.time.Duration.ofSeconds(30);

    private final CoordinatorBootstrapConfig config;
    private final Path admissionStateDirectory;
    private final CurrentModelAuthority modelAuthority =
            new JdbcCurrentModelAuthority(new JdbcCurrentDirectRoleReader());
    private final PostgresGenerationStores artifactStores = new PostgresGenerationStores(modelAuthority,
            CoordinatorTableWriteAuthority.create());
    private final org.pangrova.server.coordinator.bootstrap.lifecycle.CoordinatorLifecycleAdmission lifecycle =
            new org.pangrova.server.coordinator.bootstrap.lifecycle.CoordinatorLifecycleAdmission();
    private final CountDownLatch shutdownCompleted = new CountDownLatch(1);

    private ProcessIncarnationStore processStore;
    private SecretResolver secretResolver;
    private ObservabilityBootstrap observability;
    private MetadataBootstrap metadataBootstrap;
    private PluginConnectorHandleFactory connectorHandleFactory;
    private NativeDatasetControlBootstrap nativeDatasetControl;
    private DataSource metadataDataSource;
    private org.pangrova.persistence.coordination.api.membership.CoordinatorRegistry coordinatorRegistry;
    private AiInfrastructureStores aiInfrastructureStores;
    private org.pangrova.persistence.coordination.api.lease.DistributedLease distributedLease;
    private org.pangrova.persistence.coordination.api.provider.CoordinationProvider coordinationProvider;
    private SecurityBootstrap security;
    private ModuleIntegrationBootstrap moduleWiring;
    private StreamingBootstrap streamingBootstrap;
    private JournaledCdcRuntime cdcRuntime;
    private QueryBootstrap query;
    private HttpBootstrap http;
    private HighAvailabilityBootstrap ha;
    private org.pangrova.coordinator.indexing.lifecycle.MaintenanceLeaderElection maintenanceLeaderElection;
    private MaintenanceLeadershipBootstrap maintenanceLeadership;
    private BranchTtlCleanupTask branchTtlCleanupTask;
    private org.pangrova.coordinator.coordination.CoordinatorExecutionAuthority executionAuthority;

    public CoordinatorBootstrap(CoordinatorBootstrapConfig config) {
        this(config, null);
    }

    /** Configures the durable local ownership domain for Coordinator-hosted statements. */
    public CoordinatorBootstrap(CoordinatorBootstrapConfig config, Path admissionStateDirectory) {
        this.config = checkNotNull(config, "config must not be null");
        if (admissionStateDirectory != null && admissionStateDirectory.toString().isBlank()) {
            throw new IllegalArgumentException("Coordinator admission state directory must not be blank");
        }
        this.admissionStateDirectory = admissionStateDirectory;
    }

    /** Starts configured subsystems and rolls back all owned resources if any phase fails. */
    public void start() {
        lifecycle.beginStart();
        boolean succeeded = false;
        try {
            startOwned();
            succeeded = true;
        } finally {
            lifecycle.endStart(succeeded);
        }
    }

    private void startOwned() {
        config.requireAuthenticatedTenancy();
        LOG.info("event=coordinator_bootstrap_start coordinatorId={} role={}",
                config.coordinatorId(), config.role());
        initSecretResolver();
        try {
            observability = new ObservabilityBootstrap(config);
            observability.start();
            startMetadata();
            bindExecutionAuthority();
            startRoleSubsystems();
            LOG.info("event=coordinator_bootstrap_completed coordinatorId={} role={}",
                    config.coordinatorId(), config.role());
        } catch (RuntimeException | Error failure) {
            closeResources(failure);
            throw failure;
        }
    }

    private void startRoleSubsystems() {
        security = new SecurityBootstrap(config, metadataDataSource,
                source -> new org.pangrova.persistence.workflow.postgresql.security.PostgresTableReadPolicyAuthority(
                        source,
                        metadataBootstrap.catalogRegistry()::requireLocalDeployment,
                        new org.pangrova.metadata.security.MetadataTableControlPolicyProjections(),
                        org.pangrova.security.rowfilter.RowLevelSecurityFilter::renderPolicies,
                        org.pangrova.security.mask.ColumnMaskingProcessor::renderMasks));
        security.start();
        validateInternalAuthSecret();
        startModules();
        if (config.role().isMaintenance()) {
            startStreamingBootstrap();
            startBranchCleanup();
            moduleWiring.setStreamingBootstrap(streamingBootstrap);
            moduleWiring.setBranchTtlCleanupTask(branchTtlCleanupTask);
            startMaintenanceLeadership();
        }
        if (config.role().isQuery()) {
            startQueryAndHttp();
            LOG.info("event=shuffle_mode_configured shuffleMode={}", config.shuffleMode());
        }
    }

    private void startQueryAndHttp() {
        ha = new HighAvailabilityBootstrap(config, ensureDistributedLease(), coordinatorRegistry,
                metadataDataSource, executionAuthority, modelAuthority,
                metadataBootstrap == null ? null : metadataBootstrap.catalogRegistry(),
                observability.metricsProvider());
        query = buildQueryBootstrap();
        query.start(processStore == null ? null : processStore.identity());
        http = buildHttpBootstrap();
        int boundPort = http.bind();
        ha.start(query, boundPort);
        var invalidationPoller = moduleWiring.catalogMaintenance().getInvalidationPoller();
        if (invalidationPoller != null) {
            invalidationPoller.start();
        }
        query.startCoordinationLoops();
        startCdcWorkerRuntime();
        http.start();
    }

    private void startCdcWorkerRuntime() {
        if (streamingBootstrap == null) {
            return;
        }
        var workers = query.cdcWorkerDispatch(ha.haService());
        cdcRuntime = new JournaledCdcRuntime(streamingBootstrap, metadataBootstrap.catalogRegistry(),
                query.writeCommitJournal(),
                query.queryDispatcher().writeAuthorityResolver(),
                executionAuthority, workers, null, maintenanceLeadership, cdcStreamFeed());
        streamingBootstrap.attachJournaledRuntime(cdcRuntime);
        LOG.info("event=cdc_worker_runtime_started");
    }

    private void startLocalCdcRuntime() {
        var catalogs = metadataBootstrap.catalogRegistry();
        var journal = WriteCommitComponentFactory.createJournal(metadataDataSource);
        var quarantine = WriteCommitComponentFactory.createQuarantineStore(metadataDataSource);
        var recovery = WriteCommitComponentFactory.createRecoveryService(journal, catalogs, quarantine);
        var resolver = GeneratedValueMaterializationCoordinator.writeAuthoritySupplier(
                catalogs, metadataDataSource, null, modelAuthority,
                        new org.pangrova.persistence.security.postgresql.tenant.PostgresDeferredTenantAuthority(),
                        AuthorizationTransactionGate::lockPublication,
                        CoordinatorTableWriteAuthority.create());
        cdcRuntime = new JournaledCdcRuntime(streamingBootstrap, catalogs, journal, resolver,
                executionAuthority, null, new LocalCdcBatchExecutor(journal, executionAuthority, recovery, quarantine,
                        streamingBootstrap.converterRegistry()),
                maintenanceLeadership, cdcStreamFeed());
        streamingBootstrap.attachJournaledRuntime(cdcRuntime);
        LOG.info("event=cdc_local_journaled_runtime_started");
    }

    private JdbcRunningStreamFeed cdcStreamFeed() {
        var jdbc = config.metadataJdbcConfig();
        return new JdbcRunningStreamFeed(jdbc.jdbcUrl(), jdbc.username(), jdbc.password());
    }

    private QueryBootstrap buildQueryBootstrap() {
        return new QueryBootstrap(config,
                observability.tracingProvider(), observability.metricsProvider(),
                security.accessControl(),
                metadataBootstrap, moduleWiring, metadataDataSource, streamingBootstrap,
                ha.haService(),
                secretResolver, aiInfrastructureStores, modelAuthority);
    }

    private HttpBootstrap buildHttpBootstrap() {
        return new HttpBootstrap(config,
                observability.tracingProvider(), security.authenticationManager(),
                query.taskStatusReceiver(), query, query.workerManager(),
                moduleWiring.getCdcLifecycleManager(), this,
                metadataBootstrap != null ? metadataBootstrap.metadataService() : null);
    }

    org.pangrova.server.coordinator.http.endpoint.copy.CopySourceReadResource copySourceReadResource() {
        if (metadataDataSource == null || security.accessControl() == null || ha == null) {
            return null;
        }
        return new org.pangrova.server.coordinator.http.endpoint.copy.CopySourceReadResource(
                new org.pangrova.persistence.file.postgresql.copy.JdbcCopySourceReadAdmissions(
                        metadataDataSource, security.accessControl(),
                        new org.pangrova.persistence.query.postgresql.coordination.JdbcExecutionTransactionFence()),
                ha.haService());
    }

    /**
     * Files API tickets are issued only with durable metadata, an authorization provider, and
     * the shared secret that binds a descriptor to this cluster.
     */
    org.pangrova.server.coordinator.http.endpoint.file.FileSpaceFilesResource fileSpaceFilesResource() {
        if (metadataDataSource == null || security.accessControl() == null
                || config.internalAuthSecret() == null || config.internalAuthSecret().isBlank()) {
            return null;
        }
        var context = org.pangrova.file.access.FileSpaceAccessService.FileSpaceAccessContext.of(
                System.getProperty("pangrova.files.managed-storage-profile", ""),
                () -> getWorkerManager() == null ? java.util.List.of()
                        : getWorkerManager().getActiveWorkers().stream()
                                .map(worker -> "http://" + worker.host() + ":"
                                        + worker.httpPort())
                                .toList(),
                new org.pangrova.file.access.FileGrantAudit(query.auditEmitter()::emit),
                config.internalAuthSecret(),
                new org.pangrova.persistence.file.postgresql.space.JdbcFileSpaceAuthority(metadataDataSource,
                        new org.pangrova.persistence.security.postgresql.tenant.JdbcTenantStore(metadataDataSource)));
        var access = new org.pangrova.file.access.FileSpaceAccessService(
                new org.pangrova.persistence.file.postgresql.space.JdbcFileSpaceRegistry(metadataDataSource),
                new org.pangrova.persistence.file.postgresql.profile.JdbcStorageProfileRegistry(metadataDataSource),
                security.accessControl(), context);
        return new org.pangrova.server.coordinator.http.endpoint.file.FileSpaceFilesResource(
                access,
                new org.pangrova.file.access.FileUrlService(access,
                        org.pangrova.file.worker.WorkerFileClient.create(
                                config.internalAuthSecret())),
                new org.pangrova.server.coordinator.http.endpoint.file.StageFileUrlService(access));
    }

    /** Current HA epoch published through Worker registration and heartbeat acknowledgements. */
    long currentCoordinatorEpoch() {
        if (ha == null) {
            throw new IllegalStateException("Coordinator HA service is not initialized");
        }
        long epoch = ha.haService().getCurrentEpoch();
        if (epoch <= 0) {
            throw new IllegalStateException("Coordinator HA epoch is not initialized: " + epoch);
        }
        return epoch;
    }

    org.pangrova.common.client.query.QueryOwnerEndpoint queryOwnerEndpoint(int httpPort) {
        return new org.pangrova.common.client.query.QueryOwnerEndpoint(config.coordinatorId(),
                ha.haService().getLocalIncarnation(), config.coordinatorTransportAdvertisedHost(), httpPort);
    }

    /** Stops admission and releases every owned subsystem in dependency order. */
    public void shutdown() {
        lifecycle.close(() -> closeResources(null));
        shutdownCompleted.countDown();
    }

    /** Keeps the process entry point alive until owned resources finish shutdown. */
    public void awaitShutdown() throws InterruptedException {
        shutdownCompleted.await();
    }

    private void closeResources(Throwable primary) {
        try {
            if (processStore == null) closeOwnedResources();
            else processStore.closeAfterDrain(this::closeOwnedResources);
        } catch (RuntimeException | Error failure) {
            if (primary == null) throw failure;
            if (primary != failure) primary.addSuppressed(failure);
        }
    }

    private void closeOwnedResources() {
        CoordinatorShutdownSequence.closeOwnedResources(lifecycle, new CoordinatorShutdownSequence.ShutdownOwners(
                query, ha, maintenanceLeadership, branchTtlCleanupTask, cdcRuntime,
                http, moduleWiring,
                nativeDatasetControl, metadataBootstrap, connectorHandleFactory, executionAuthority,
                metadataDataSource, security, observability, coordinationProvider));
    }

    private void initSecretResolver() {
        secretResolver = SecretResolverRegistry.discover(CoordinatorBootstrap.class.getClassLoader());
        LOG.info("event=phase_started phase=secret_resolver");
    }

    private void validateInternalAuthSecret() {
        if (config.securityEnabled()
                && config.securityConfig() != null
                && !config.securityConfig().bypassesAuthentication()) {
            String secret = config.internalAuthSecret();
            if (secret == null || secret.isBlank()) {
                throw new IllegalStateException(
                        "internalAuthSecret is required when security is enabled. "
                                + "Configure 'security.internalAuthSecret' in coordinator config.");
            }
        }
    }

    private void startMetadata() {
        if (!config.metadataEnabled()) {
            LOG.info("event=phase_skipped phase=metadata reason=not_configured");
            return;
        }
        try {
            CoordinatorBootstrapConfig.MetadataJdbcConfig jdbc = config.metadataJdbcConfig();
            if (jdbc == null) {
                throw new IllegalStateException("metadataEnabled=true but metadataJdbcConfig is null");
            }
            metadataDataSource = PooledDataSourceFactory.create(jdbc.jdbcUrl(), jdbc.username(), jdbc.password());
            coordinationProvider = CoordinatorLeaseBootstrap.resolve(metadataDataSource);
            distributedLease = java.util.Objects.requireNonNull(coordinationProvider.lease(), "coordination lease");
            SchemaMigrationRunner.migrate(metadataDataSource);
            coordinatorRegistry = java.util.Objects.requireNonNull(coordinationProvider.registry(),
                    "coordinator registry");
            aiInfrastructureStores = AiInfrastructureStores.jdbc(
                    metadataDataSource, artifactStores.builds(metadataDataSource));
            CatalogDeploymentRepository deployments = new PostgresCatalogDeploymentRepository(metadataDataSource);
            var tables = new PostgresTableProjectionRepository(metadataDataSource);
            var projections = new PostgresMetadataProjectionTransactions(metadataDataSource);
            ClassLoader cl = getClass().getClassLoader();
            nativeDatasetControl = new NativeDatasetControlBootstrap(config.internalAuthSecret(),
                    () -> query, () -> ha == null ? null : ha.haService(), metadataDataSource, modelAuthority);
            connectorHandleFactory = new PluginConnectorHandleFactory(
                    cl, secretResolver, nativeDatasetControl::forCatalog, nativeDatasetControl::sourceForCatalog,
                    config.core().pluginDirectory());
            var cache = jdbc.cache().open(cl);
            try {
                var federationFactory = new PluginFederationHandleFactory(cl, secretResolver, cache);
                metadataBootstrap = MetadataBootstrap.initialize(
                        loadTableStores(cl, metadataDataSource), deployments, tables, projections,
                                new PostgresMetadataSyncRepository(metadataDataSource),
                        connectorHandleFactory, federationFactory,
                        new JdbcMaterializationRepository(metadataDataSource),
                        new JdbcMaterializationCatalog(metadataDataSource), cache);
            } catch (RuntimeException | Error failure) {
                try {
                    cache.close();
                } catch (RuntimeException cleanup) {
                    failure.addSuppressed(cleanup);
                }
                throw failure;
            }
            LOG.info("event=phase_started phase=metadata");
        } catch (RuntimeException e) {
            throw new BootstrapFailedException("Metadata", e);
        }
    }

    private void startModules() {
        moduleWiring = new ModuleIntegrationBootstrap();
        if (config.role().isMaintenance()) {
            org.pangrova.persistence.coordination.api.lease.DistributedLease lease = ensureDistributedLease();
            String processOwner = config.coordinatorId() + "/"
                    + java.util.UUID.randomUUID();
            maintenanceLeaderElection =
                    new org.pangrova.coordinator.indexing.lifecycle.MaintenanceLeaderElection(
                            lease, processOwner);
        }
        if (distributedLease != null) {
            moduleWiring.setDistributedLease(distributedLease);
        }
        if (metadataBootstrap != null) {
            moduleWiring.setCatalogRegistry(metadataBootstrap.catalogRegistry());
            moduleWiring.catalogMaintenance().setMetadataSyncDeps(
                    metadataBootstrap.syncService(), metadataBootstrap.catalogRegistry());
        }
        if (config.governanceEnabled()) {
            var metadata = java.util.Objects.requireNonNull(metadataBootstrap, "metadataBootstrap");
            moduleWiring.startGovernance(config, metadataDataSource, metadata.columnTagEvents());
            metadata.replayColumnTags();
        }
        moduleWiring.startResilience(config);
        moduleWiring.startTenant(config, metadataDataSource);
        moduleWiring.startOperations(config);
        startRoleIndexing();
        if (metadataBootstrap != null) {
            moduleWiring.catalogMaintenance().setVersionObserver(metadataBootstrap.versionObserver());
        }
        // Wire invalidation poller for leader-gated TTL cleanup (Task 1.9)
        if (metadataDataSource != null && metadataBootstrap != null) {
            var invalidationPoller = CoordinatorInvalidationProgress.createPoller(metadataDataSource,
                    metadataBootstrap.metadataCache(), coordinatorRegistry, config.role(), config.coordinatorId());
            moduleWiring.catalogMaintenance().setInvalidationPoller(invalidationPoller);
            invalidationPoller.validateSchema();
            if (!config.role().isQuery()) {
                invalidationPoller.start();
            }
        }
    }

    private void startRoleIndexing() {
        if (config.role().isMaintenance()) {
            moduleWiring.startStreaming(config, metadataDataSource,
                    metadataBootstrap != null ? metadataBootstrap.catalogRegistry() : null);
            startMaintenanceIndexing();
        } else if (config.role().isQuery() && config.indexingEnabled() && metadataBootstrap != null) {
            startIndexQuery();
        }
    }

    private void startMaintenanceIndexing() {
        if (!config.indexingEnabled() || metadataBootstrap == null) return;
        startIndexing();
        var indexing = moduleWiring.getIndexingBootstrap();
        if (indexing == null) {
            throw new IllegalStateException("configured durable indexing/publication failed to initialize");
        }
        indexing.wireExecutionAuthority(executionAuthority);
    }

    private void bindExecutionAuthority() {
        if (metadataDataSource == null) {
            if (config.role().isMaintenance() && config.indexingEnabled()) {
                throw new IllegalStateException(
                        "maintenance artifact publication requires its durable execution backend");
            }
            return;
        }
        String incarnation = java.util.UUID.randomUUID().toString();
        if (config.role().isQuery() && admissionStateDirectory != null) {
            processStore = ProcessIncarnationStore.open(admissionStateDirectory);
            processStore.claimAuthority("coordinator-statement");
            JdbcCoordinatorWorkRecovery.publishRetired(metadataDataSource, processStore);
            incarnation = processStore.identity().incarnationId();
        }
        executionAuthority = new org.pangrova.coordinator.coordination.CoordinatorExecutionAuthority(
                new org.pangrova.persistence.query.postgresql.coordination.JdbcExecutionAnchorStore(metadataDataSource),
                config.coordinatorId(), incarnation, EXECUTION_OWNER_LEASE_TTL);
    }

    private void startIndexing() {
        try {
            org.pangrova.persistence.indexing.api.metadata.IndexMetadataStore indexMetadataStore =
                    new org.pangrova.persistence.indexing.postgresql.metadata.JdbcIndexMetadataStore(metadataDataSource,
                            CoordinatorTableWriteAuthority.create()::requireCurrent);
            org.pangrova.metadata.catalog.registry.CatalogRegistry catalogRegistry =
                    metadataBootstrap.catalogRegistry();
            org.pangrova.observability.api.metrics.MetricsProvider metricsProvider =
                    observability != null ? observability.metricsProvider() : null;
            org.pangrova.security.api.authorization.AccessControl accessControl =
                    security != null ? security.accessControl() : null;
            moduleWiring.startIndexing(
                    indexMetadataStore,
                    new JdbcIndexBuildClaimRepository(metadataDataSource),
                    catalogRegistry, metricsProvider, accessControl,
                    config.baseIndexPath(), distributedLease,
                    artifactStores.builds(
                            metadataDataSource),
                    IndexingBootstrap.publicationRouter(artifactStores.indexPublications(metadataDataSource),
                            artifactStores.generatedPublications(metadataDataSource),
                            artifactStores.tabularPublications(metadataDataSource)),
                    maintenanceLeaderElection, config.indexingConfig(), config.artifactPublicationSettings());
            LOG.info("event=phase_started phase=indexing");
        } catch (Exception e) {
            LOG.warn("event=phase_failed phase=indexing error={}", e.getMessage(), e);
        }
    }

    private void startIndexQuery() {
        try {
            org.pangrova.persistence.indexing.api.metadata.IndexMetadataStore indexMetadataStore =
                    new org.pangrova.persistence.indexing.postgresql.metadata.JdbcIndexMetadataStore(metadataDataSource,
                            CoordinatorTableWriteAuthority.create()::requireCurrent);
            org.pangrova.observability.api.metrics.MetricsProvider metricsProvider =
                    observability != null ? observability.metricsProvider() : null;
            moduleWiring.startIndexQuery(indexMetadataStore, metricsProvider,
                    artifactStores.builds(metadataDataSource));
            LOG.info("event=phase_started phase=index_query");
        } catch (Exception e) {
            LOG.warn("event=phase_failed phase=index_query error={}", e.getMessage(), e);
        }
    }

    private void startStreamingBootstrap() {
        if (!config.streamingEnabled() || metadataDataSource == null) {
            LOG.info("event=phase_skipped phase=streaming_bootstrap reason={}",
                    !config.streamingEnabled() ? "not_enabled" : "no_datasource");
            return;
        }
        try {
            streamingBootstrap = new StreamingBootstrap(metadataDataSource);
            LOG.info("event=phase_started phase=streaming_bootstrap");
        } catch (Exception e) {
            LOG.warn("event=phase_failed phase=streaming_bootstrap error={}", e.getMessage(), e);
        }
    }

    private void startMaintenanceLeadership() {
        if (maintenanceLeaderElection == null) {
            throw new IllegalStateException(
                    "maintenance role requires an independent maintenance lease");
        }
        maintenanceLeadership = new MaintenanceLeadershipBootstrap(
                maintenanceLeaderElection, moduleWiring);
        if (!config.role().isQuery() && streamingBootstrap != null) {
            startLocalCdcRuntime();
        }
        maintenanceLeadership.start();
    }

    org.pangrova.persistence.coordination.api.lease.DistributedLease ensureDistributedLease() {
        if (distributedLease != null) {
            return distributedLease;
        }
        if (System.getProperty("pangrova.coordination.lease.backend") != null) {
            throw new IllegalStateException("The explicitly configured coordination lease backend requires "
                    + "metadataEnabled=true and a metadata JDBC data source");
        }
        if (!config.transport().isLoopbackAdvertisedHost()) {
            throw new IllegalStateException(
                    "A distributed lease backend is required for a non-loopback coordinator "
                            + "(advertisedHost="
                            + config.coordinatorTransportAdvertisedHost() + ")");
        }
        distributedLease = new org.pangrova.persistence.coordination.memory.lease.InMemoryDistributedLease();
        LOG.warn("event=distributed_lease_fallback type=InMemory reason=single_node_loopback");
        return distributedLease;
    }

    private void startBranchCleanup() {
        if (metadataBootstrap == null) {
            LOG.info("event=phase_skipped phase=branch_ttl_cleanup reason=no_metadata");
            return;
        }
        try {
            org.pangrova.metadata.catalog.registry.CatalogRegistry catalogRegistry =
                    metadataBootstrap.catalogRegistry();
            branchTtlCleanupTask = new BranchTtlCleanupTask(catalogRegistry);
            LOG.info("event=phase_initialized phase=branch_ttl_cleanup state=leader_gated");
        } catch (Exception e) {
            LOG.warn("event=phase_failed phase=branch_ttl_cleanup error={}", e.getMessage(), e);
        }
    }

    // -- Accessors for testing and health checks --
    public boolean isStarted() { return lifecycle.isRunning(); }
    public int getHttpPort() { return http != null && http.httpServer() != null ? http.httpServer().getPort() : -1; }
    public QueryManager getQueryManager() { return query != null ? query.queryManager() : null; }
    public WorkerManager getWorkerManager() { return query != null ? query.workerManager() : null; }
    public TracingProvider getTracingProvider() {
        return observability != null ? observability.tracingProvider() : null;
    }
    public MetadataBootstrap getMetadataBootstrap() { return metadataBootstrap; }
    public AiInfrastructureStores getAiInfrastructureStores() { return aiInfrastructureStores; }
    DataSource metadataDataSource() { return metadataDataSource; }
    TransportServerLifecycle getCoordinatorTransportServer() {
        return query != null ? query.coordinatorTransportServer() : null;
    }
    public CircuitBreakerPolicy getCircuitBreaker() { return query != null ? query.circuitBreaker() : null; }
    public org.pangrova.operations.recovery.RecoveryPlaybook getRecoveryPlaybook() {
        return moduleWiring != null ? moduleWiring.getRecoveryPlaybook() : null;
    }
    static TableStores loadTableStores(ClassLoader loader, DataSource source) {
        var factories = new java.util.HashMap<String, TableStoreFactory>();
        for (var factory : java.util.ServiceLoader.load(TableStoreFactory.class, loader)) {
            String id = factory.identifier();
            if (factory.apiVersion() != TableStoreFactory.API_VERSION) {
                throw new IllegalStateException("Incompatible table storage provider: " + id);
            }
            if (id == null || id.isBlank() || factories.putIfAbsent(id, factory) != null) {
                throw new IllegalStateException("Blank or duplicate table storage provider: " + id);
            }
        }
        var factory = factories.get("postgresql");
        if (factory == null) throw new IllegalStateException("Missing PostgreSQL table storage provider");
        return factory.create(java.util.Optional.of(source));
    }

}
