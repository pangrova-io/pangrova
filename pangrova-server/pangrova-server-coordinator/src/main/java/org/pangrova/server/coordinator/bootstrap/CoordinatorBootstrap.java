package org.pangrova.server.coordinator.bootstrap;

import org.pangrova.persistence.table.postgresql.control.JdbcTableControlStore;
import org.pangrova.persistence.table.postgresql.governancebinding.JdbcTableControlGovernanceBindingStore;
import org.pangrova.persistence.table.postgresql.statistics.JdbcTableControlStatisticsStore;
import org.pangrova.metadata.assembly.generation.GenerationStores;
import org.pangrova.ai.control.store.jdbc.embedding.JdbcCurrentModelAuthority;
import org.pangrova.persistence.jdbc.api.security.model.CurrentModelAuthority;
import org.pangrova.persistence.security.postgresql.policy.JdbcCurrentDirectRoleReader;
import org.pangrova.coordinator.ai.branch.BranchTtlCleanupTask;
import org.pangrova.coordinator.metadata.MetadataBootstrap;
import org.pangrova.common.lifecycle.BootstrapFailedException;
import org.pangrova.coordinator.metadata.PluginConnectorHandleFactory;
import org.pangrova.coordinator.metadata.PluginFederationHandleFactory;
import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.ai.control.assembly.AiInfrastructureStores;
import org.pangrova.persistence.catalog.postgresql.catalog.PostgresCatalogDeploymentRepository;
import org.pangrova.persistence.catalog.api.catalog.CatalogDeploymentRepository;
import org.pangrova.metadata.store.jdbc.catalog.PooledDataSourceFactory;
import org.pangrova.metadata.store.jdbc.catalog.SchemaMigrationRunner;
import org.pangrova.security.api.secret.SecretResolver;
import org.pangrova.security.core.secret.SecretResolverRegistry;
import org.pangrova.security.secret.env.EnvSecretProvider;
import org.pangrova.security.secret.file.FileSecretProvider;
import org.pangrova.observability.spi.tracing.TracingProvider;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.protocol.transport.TransportServerLifecycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.sql.DataSource;
import java.util.concurrent.CountDownLatch;
import static org.pangrova.common.util.Preconditions.checkNotNull;
import org.pangrova.persistence.catalog.postgresql.projection.PostgresTableProjectionRepository;
import org.pangrova.persistence.catalog.postgresql.projection.PostgresMetadataProjectionTransactions;
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
    private final CurrentModelAuthority modelAuthority =
            new JdbcCurrentModelAuthority(new JdbcCurrentDirectRoleReader());
    private final GenerationStores artifactStores = new GenerationStores(modelAuthority);
    private final org.pangrova.server.coordinator.bootstrap.lifecycle.CoordinatorLifecycleAdmission lifecycle =
            new org.pangrova.server.coordinator.bootstrap.lifecycle.CoordinatorLifecycleAdmission();
    private final CountDownLatch shutdownCompleted = new CountDownLatch(1);

    private SecretResolver secretResolver;
    private ObservabilityBootstrap observability;
    private MetadataBootstrap metadataBootstrap;
    private PluginConnectorHandleFactory connectorHandleFactory;
    private NativeDatasetControlBootstrap nativeDatasetControl;
    private DataSource metadataDataSource;
    private AiInfrastructureStores aiInfrastructureStores;
    private org.pangrova.persistence.coordination.api.lease.DistributedLease distributedLease;
    private org.pangrova.persistence.coordination.spi.provider.CoordinationProvider zkLeaseProvider;
    private SecurityBootstrap security;
    private ModuleIntegrationBootstrap moduleWiring;
    private StreamingBootstrap streamingBootstrap;
    private QueryBootstrap query;
    private HttpBootstrap http;
    private HighAvailabilityBootstrap ha;
    private org.pangrova.coordinator.indexing.lifecycle.MaintenanceLeaderElection maintenanceLeaderElection;
    private MaintenanceLeadershipBootstrap maintenanceLeadership;
    private BranchTtlCleanupTask branchTtlCleanupTask;
    private org.pangrova.coordinator.coordination.CoordinatorExecutionAuthority executionAuthority;

    public CoordinatorBootstrap(CoordinatorBootstrapConfig config) {
        this.config = checkNotNull(config, "config must not be null");
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
        security = new SecurityBootstrap(config, metadataDataSource, source ->
                org.pangrova.metadata.assembly.security.ColumnMaskStoreWiring.tableControl(
                        new org.pangrova.persistence.security.postgresql.mask.JdbcColumnMaskStore(source),
                        metadataBootstrap.catalogRegistry()),
                source -> new org.pangrova.metadata.service.TableControlRowPolicyStore(
                        new org.pangrova.metadata.service.RegistryTablePolicyAuthority(
                                metadataBootstrap.catalogRegistry()),
                        new org.pangrova.persistence.security.postgresql.rowfilter.JdbcRowPolicyStore(source)));
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
        query = buildQueryBootstrap();
        query.start();
        ha = buildHighAvailabilityBootstrap();
        ha.setClaimedExecutionRecovery(claimed ->
                query.recoverWriteCommitIntents(claimed, ha.haService()));
        ha.start();
        query.wireHighAvailability(ha.haService());
        wireQueryOwnershipTracking();
        http = buildHttpBootstrap();
        http.start();
    }

    private QueryBootstrap buildQueryBootstrap() {
        return new QueryBootstrap(config,
                observability.tracingProvider(), observability.metricsProvider(),
                security.accessControl(),
                metadataBootstrap, moduleWiring, metadataDataSource, streamingBootstrap,
                ha != null ? ha.haService() : null,
                secretResolver, aiInfrastructureStores, modelAuthority);
    }

    private HttpBootstrap buildHttpBootstrap() {
        return new HttpBootstrap(config,
                observability.tracingProvider(), security.authenticationManager(),
                query.taskStatusReceiver(), query, query.workerManager(),
                moduleWiring.getCdcLifecycleManager(), this,
                query.splitAssignmentManager(),
                metadataBootstrap != null ? metadataBootstrap.metadataService() : null);
    }

    org.pangrova.coordinator.http.endpoint.copy.CopySourceReadResource copySourceReadResource() {
        if (metadataDataSource == null || security.accessControl() == null || ha == null) {
            return null;
        }
        return new org.pangrova.coordinator.http.endpoint.copy.CopySourceReadResource(
                new org.pangrova.persistence.file.postgresql.copy.JdbcCopySourceReadAdmissions(
                        metadataDataSource, security.accessControl(),
                        new org.pangrova.persistence.query.postgresql.coordination.JdbcExecutionTransactionFence()),
                ha.haService());
    }

    /**
     * Files API tickets are issued only with durable metadata, an authorization provider, and
     * the shared secret that binds a descriptor to this cluster.
     */
    org.pangrova.coordinator.http.endpoint.file.FileSpaceFilesResource fileSpaceFilesResource() {
        if (metadataDataSource == null || security.accessControl() == null
                || config.internalAuthSecret() == null || config.internalAuthSecret().isBlank()) {
            return null;
        }
        var context = org.pangrova.file.core.access.FileSpaceAccessService.FileSpaceAccessContext.of(
                System.getProperty("pangrova.files.managed-storage-profile", ""),
                () -> getWorkerManager() == null ? java.util.List.of()
                        : getWorkerManager().getActiveWorkers().stream()
                                .map(worker -> "http://" + worker.host() + ":"
                                        + worker.httpPort())
                                .toList(),
                new org.pangrova.coordinator.query.audit.FileSpaceAuditEmitter(
                        query.auditEmitter()),
                config.internalAuthSecret(),
                new org.pangrova.persistence.file.postgresql.space.JdbcFileSpaceAuthority(metadataDataSource,
                        new org.pangrova.persistence.security.postgresql.tenant.JdbcTenantStore(metadataDataSource)));
        var access = new org.pangrova.file.core.access.FileSpaceAccessService(
                new org.pangrova.persistence.file.postgresql.space.JdbcFileSpaceRegistry(metadataDataSource),
                new org.pangrova.persistence.file.postgresql.profile.JdbcStorageProfileRegistry(metadataDataSource),
                security.accessControl(), context);
        return new org.pangrova.coordinator.http.endpoint.file.FileSpaceFilesResource(
                access,
                new org.pangrova.file.core.access.FileUrlService(access,
                        org.pangrova.file.adapter.worker.WorkerFileClient.create(
                                config.internalAuthSecret())),
                new org.pangrova.coordinator.file.StageFileUrlService(access));
    }

    private HighAvailabilityBootstrap buildHighAvailabilityBootstrap() {
        org.pangrova.persistence.coordination.api.lease.DistributedLease lease = ensureDistributedLease();
        var haBootstrap = new HighAvailabilityBootstrap(config, query.queryManager(), query.workerManager(),
                query.taskTracker(), query.schedulerExecutor(), lease, modelAuthority);
        if (metadataDataSource != null) {
            haBootstrap.setDataSource(metadataDataSource);
            haBootstrap.setExecutionAuthority(executionAuthority);
        }
        if (zkLeaseProvider != null) {
            // A-HA-01: run coordinator membership/discovery on the same ZooKeeper quorum as the lease.
            haBootstrap.setQuorumCoordinatorRegistry(zkLeaseProvider.registry());
        }
        // Task 38 / G-META-02: Wire catalog registry for peer divergence
        // detection/repair at leader election time.
        if (metadataBootstrap != null) {
            haBootstrap.setCatalogRegistry(metadataBootstrap.catalogRegistry());
        }
        return haBootstrap;
    }

    /** Current HA epoch published through Worker registration and heartbeat acknowledgements. */
    long currentCoordinatorEpoch() {
        if (ha == null || ha.haService() == null) {
            throw new IllegalStateException("Coordinator HA service is not initialized");
        }
        long epoch = ha.haService().getCurrentEpoch();
        if (epoch <= 0) {
            throw new IllegalStateException("Coordinator HA epoch is not initialized: " + epoch);
        }
        return epoch;
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
            closeOwnedResources();
        } catch (RuntimeException | Error failure) {
            if (primary == null) throw failure;
            if (primary != failure) primary.addSuppressed(failure);
        }
    }

    private void closeOwnedResources() {
        CoordinatorShutdownSequence.closeOwnedResources(lifecycle, new CoordinatorShutdownSequence.ShutdownOwners(
                query, ha, maintenanceLeadership, branchTtlCleanupTask, streamingBootstrap, http, moduleWiring,
                nativeDatasetControl, metadataBootstrap, connectorHandleFactory, executionAuthority,
                metadataDataSource, security, observability, zkLeaseProvider));
    }

    private void initSecretResolver() {
        SecretResolverRegistry registry = new SecretResolverRegistry();
        registry.register(new EnvSecretProvider());
        registry.register(new FileSecretProvider());
        secretResolver = registry;
        LOG.info("event=phase_started phase=secret_resolver providers=ENV,FILE");
    }

    private void validateInternalAuthSecret() {
        if (config.securityEnabled()
                && config.securityConfig() != null
                && config.securityConfig().authenticatorType()
                        != CoordinatorBootstrapConfig.SecurityConfig.AuthenticatorType.NOOP) {
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
            distributedLease = resolveDistributedLease(metadataDataSource);
            SchemaMigrationRunner.migrate(metadataDataSource);
            aiInfrastructureStores = AiInfrastructureStores.jdbc(
                    metadataDataSource, artifactStores.builds(metadataDataSource));
            CatalogDeploymentRepository deployments = new PostgresCatalogDeploymentRepository(metadataDataSource);
            var tables = new PostgresTableProjectionRepository(metadataDataSource);
            var projections = new PostgresMetadataProjectionTransactions(metadataDataSource);
            ClassLoader cl = getClass().getClassLoader();
            nativeDatasetControl = new NativeDatasetControlBootstrap(config.internalAuthSecret(),
                    () -> query, () -> ha == null ? null : ha.haService(), metadataDataSource, modelAuthority);
            connectorHandleFactory = new PluginConnectorHandleFactory(
                    cl, secretResolver, nativeDatasetControl::forCatalog, nativeDatasetControl::sourceForCatalog);
            var federationFactory = new PluginFederationHandleFactory(cl, secretResolver);
            metadataBootstrap = MetadataBootstrap.initialize(
                    deployments, tables, projections, new PostgresMetadataSyncRepository(metadataDataSource),
                    connectorHandleFactory, federationFactory, metadataDataSource);
            // Control records (ownership/identity/version) are durable in the metadata
            // schema; the in-memory default only serves tests and metadata-less modes.
            metadataBootstrap.catalogRegistry().setTableControlService(
                    new org.pangrova.table.core.ownership.TableControlService(
                            new JdbcTableControlStore(metadataDataSource),
                            metadataBootstrap.catalogRegistry()::requireCurrentCatalogIdentity,
                            org.pangrova.table.core.config.TableControlSettings.fromSystemProperties()));
            metadataBootstrap.catalogRegistry().setTableStatisticsService(
                    new org.pangrova.table.core.statistics.TableControlStatisticsService(
                            metadataBootstrap.catalogRegistry().tableControl(),
                            new JdbcTableControlStatisticsStore(metadataDataSource)));
            metadataBootstrap.catalogRegistry().setGovernanceBindingStore(
                    new JdbcTableControlGovernanceBindingStore(metadataDataSource));
            LOG.info("event=phase_started phase=metadata");
        } catch (Exception e) {
            throw new BootstrapFailedException("Metadata", e);
        }
    }

    /**
     * Selects the distributed-lease backend (A-HA-01). Defaults to JDBC (single coordination DB) for
     * backward compatibility. Set {@code -Dpangrova.coordination.lease.backend=zookeeper} plus
     * {@code -Dpangrova.coordination.lease.zookeeper.connectString=host:port,...} to run leader election
     * on a ZooKeeper quorum. Durable execution still requires the relational metadata backend.
     * The ZooKeeper client is owned by this bootstrap and released in {@link #shutdown()}.
     */
    private org.pangrova.persistence.coordination.api.lease.DistributedLease resolveDistributedLease(
            DataSource source) {
        var binding = CoordinatorLeaseBootstrap.resolve(source);
        zkLeaseProvider = binding.quorumProvider();
        return binding.lease();
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
            moduleWiring.setMetadataSyncDeps(metadataBootstrap.syncService(), metadataBootstrap.catalogRegistry());
        }
        moduleWiring.startGovernance(config, metadataDataSource);
        // Share the governance tag event source with metadata DDL so SET/UNSET
        // COLUMN TAG activates masking, and replay durable tags after a restart.
        if (metadataBootstrap != null && metadataBootstrap.metadataService()
                instanceof org.pangrova.metadata.service.DefaultMetadataService taggedService) {
            org.pangrova.governance.core.classification.ColumnTagService tagService =
                    moduleWiring.getColumnTagService();
            if (tagService != null) {
                taggedService.setColumnTagService(tagService);
                taggedService.replayColumnTags();
            }
        }
        moduleWiring.startResilience(config);
        moduleWiring.startTenant(config, metadataDataSource);
        moduleWiring.startOperations(config);
        startRoleIndexing();
        // Wire metadata sync deps for leader-gated periodic sync (Pillar 5)
        if (metadataBootstrap != null) {
            moduleWiring.setMetadataSyncDeps(
                    metadataBootstrap.syncService(),
                    metadataBootstrap.catalogRegistry());
            moduleWiring.setVersionObserver(metadataBootstrap.versionObserver());
        }
        // Wire invalidation poller for leader-gated TTL cleanup (Task 1.9)
        if (metadataDataSource != null && metadataBootstrap != null) {
            var invalidationPoller =
                    new org.pangrova.persistence.catalog.postgresql.invalidation.PostgresInvalidationPoller(
                    metadataDataSource,
                    metadataBootstrap.metadataCache());
            moduleWiring.setInvalidationPoller(invalidationPoller);
            invalidationPoller.initSchema();
            invalidationPoller.start();
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
        executionAuthority = new org.pangrova.coordinator.coordination.CoordinatorExecutionAuthority(
                new org.pangrova.persistence.query.postgresql.coordination.JdbcExecutionAnchorStore(metadataDataSource),
                config.coordinatorId(), java.util.UUID.randomUUID().toString(), EXECUTION_OWNER_LEASE_TTL);
    }

    private void startIndexing() {
        try {
            org.pangrova.persistence.indexing.api.metadata.IndexMetadataStore indexMetadataStore =
                    new org.pangrova.persistence.indexing.postgresql.metadata.JdbcIndexMetadataStore(metadataDataSource,
                            org.pangrova.metadata.service.TableControlDeferredPolicy::requireCurrent);
            org.pangrova.metadata.catalog.registry.CatalogRegistry catalogRegistry =
                    metadataBootstrap.catalogRegistry();
            org.pangrova.observability.spi.metrics.MetricsProvider metricsProvider =
                    observability != null ? observability.metricsProvider() : null;
            org.pangrova.security.spi.authorization.AccessControl accessControl =
                    security != null ? security.accessControl() : null;
            moduleWiring.startIndexing(
                    indexMetadataStore,
                    new org.pangrova.persistence.indexing.postgresql.build.JdbcIndexBuildClaimRepository(metadataDataSource),
                    catalogRegistry, metricsProvider, accessControl,
                    config.baseIndexPath(), distributedLease,
                    artifactStores.builds(
                            metadataDataSource),
                    org.pangrova.generation.core.submission.GenerationPublicationFactory.route(java.util.Map.of(
                            org.pangrova.generation.api.output.GenerationOutputTypeIds.VECTOR_INDEX,
                            artifactStores.indexPublications(
                                    metadataDataSource),
                            org.pangrova.generation.api.output.GenerationOutputTypeIds.TEXT_INDEX,
                            artifactStores.indexPublications(
                                    metadataDataSource),
                            org.pangrova.generation.api.output.GenerationOutputTypeIds.GENERATED_COLUMN_VALUES,
                            artifactStores.generatedPublications(
                                    metadataDataSource),
                            org.pangrova.generation.api.output.GenerationOutputTypeIds.TABULAR_EXPORT,
                            artifactStores.tabularPublications(
                                    metadataDataSource))),
                    maintenanceLeaderElection);
            LOG.info("event=phase_started phase=indexing");
        } catch (Exception e) {
            LOG.warn("event=phase_failed phase=indexing error={}", e.getMessage(), e);
        }
    }

    private void startIndexQuery() {
        try {
            org.pangrova.persistence.indexing.api.metadata.IndexMetadataStore indexMetadataStore =
                    new org.pangrova.persistence.indexing.postgresql.metadata.JdbcIndexMetadataStore(metadataDataSource,
                            org.pangrova.metadata.service.TableControlDeferredPolicy::requireCurrent);
            org.pangrova.observability.spi.metrics.MetricsProvider metricsProvider =
                    observability != null ? observability.metricsProvider() : null;
            moduleWiring.startIndexQuery(indexMetadataStore, metricsProvider);
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
            org.pangrova.streaming.pipeline.StreamingContext streamingContext = moduleWiring.getStreamingContext();
            if (streamingContext == null) {
                LOG.info("event=phase_skipped phase=streaming_bootstrap reason=streaming_wiring_unavailable");
                return;
            }
            streamingBootstrap = StreamingBootstrap.build(
                    metadataDataSource,
                    streamingContext.watermarkTracker(),
                    streamingContext.scheduler(),
                    metadataBootstrap.catalogRegistry(), config.coordinatorId());
            LOG.info("event=phase_started phase=streaming_bootstrap");
        } catch (Exception e) {
            LOG.warn("event=phase_failed phase=streaming_bootstrap error={}", e.getMessage(), e);
        }
    }

    /**
     * Wires the HA service into QueryDispatcher for query ownership
     * acquire/release. Must be called after both QueryBootstrap and
     * HighAvailabilityBootstrap have started, since QueryDispatcher is
     * constructed before the HA service is available.
     */
    private void wireQueryOwnershipTracking() {
        if (ha != null && ha.haService() != null && query != null && query.queryDispatcher() != null) {
            query.queryDispatcher().setOwnershipTracking(ha.haService(), config.coordinatorId());
        }
        wireInvalidationPollerRegistry();
    }

    private void wireInvalidationPollerRegistry() {
        if (moduleWiring == null || ha == null || ha.coordinatorRegistry() == null) {
            return;
        }
        var poller = moduleWiring.getInvalidationPoller();
        if (poller != null) {
            poller.setInvalidationProgressRegistry(
                    new org.pangrova.coordinator.coordination.InvalidationProgressRegistryAdapter(
                            ha.coordinatorRegistry()),
                    config.coordinatorId());
            LOG.debug("event=invalidation_poller_registry_wired coordinatorId={}", config.coordinatorId());
        }
    }

    private void startMaintenanceLeadership() {
        if (maintenanceLeaderElection == null) {
            throw new IllegalStateException(
                    "maintenance role requires an independent maintenance lease");
        }
        maintenanceLeadership = new MaintenanceLeadershipBootstrap(
                maintenanceLeaderElection, moduleWiring);
        maintenanceLeadership.start();
    }

    org.pangrova.persistence.coordination.api.lease.DistributedLease ensureDistributedLease() {
        if (distributedLease != null) {
            return distributedLease;
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
    public org.pangrova.operations.recovery.RecoveryPlaybookFramework getRecoveryPlaybook() {
        return moduleWiring != null ? moduleWiring.getRecoveryPlaybook() : null;
    }
}
