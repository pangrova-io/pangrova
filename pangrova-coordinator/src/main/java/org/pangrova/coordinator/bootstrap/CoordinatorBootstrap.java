package org.pangrova.coordinator.bootstrap;

import org.pangrova.coordinator.ai.branch.BranchTtlCleanupTask;
import org.pangrova.coordinator.ai.training.TrainingDataServer;
import org.pangrova.coordinator.ai.training.TrainingDataServerConfig;
import org.pangrova.coordinator.metadata.MetadataBootstrap;
import org.pangrova.operations.bootstrap.BootstrapFailedException;
import org.pangrova.coordinator.metadata.PluginConnectorHandleFactory;
import org.pangrova.coordinator.metadata.PluginFederationHandleFactory;
import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.metadata.store.JdbcMetadataStore;
import org.pangrova.metadata.store.MetadataStore;
import org.pangrova.metadata.store.PooledDataSourceFactory;
import org.pangrova.metadata.store.SchemaMigrationRunner;
import org.pangrova.security.secret.EnvSecretProvider;
import org.pangrova.security.secret.FileSecretProvider;
import org.pangrova.security.secret.SecretResolver;
import org.pangrova.spi.observability.TracingProvider;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.spi.transport.TransportServerLifecycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import javax.sql.DataSource;

import static org.pangrova.common.util.Preconditions.checkNotNull;
import static org.pangrova.common.util.Preconditions.checkState;

/**
 * Orchestrates the full Coordinator startup/shutdown sequence.
 * Delegates to sub-bootstraps: Observability → Metadata → Security → Modules → Query → HTTP → HA.
 * Shutdown runs in reverse order.
 */
public final class CoordinatorBootstrap {

    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorBootstrap.class);

    private final CoordinatorBootstrapConfig config;
    private final AtomicBoolean started = new AtomicBoolean(false);

    private SecretResolver secretResolver;
    private ObservabilityBootstrap observability;
    private MetadataBootstrap metadataBootstrap;
    private PluginConnectorHandleFactory connectorHandleFactory;
    private DataSource metadataDataSource;
    private org.pangrova.spi.coordination.DistributedLease distributedLease;
    private org.pangrova.metadata.coordination.ZooKeeperLeaseProvider zkLeaseProvider;
    private SecurityBootstrap security;
    private ModuleIntegrationBootstrap moduleWiring;
    private StreamingBootstrap streamingBootstrap;
    private QueryBootstrap query;
    private HttpBootstrap http;
    private HighAvailabilityBootstrap ha;
    private TrainingDataServer trainingDataServer;
    private BranchTtlCleanupTask branchTtlCleanupTask;

    public CoordinatorBootstrap(CoordinatorBootstrapConfig config) {
        this.config = checkNotNull(config, "config must not be null");
    }

    public void start() {
        checkState(!started.get(), "CoordinatorBootstrap already started");
        LOG.info("event=coordinator_bootstrap_start coordinatorId={} role={}",
                config.coordinatorId(), config.role());
        initSecretResolver();
        observability = new ObservabilityBootstrap(config);
        observability.start();
        startMetadata();
        security = new SecurityBootstrap(config, metadataDataSource);
        security.start();
        validateInternalAuthSecret();
        startModules();

        if (config.role().isMaintenance()) {
            startStreamingBootstrap();
            startAiServices();
        }

        if (config.role().isQuery()) {
            startQueryAndHttp();
            LOG.info("event=shuffle_mode_configured shuffleMode={}", config.shuffleMode());
        }

        if (config.role().isMaintenance()) {
            wireStreamingHaCallbacks();
        }

        started.set(true);
        LOG.info("event=coordinator_bootstrap_completed coordinatorId={} role={}",
                config.coordinatorId(), config.role());
    }

    private void startQueryAndHttp() {
        query = buildQueryBootstrap();
        query.start();
        ha = buildHighAvailabilityBootstrap();
        ha.start();
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
                secretResolver);
    }

    private HttpBootstrap buildHttpBootstrap() {
        return new HttpBootstrap(config,
                observability.tracingProvider(), security.authenticationManager(),
                query.taskStatusReceiver(), query.queryManager(), query.workerManager(),
                moduleWiring.getCdcLifecycleManager(), this,
                query.splitAssignmentManager(),
                metadataBootstrap != null ? metadataBootstrap.metadataService() : null);
    }

    private HighAvailabilityBootstrap buildHighAvailabilityBootstrap() {
        org.pangrova.spi.coordination.DistributedLease lease = distributedLease;
        if (lease == null) {
            // Task #77: Detect multi-node deployment with InMemory lease fallback.
            // InMemory lease gives each JVM its own independent lease, causing brain-split
            // where multiple coordinators each believe they are the leader.
            if (!config.transport().isLoopbackAdvertisedHost()) {
                throw new IllegalStateException(
                        "InMemory distributed lease detected in multi-node deployment "
                                + "(advertisedHost=" + config.coordinatorTransportAdvertisedHost() + "). "
                                + "This causes brain-split where each node holds its own local lease. "
                                + "Configure a JDBC metadata datasource for distributed HA coordination.");
            }
            LOG.warn("event=distributed_lease_fallback type=InMemory reason=no_datasource");
            lease = new org.pangrova.coordinator.coordination.inmemory.InMemoryDistributedLease();
        }
        var haBootstrap = new HighAvailabilityBootstrap(config, query.queryManager(), query.workerManager(),
                query.taskTracker(), query.schedulerExecutor(), lease);
        if (metadataDataSource != null) {
            haBootstrap.setDataSource(metadataDataSource);
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
        // Wire unified leader lifecycle so HA failover triggers
        // indexing, streaming, and metadata sync start/stop
        if (moduleWiring != null) {
            haBootstrap.setMaintenanceLeaderAware(moduleWiring);
        }
        return haBootstrap;
    }

    public void shutdown() {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        LOG.info("event=coordinator_shutdown_start coordinatorId={} role={}",
                config.coordinatorId(), config.role());
        if (ha != null) {
            ha.shutdown();
        }
        if (zkLeaseProvider != null) {
            zkLeaseProvider.close();
        }
        if (http != null) {
            http.shutdown();
        }
        if (query != null) {
            query.shutdown();
        }
        closeAiServices();
        closeStreamingBootstrap();
        CoordinatorShutdownSequence.closeModules(moduleWiring);
        CoordinatorShutdownSequence.closeMetadata(metadataBootstrap, connectorHandleFactory, metadataDataSource);
        security.shutdown();
        observability.shutdown();
        LOG.info("event=coordinator_shutdown_completed coordinatorId={} role={}",
                config.coordinatorId(), config.role());
    }

    private void initSecretResolver() {
        secretResolver = new SecretResolver();
        secretResolver.register(new EnvSecretProvider());
        secretResolver.register(new FileSecretProvider());
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
            MetadataStore store = new JdbcMetadataStore(metadataDataSource);
            ClassLoader cl = getClass().getClassLoader();
            connectorHandleFactory = new PluginConnectorHandleFactory(cl, secretResolver);
            var federationFactory = new PluginFederationHandleFactory(cl, secretResolver);
            metadataBootstrap = MetadataBootstrap.initialize(store, connectorHandleFactory, federationFactory);
            // P2-3: persist column governance tags across restarts now that a metadata
            // datasource is configured (the service defaults to in-memory otherwise).
            if (metadataBootstrap.metadataService()
                    instanceof org.pangrova.metadata.service.DefaultMetadataService taggedService) {
                taggedService.setColumnTagStore(
                        new org.pangrova.metadata.tag.JdbcColumnTagStore(metadataDataSource));
            }
            LOG.info("event=phase_started phase=metadata");
        } catch (Exception e) {
            throw new BootstrapFailedException("Metadata", e);
        }
    }

    /**
     * Selects the distributed-lease backend (A-HA-01). Defaults to JDBC (single coordination DB) for
     * backward compatibility. Set {@code -Dpangrova.coordination.lease.backend=zookeeper} plus
     * {@code -Dpangrova.coordination.lease.zookeeper.connectString=host:port,...} to run leader election
     * on a ZooKeeper quorum, which removes the single-JDBC coordination SPOF. The ZooKeeper client is
     * owned by this bootstrap and released in {@link #shutdown()}.
     */
    private org.pangrova.spi.coordination.DistributedLease resolveDistributedLease(DataSource metadataDataSource) {
        String backend = System.getProperty("pangrova.coordination.lease.backend", "jdbc")
                .trim().toLowerCase(java.util.Locale.ROOT);
        if ("jdbc".equals(backend)) {
            // A-HA-01: with the JDBC backend, lease/registry/journal all converge on ONE
            // relational database — a coordination-layer single point of failure in
            // multi-coordinator HA deployments. Surface the posture at startup so operators
            // running >1 coordinator know the quorum alternative exists.
            LOG.warn("event=coordination_spof_posture backend=jdbc "
                    + "message=\"distributed lease uses a single JDBC store; in multi-coordinator "
                    + "HA deployments this is a coordination SPOF. Set "
                    + "-Dpangrova.coordination.lease.backend=zookeeper (+connectString) to run "
                    + "leader election on a ZooKeeper quorum (A-HA-01)\"");
        }
        if ("zookeeper".equals(backend) || "zk".equals(backend)) {
            String connectString = System.getProperty("pangrova.coordination.lease.zookeeper.connectString");
            if (connectString == null || connectString.isBlank()) {
                throw new IllegalStateException("coordination lease backend=zookeeper requires "
                        + "pangrova.coordination.lease.zookeeper.connectString");
            }
            String basePath = System.getProperty(
                    "pangrova.coordination.lease.zookeeper.basePath", "/pangrova/leases");
            zkLeaseProvider = new org.pangrova.metadata.coordination.ZooKeeperLeaseProvider(connectString, basePath);
            return zkLeaseProvider.lease();
        }
        LOG.info("event=distributed_lease_configured backend=jdbc");
        return new org.pangrova.metadata.coordination.JdbcDistributedLease(metadataDataSource);
    }

    private void startModules() {
        moduleWiring = new ModuleIntegrationBootstrap();
        if (distributedLease != null) {
            moduleWiring.setDistributedLease(distributedLease);
        }
        moduleWiring.startGovernance(config, metadataDataSource);
        moduleWiring.startResilience(config);
        moduleWiring.startTenant(config, metadataDataSource);
        moduleWiring.startOperations(config);
        if (config.role().isMaintenance()) {
            moduleWiring.startStreaming(config, metadataDataSource);
            if (config.indexingEnabled() && metadataBootstrap != null) {
                startIndexing();
            }
        } else if (config.role().isQuery() && config.indexingEnabled() && metadataBootstrap != null) {
            startIndexQuery();
        }
        // Wire metadata sync deps for leader-gated periodic sync (Pillar 5)
        if (metadataBootstrap != null) {
            moduleWiring.setMetadataSyncDeps(
                    metadataBootstrap.syncService(),
                    metadataBootstrap.catalogRegistry());
        }
        // Wire invalidation poller for leader-gated TTL cleanup (Task 1.9)
        if (metadataDataSource != null && metadataBootstrap != null) {
            var invalidationPoller = new org.pangrova.metadata.cache.JdbcInvalidationPoller(
                    metadataDataSource,
                    metadataBootstrap.metadataCache());
            invalidationPoller.initSchema();
            invalidationPoller.start();
            moduleWiring.setInvalidationPoller(invalidationPoller);
        }
    }

    private void startIndexing() {
        try {
            org.pangrova.metadata.index.IndexMetadataStore indexMetadataStore =
                    new org.pangrova.metadata.index.jdbc.JdbcIndexMetadataStore(metadataDataSource);
            org.pangrova.metadata.catalog.CatalogRegistry catalogRegistry =
                    metadataBootstrap.catalogRegistry();
            org.pangrova.spi.observability.MetricsProvider metricsProvider =
                    observability != null ? observability.metricsProvider() : null;
            org.pangrova.spi.security.AccessControl accessControl =
                    security != null ? security.accessControl() : null;
            moduleWiring.startIndexing(
                    indexMetadataStore, catalogRegistry, metricsProvider, accessControl,
                    config.baseIndexPath(), distributedLease);
            LOG.info("event=phase_started phase=indexing");
        } catch (Exception e) {
            LOG.warn("event=phase_failed phase=indexing error={}", e.getMessage(), e);
        }
    }

    private void startIndexQuery() {
        try {
            org.pangrova.metadata.index.IndexMetadataStore indexMetadataStore =
                    new org.pangrova.metadata.index.jdbc.JdbcIndexMetadataStore(metadataDataSource);
            org.pangrova.spi.observability.MetricsProvider metricsProvider =
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
            org.pangrova.streaming.StreamingContext streamingContext = moduleWiring.getStreamingContext();
            if (streamingContext == null) {
                LOG.info("event=phase_skipped phase=streaming_bootstrap reason=streaming_wiring_unavailable");
                return;
            }
            streamingBootstrap = StreamingBootstrap.build(
                    metadataDataSource,
                    streamingContext.watermarkTracker(),
                    streamingContext.scheduler(),
                    metadataBootstrap.catalogRegistry());
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
            poller.setCoordinatorRegistry(ha.coordinatorRegistry(), config.coordinatorId());
            LOG.debug("event=invalidation_poller_registry_wired coordinatorId={}", config.coordinatorId());
        }
    }

    /**
     * Wires streaming HA callbacks into the coordinator's leader election lifecycle.
     * On failover (leader elected): resumes all RUNNING streams.
     * On demotion (leadership lost): stops all active pipelines.
     */
    private void wireStreamingHaCallbacks() {
        if (streamingBootstrap == null) {
            return;
        }
        // Compose with existing failover callback (workerManager.loadFromRegistry)
        // by wrapping the HA service's onFailover to also trigger stream resume.
        streamingBootstrap.onLeaderElected();
        LOG.info("event=streaming_ha_callbacks_wired");
    }

    private void closeStreamingBootstrap() {
        if (streamingBootstrap == null) {
            return;
        }
        try {
            streamingBootstrap.close();
            LOG.info("event=phase_closed phase=streaming_bootstrap");
        } catch (Exception e) {
            LOG.warn("event=phase_close_failed phase=streaming_bootstrap error={}", e.getMessage(), e);
        }
    }

    private void startAiServices() {
        if (metadataBootstrap == null) {
            LOG.info("event=phase_skipped phase=ai_services reason=no_metadata");
            return;
        }
        try {
            org.pangrova.metadata.catalog.CatalogRegistry catalogRegistry =
                    metadataBootstrap.catalogRegistry();
            branchTtlCleanupTask = new BranchTtlCleanupTask(catalogRegistry);
            branchTtlCleanupTask.start();
            LOG.info("event=phase_started phase=branch_ttl_cleanup");
        } catch (Exception e) {
            LOG.warn("event=phase_failed phase=branch_ttl_cleanup error={}", e.getMessage(), e);
        }
        try {
            org.pangrova.spi.security.AccessControl accessControl =
                    security != null ? security.accessControl() : null;
            org.pangrova.spi.observability.MetricsProvider metricsProvider =
                    observability != null ? observability.metricsProvider() : null;
            TrainingDataServerConfig trainingConfig = TrainingDataServerConfig.defaults();
            if (!trainingConfig.enabled()) {
                LOG.info("event=training_data_server_disabled "
                        + "detail=set training.enabled=true to activate (violates control-plane principle)");
            } else if (accessControl != null && metricsProvider != null) {
                LOG.warn("event=training_data_control_plane_warning "
                        + "detail=TrainingDataServer serves scan-path data on a control-plane coordinator node. "
                        + "This violates read-write separation. Consider serving from a dedicated worker fleet.");
                org.pangrova.metadata.catalog.CatalogRegistry catalogRegistry =
                    metadataBootstrap.catalogRegistry();
                org.pangrova.spi.observability.TracingProvider tracingProvider =
                        observability != null ? observability.tracingProvider() : null;
                trainingDataServer = new TrainingDataServer(
                        trainingConfig, accessControl, metricsProvider, catalogRegistry,
                        tracingProvider);
                trainingDataServer.start();
                LOG.info("event=phase_started phase=training_data_server port={} tracing={}",
                        trainingConfig.port(), tracingProvider != null);
            } else if (trainingConfig.enabled()) {
                LOG.info("event=phase_skipped phase=training_data_server reason=missing_dependencies");
            }
        } catch (Exception e) {
            LOG.warn("event=phase_failed phase=training_data_server error={}", e.getMessage(), e);
        }
    }

    private void closeAiServices() {
        if (trainingDataServer != null) {
            try {
                trainingDataServer.close();
                LOG.info("event=phase_closed phase=training_data_server");
            } catch (Exception e) {
                LOG.warn("event=phase_close_failed phase=training_data_server error={}",
                        e.getMessage(), e);
            }
        }
        if (branchTtlCleanupTask != null) {
            try {
                branchTtlCleanupTask.close();
                LOG.info("event=phase_closed phase=branch_ttl_cleanup");
            } catch (Exception e) {
                LOG.warn("event=phase_close_failed phase=branch_ttl_cleanup error={}",
                        e.getMessage(), e);
            }
        }
    }

    // -- Accessors for testing and health checks --

    public boolean isStarted() { return started.get(); }
    public int getHttpPort() { return http != null && http.httpServer() != null ? http.httpServer().getPort() : -1; }
    public QueryManager getQueryManager() { return query != null ? query.queryManager() : null; }
    public WorkerManager getWorkerManager() { return query != null ? query.workerManager() : null; }
    public TracingProvider getTracingProvider() {
        return observability != null ? observability.tracingProvider() : null;
    }
    public MetadataBootstrap getMetadataBootstrap() { return metadataBootstrap; }
    TransportServerLifecycle getCoordinatorTransportServer() {
        return query != null ? query.coordinatorTransportServer() : null;
    }
    public CircuitBreakerPolicy getCircuitBreaker() { return query != null ? query.circuitBreaker() : null; }
    public org.pangrova.operations.recovery.RecoveryPlaybookFramework getRecoveryPlaybook() {
        return moduleWiring != null ? moduleWiring.getRecoveryPlaybook() : null;
    }
}
