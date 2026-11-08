package org.pangrova.server.coordinator.bootstrap;

import org.pangrova.query.api.lifecycle.QueryState;

import org.pangrova.metadata.assembly.generation.GenerationStores;
import org.pangrova.persistence.jdbc.api.security.model.CurrentModelAuthority;

import org.pangrova.server.coordinator.lifecycle.PangrovaCoordinator;
import org.pangrova.coordinator.coordination.HighAvailabilityCoordinationService;
import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.query.core.recovery.JournalRecoveryService;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.WorkerHealthScheduler;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.persistence.coordination.postgresql.membership.JdbcCoordinatorRegistry;
import org.pangrova.persistence.coordination.api.lease.DistributedLease;
import org.pangrova.query.api.ownership.ExecutionAnchor;
import org.pangrova.persistence.coordination.api.membership.CoordinatorRegistry;
import org.pangrova.persistence.query.api.state.QueryStateJournal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.sql.DataSource;

/**
 * Bootstraps the HA coordination, worker health monitoring, and task timeout checking.
 *
 * <p>Accepts a {@link DistributedLease} for distributed leader election.
 * The Server composition root selects the lease backend; this bootstrap does not
 * replace it. Execution anchors use the bound durable authority or the explicit
 * non-durable Memory path.</p>
 *
 * <h3>Task 38 / G-META-02: Catalog divergence detection</h3>
 * <p>When a {@link CatalogRegistry} is configured via {@link #setCatalogRegistry},
 * the failover callback calls {@link CatalogRegistry#reconcileFromStore()} to detect
 * and repair any divergence between the local in-memory catalog registry and the
 * authoritative shared store. This ensures the newly promoted leader has a consistent
 * catalog view even if invalidation-log propagation was delayed.</p>
 *
 * <h3>Task 51 / SI-6: Owner-scoped journal recovery with grace window</h3>
 * <p>Journal recovery is owner-scoped: when a previous leader is known, only journal
 * entries owned by that coordinator are replayed. A worker-liveness grace window
 * (default 60s) prevents premature query failure for workers that have not yet
 * re-registered with the new leader after failover.</p>
 */
final class HighAvailabilityBootstrap {

    private static final Logger LOG = LoggerFactory.getLogger(HighAvailabilityBootstrap.class);

    /** Default grace window for worker re-registration after failover (SI-6). */
    static final long DEFAULT_WORKER_LIVENESS_GRACE_MS = 60_000;

    private final CoordinatorBootstrapConfig config;
    private final QueryManager queryManager;
    private final WorkerManager workerManager;
    private final CoordinatorTaskTracker taskTracker;
    private final ScheduledExecutorService schedulerExecutor;
    private final DistributedLease distributedLease;
    private final GenerationStores artifactStores;

    private DataSource dataSource;
    private PangrovaCoordinator coordinator;
    private HighAvailabilityCoordinationService haService;
    private WorkerHealthScheduler healthScheduler;
    private CatalogRegistry catalogRegistry;
    private CoordinatorRegistry quorumRegistry;
    private boolean durableExecutionAnchors;
    private org.pangrova.persistence.generation.api.job.GenerationJobStore artifactExecutions;
    private org.pangrova.coordinator.coordination.CoordinatorExecutionAuthority executionAuthority;
    private final AtomicBoolean executionRecoveryRunning = new AtomicBoolean();
    private volatile boolean querySchedulingStopped;
    private volatile java.util.concurrent.ScheduledFuture<?> recoveryFuture;
    private volatile java.util.concurrent.ScheduledFuture<?> taskTimeoutFuture;
    private java.util.function.Function<java.util.List<ExecutionAnchor>, java.util.Set<String>>
            claimedExecutionRecovery = ignored -> java.util.Set.of();
    private long workerLivenessGraceMs = DEFAULT_WORKER_LIVENESS_GRACE_MS;

    HighAvailabilityBootstrap(CoordinatorBootstrapConfig config,
                QueryManager queryManager,
                WorkerManager workerManager,
                CoordinatorTaskTracker taskTracker,
                ScheduledExecutorService schedulerExecutor,
                DistributedLease distributedLease, CurrentModelAuthority modelAuthority) {
        this.config = config;
        this.queryManager = queryManager;
        this.workerManager = workerManager;
        this.taskTracker = taskTracker;
        this.schedulerExecutor = schedulerExecutor;
        this.distributedLease = distributedLease;
        artifactStores = new GenerationStores(modelAuthority);
    }

    /**
     * Sets an optional {@link DataSource} for JDBC-backed coordinator registry.
     * When set, {@link #startHA()} constructs a {@link JdbcCoordinatorRegistry}
     * and registers it with the HA service for coordinator self-registration.
     * When not set, the HA service uses no coordinator registry (in-memory default).
     */
    void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    void setExecutionAuthority(org.pangrova.coordinator.coordination.CoordinatorExecutionAuthority authority) {
        executionAuthority = java.util.Objects.requireNonNull(authority, "executionAuthority");
    }

    /**
     * Sets a pre-built quorum-backed {@link CoordinatorRegistry} (ZooKeeper). When set it takes
     * precedence over the JDBC registry, so coordinator membership/discovery runs on the ZK quorum
     * (A-HA-01) rather than the single HA database.
     */
    void setQuorumCoordinatorRegistry(CoordinatorRegistry registry) {
        this.quorumRegistry = registry;
    }

    /**
     * Sets the catalog registry for peer divergence detection at leader election time.
     * When set, the failover callback calls {@link CatalogRegistry#reconcileFromStore()}
     * to sync the local registry with the authoritative store (Task 38 / G-META-02).
     */
    void setCatalogRegistry(CatalogRegistry catalogRegistry) {
        this.catalogRegistry = catalogRegistry;
    }

    void setClaimedExecutionRecovery(
            java.util.function.Function<java.util.List<ExecutionAnchor>, java.util.Set<String>>
                    recovery) {
        this.claimedExecutionRecovery = java.util.Objects.requireNonNull(
                recovery, "recovery must not be null");
    }

    void start() {
        startHealthScheduler();
        startHA();
    }

    void shutdown() {
        CoordinatorShutdownSequence.drainQueries(coordinator);
        stopQueryScheduling();
        CoordinatorShutdownSequence.stopHealthScheduler(healthScheduler, schedulerExecutor);
        // BUG-D24 fix: close the HA service to release the leader lease,
        // persist orphaned queries, and unregister from the coordinator registry.
        // Without this, graceful shutdown leaves a stale lease (delaying failover),
        // loses in-flight queries, and leaves a stale registry entry.
        if (haService != null) {
            haService.close();
        }
    }

    /** Cancels future query-producing callbacks; the assembly still joins their actual executor before retirement. */
    void stopQueryScheduling() {
        querySchedulingStopped = true;
        if (recoveryFuture != null) recoveryFuture.cancel(false);
        if (taskTimeoutFuture != null) taskTimeoutFuture.cancel(false);
        if (healthScheduler != null) healthScheduler.stop();
    }

    private void startHealthScheduler() {
        healthScheduler = new WorkerHealthScheduler(
                workerManager, this::onDeadWorker, schedulerExecutor,
                config.workerHealthCheckIntervalMs());
        healthScheduler.start();
        startTaskTimeoutChecker();
        LOG.info("Phase: HealthScheduler -- started");
    }

    private void startHA() {
        if (dataSource != null && executionAuthority == null) {
            throw new IllegalStateException("durable HA requires the process-wide execution authority binding");
        }
        var anchors = dataSource != null ? executionAuthority.backend()
                : new org.pangrova.persistence.query.memory.coordination.InMemoryExecutionAnchorStore();
        haService = new HighAvailabilityCoordinationService(distributedLease,
                HighAvailabilityCoordinationService.CoordinationConfig.defaults(), anchors);
        if (dataSource != null) {
            haService.bindExecutionAuthority(executionAuthority);
            durableExecutionAnchors = true;
            artifactExecutions = artifactStores.builds(dataSource);
        }
        if (quorumRegistry != null) {
            haService.setCoordinatorRegistry(quorumRegistry,
                    config.coordinatorTransportAdvertisedHost(), config.httpPort());
            LOG.info("event=coordinator_registry_configured backend=zookeeper host={} httpPort={}",
                    config.coordinatorTransportAdvertisedHost(), config.httpPort());
        } else if (dataSource != null) {
            haService.setCoordinatorRegistry(
                    new JdbcCoordinatorRegistry(dataSource),
                    config.coordinatorTransportAdvertisedHost(),
                    config.httpPort());
            LOG.info("event=coordinator_registry_configured backend=jdbc host={} httpPort={}",
                    config.coordinatorTransportAdvertisedHost(), config.httpPort());
        }
        coordinator = new PangrovaCoordinator(
                config.coordinatorId(), queryManager, workerManager, haService);
        coordinator.start();
        // BUG-D30 fix: Register failover callback BEFORE start() because start()
        // may immediately win the election and trigger notifyFailover(). If the
        // callback is registered after start(), the new leader has an empty worker
        // list until the first heartbeat cycle.
        haService.onFailover(id -> {
            workerManager.loadFromRegistry();
            // Task 38 / G-META-02: reconcile catalog registry with authoritative store
            // on leader promotion to detect and repair peer divergence.
            reconcileCatalogRegistry();
            // Query coordinators are active-active. A control-leader transition
            // must never replay live peers' journals; recover only explicitly
            // orphaned owners claimed atomically from the durable store.
            recoverExpiredJournalState();
        });
        haService.start(config.coordinatorId());
        if (durableExecutionAnchors) {
            recoveryFuture = schedulerExecutor.scheduleWithFixedDelay(
                    () -> org.pangrova.common.concurrent.ExecutorDrain.runOwned(
                            schedulerExecutor, this::recoverExecutionsIfControlLeader),
                    config.workerHealthCheckIntervalMs(),
                    config.workerHealthCheckIntervalMs(),
                    TimeUnit.MILLISECONDS);
        }
        // Recovery is driven by the singleton callback, but it consumes only
        // explicit orphan records. It never performs an unscoped replay of the
        // shared journal, because other active Coordinators may own those rows.
        LOG.info("Phase: HA -- coordinator registered as leader candidate");
    }

    /**
     * Task 38 / G-META-02: Reconcile the local catalog registry with the authoritative
     * shared store at leader election time. Detects and repairs peer divergence:
     * catalogs added or dropped by peers while this node was standby.
     */
    private void reconcileCatalogRegistry() {
        if (catalogRegistry == null) {
            return;
        }
        try {
            CatalogRegistry.DivergenceReport report = catalogRegistry.reconcileFromStore();
            if (!report.isEmpty()) {
                LOG.info("event=catalog_divergence_repaired loaded={} removed={} loadFailed={}",
                        report.loaded().size(), report.removed().size(), report.loadFailed().size());
            }
        } catch (Exception e) {
            LOG.error("event=catalog_reconcile_failed error={}", e.getMessage(), e);
        }
    }

    /**
     * Claims explicit durable orphans and replays only their original owners.
     * Active peers' journal rows are never included. Recovery is bounded by the
     * reclaimer batch size and keeps the worker-liveness grace window.
     */
    private void recoverExpiredJournalState() {
        if (querySchedulingStopped || !durableExecutionAnchors
                || !executionRecoveryRunning.compareAndSet(false, true)) {
            return;
        }
        java.util.List<ExecutionAnchor> claimed = java.util.List.of();
        try {
            QueryStateJournal journal = queryManager.getStateJournal();
            claimed = haService.claimExpiredExecutions(256);
            if (claimed.isEmpty()) {
                return;
            }
            var artifacts = claimed.stream().filter(anchor -> artifactJob(anchor).isPresent()).toList();
            java.util.Set<String> terminalCleaned = new java.util.HashSet<>(claimedExecutionRecovery.apply(
                    claimed.stream().filter(anchor -> !artifacts.contains(anchor)).toList()));
            artifacts.forEach(anchor -> terminalCleaned.add(anchor.executionId()));
            var queryIds = claimed.stream()
                    .map(ExecutionAnchor::executionId)
                    .filter(id -> !terminalCleaned.contains(id))
                    .collect(java.util.stream.Collectors.toUnmodifiableSet());
            int recovered = new JournalRecoveryService(
                    journal, queryManager, taskTracker, workerManager,
                    null, workerLivenessGraceMs).recover(queryIds);
            settleRecoveryClaims(claimed);
            LOG.info("event=execution_anchor_recovery_completed "
                            + "queriesClaimed={} queriesRecovered={} graceMs={}",
                    claimed.size(), recovered, workerLivenessGraceMs);
        } catch (Exception e) {
            // NEW-COORD-05: per-entry poison rows are already isolated inside JournalRecoveryService
            // (each query/task replay is try/caught + skipped with a warning). A failure reaching
            // here means the whole replay could not run (e.g. metadata DB transiently unavailable).
            // Hard-failing would crash a newly-promoted leader and leave the cluster with NO leader
            // — a worse outage than degrading. Raise a CRITICAL alarm and continue starting DEGRADED:
            // un-recovered predecessor queries are fail-safe (detected + failed by worker-liveness /
            // task-timeout checks) rather than blocking the entire cluster's recovery.
            for (ExecutionAnchor anchor : claimed) {
                haService.yieldExecution(anchor);
            }
            LOG.error("event=coordinator_alarm severity=CRITICAL component=execution_anchor_recovery "
                    + "message=\"journal recovery could not complete; claims yielded for retry\" "
                    + "error={}", e.getMessage(), e);
        } finally {
            executionRecoveryRunning.set(false);
        }
    }

    private void settleRecoveryClaims(java.util.List<ExecutionAnchor> claimed) {
        for (ExecutionAnchor anchor : claimed) {
            var artifact = artifactJob(anchor);
            if (artifact.isPresent()) {
                var key = artifact.orElseThrow();
                var job = artifactExecutions.findJob(key.tenantId(), key.jobId()).orElseThrow(() ->
                        new IllegalStateException("artifact recovery association lost its exact job"));
                if (job.state() == org.pangrova.generation.api.job.GenerationJobState.PUBLISHED
                        || job.state() == org.pangrova.generation.api.job.GenerationJobState.FAILED
                        || job.state() == org.pangrova.generation.api.job.GenerationJobState.CANCELLED) {
                    haService.releaseQueryOwnership(anchor.executionId(), config.coordinatorId());
                }
                continue;
            }
            var state = queryManager.getQueryState(anchor.executionId());
            if (state.isEmpty() || isTerminal(state.orElseThrow())) {
                haService.releaseQueryOwnership(
                        anchor.executionId(), config.coordinatorId());
            }
        }
    }

    private java.util.Optional<org.pangrova.generation.api.identity.GenerationJobKey> artifactJob(
            ExecutionAnchor anchor) {
        if (!anchor.executionId().startsWith("artifact:")) {
            return java.util.Optional.empty();
        }
        if (artifactExecutions == null) {
            throw new IllegalStateException("artifact recovery requires its durable routing store");
        }
        return java.util.Optional.of(artifactExecutions.findExecutionJob(anchor.executionId()).orElseThrow(() ->
                new IllegalStateException("artifact recovery has no exact durable association")));
    }

    private static boolean isTerminal(QueryState state) {
        return state == QueryState.FINISHED
                || state == QueryState.FAILED
                || state == QueryState.CANCELLED;
    }

    private void recoverExecutionsIfControlLeader() {
        try {
            if (haService != null && haService.isLeader(config.coordinatorId())) {
                recoverExpiredJournalState();
            }
        } catch (RuntimeException failure) {
            LOG.error("event=execution_anchor_recovery_poll_failed error={}",
                    failure.getMessage(), failure);
        }
    }

    private void startTaskTimeoutChecker() {
        long intervalMs = config.workerHealthCheckIntervalMs();
        taskTimeoutFuture = schedulerExecutor.scheduleAtFixedRate(
                () -> org.pangrova.common.concurrent.ExecutorDrain.runOwned(schedulerExecutor,
                        () -> CoordinatorShutdownSequence.checkTaskTimeouts(taskTracker, queryManager)),
                intervalMs, intervalMs, TimeUnit.MILLISECONDS);
        LOG.info("Phase: TaskTimeoutChecker -- started (taskTimeout={}s)",
                config.effectiveTaskTimeoutSeconds());
    }

    private void onDeadWorker(String workerId) {
        CoordinatorShutdownSequence.onDeadWorker(workerId, taskTracker, queryManager);
    }

    /** Returns the HA service for post-construction wiring into other components. */
    HighAvailabilityCoordinationService haService() {
        return haService;
    }

    /** Returns the coordinator registry, or null if not configured (no DataSource). */
    org.pangrova.persistence.coordination.api.membership.CoordinatorRegistry coordinatorRegistry() {
        return haService != null ? haService.coordinatorRegistry() : null;
    }

}
