package org.pangrova.server.coordinator.bootstrap;

import org.pangrova.persistence.workflow.postgresql.generation.PostgresGenerationStores;
import org.pangrova.persistence.jdbc.api.security.model.CurrentModelAuthority;

import org.pangrova.server.coordinator.lifecycle.PangrovaCoordinator;
import org.pangrova.coordinator.coordination.CoordinatorExecutionAuthority;
import org.pangrova.coordinator.coordination.HighAvailabilityCoordinationService;
import org.pangrova.query.runtime.QueryManager;
import org.pangrova.query.recovery.JournalRecoveryService;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.WorkerHealthScheduler;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.persistence.coordination.api.lease.DistributedLease;
import org.pangrova.execution.api.ownership.ExecutionAnchor;
import org.pangrova.persistence.coordination.api.membership.CoordinatorRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
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
 * <p>When a {@link CatalogRegistry} is configured,
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
    private final HighAvailabilityCoordinationService haService;
    private final CoordinatorRegistry coordinatorRegistry;
    private final CatalogRegistry catalogRegistry;
    private final boolean durableExecutionAnchors;
    private final org.pangrova.persistence.generation.api.job.GenerationJobStore artifactExecutions;
    private QueryManager queryManager;
    private WorkerManager workerManager;
    private CoordinatorTaskTracker taskTracker;
    private ScheduledExecutorService schedulerExecutor;
    private QueryBootstrap query;
    private PangrovaCoordinator coordinator;
    private WorkerHealthScheduler healthScheduler;
    private final AtomicBoolean executionRecoveryRunning = new AtomicBoolean();
    private volatile boolean querySchedulingStopped;
    private volatile java.util.concurrent.ScheduledFuture<?> recoveryFuture;
    private volatile java.util.concurrent.ScheduledFuture<?> taskTimeoutFuture;
    private long workerLivenessGraceMs = DEFAULT_WORKER_LIVENESS_GRACE_MS;

    HighAvailabilityBootstrap(CoordinatorBootstrapConfig config,
            DistributedLease lease, CoordinatorRegistry coordinatorRegistry, DataSource dataSource,
            CoordinatorExecutionAuthority executionAuthority, CurrentModelAuthority modelAuthority,
            CatalogRegistry catalogRegistry) {
        this.config = config;
        this.coordinatorRegistry = coordinatorRegistry;
        this.catalogRegistry = catalogRegistry;
        durableExecutionAnchors = dataSource != null;
        if (durableExecutionAnchors && executionAuthority == null) {
            throw new IllegalStateException("durable HA requires the process-wide execution authority binding");
        }
        var anchors = durableExecutionAnchors ? executionAuthority.backend()
                : new org.pangrova.persistence.query.memory.coordination.InMemoryExecutionAnchorStore();
        artifactExecutions = durableExecutionAnchors
                ? new PostgresGenerationStores(modelAuthority, CoordinatorTableWriteAuthority.create())
                        .builds(dataSource)
                : null;
        haService = new HighAvailabilityCoordinationService(lease, anchors);
        if (durableExecutionAnchors) {
            haService.bindExecutionAuthority(executionAuthority);
        }
    }

    void start(QueryBootstrap queryBootstrap, int httpPort) {
        query = queryBootstrap;
        queryManager = queryBootstrap.queryManager();
        workerManager = queryBootstrap.workerManager();
        taskTracker = queryBootstrap.taskTracker();
        schedulerExecutor = queryBootstrap.schedulerExecutor();
        if (coordinatorRegistry != null) {
            haService.setCoordinatorRegistry(
                    coordinatorRegistry, config.coordinatorTransportAdvertisedHost(), httpPort);
        }
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
        haService.close();
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
            org.pangrova.query.lifecycle.QueryStateJournalPort journal =
                    queryManager.getStateJournal();
            claimed = haService.claimExpiredExecutions(256);
            if (claimed.isEmpty()) {
                return;
            }
            var ordinaryClaims = claimed.stream().filter(anchor -> !isArtifactExecution(anchor)).toList();
            var deferredOrShufflePending = query.recoverWriteCommitIntents(ordinaryClaims, haService);
            var queryIds = ordinaryClaims.stream()
                    .map(ExecutionAnchor::executionId)
                    .filter(id -> !deferredOrShufflePending.contains(id))
                    .collect(java.util.stream.Collectors.toUnmodifiableSet());
            var recovered = new JournalRecoveryService(
                    journal, queryManager, taskTracker, workerManager,
                    null, workerLivenessGraceMs).recoverClaimed(queryIds);
            settleRecoveryClaims(claimed, recovered.withoutJournalEvidence());
            query.trackRecoveredTaskDrains(ordinaryClaims.stream()
                    .filter(anchor -> !recovered.withoutJournalEvidence().contains(anchor.executionId()))
                    .toList());
            LOG.info("event=execution_anchor_recovery_completed "
                            + "queriesClaimed={} queriesRecovered={} graceMs={}",
                    claimed.size(), recovered.recoveredQueries(), workerLivenessGraceMs);
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

    private void settleRecoveryClaims(java.util.List<ExecutionAnchor> claimed, Set<String> withoutJournalEvidence) {
        for (ExecutionAnchor anchor : claimed) {
            var artifact = artifactJob(anchor);
            if (artifact.isPresent()) {
                var key = artifact.orElseThrow();
                var job = artifactExecutions.findJob(key.tenantId(), key.jobId()).orElseThrow(() ->
                        new IllegalStateException("artifact recovery association lost its exact job"));
                if (job.state() == org.pangrova.generation.api.enums.GenerationJobState.PUBLISHED
                        || job.state() == org.pangrova.generation.api.enums.GenerationJobState.FAILED
                        || job.state() == org.pangrova.generation.api.enums.GenerationJobState.CANCELLED) {
                    haService.releaseRecoveryClaim(anchor);
                }
                continue;
            }
            // Admission is committed before dispatch. A local projection or a logical terminal
            // event does not prove that every originally issued attempt has actually exited.
            if (withoutJournalEvidence.contains(anchor.executionId())
                    && queryManager.getStateMachine(anchor.executionId()) == null) {
                haService.releaseRecoveryClaim(anchor);
            }
        }
    }

    private static boolean isArtifactExecution(ExecutionAnchor anchor) {
        return anchor.executionId().startsWith("artifact:");
    }

    private java.util.Optional<org.pangrova.generation.api.identity.GenerationJobKey> artifactJob(
            ExecutionAnchor anchor) {
        if (!isArtifactExecution(anchor)) {
            return java.util.Optional.empty();
        }
        if (artifactExecutions == null) {
            throw new IllegalStateException("artifact recovery requires its durable routing store");
        }
        return java.util.Optional.of(artifactExecutions.findExecutionJob(anchor.executionId()).orElseThrow(() ->
                new IllegalStateException("artifact recovery has no exact durable association")));
    }

    private void recoverExecutionsIfControlLeader() {
        try {
            if (haService.isLeader(config.coordinatorId())) {
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
        CoordinatorShutdownSequence.onDeadWorker(
                workerId, taskTracker, queryManager, query::retryTaskOnDeadWorker);
    }

    /** Provides the shared authority when assembling query and HTTP services. */
    HighAvailabilityCoordinationService haService() {
        return haService;
    }
}
