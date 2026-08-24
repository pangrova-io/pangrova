package org.pangrova.coordinator.bootstrap;

import org.pangrova.coordinator.PangrovaCoordinator;
import org.pangrova.coordinator.coordination.HighAvailabilityCoordinationService;
import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.coordinator.query.state.JournalRecoveryService;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.WorkerHealthScheduler;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.metadata.coordination.JdbcCoordinatorRegistry;
import org.pangrova.spi.coordination.DistributedLease;
import org.pangrova.spi.coordination.CoordinatorRegistry;
import org.pangrova.coordinator.coordination.inmemory.InMemoryDistributedLease;
import org.pangrova.spi.coordination.QueryStateJournal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;

/**
 * Bootstraps the HA coordination, worker health monitoring, and task timeout checking.
 *
 * <p>Accepts a {@link DistributedLease} for distributed leader election.
 * Defaults to {@link InMemoryDistributedLease} for single-node deployments.</p>
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

    private DataSource dataSource;
    private PangrovaCoordinator coordinator;
    private HighAvailabilityCoordinationService haService;
    private WorkerHealthScheduler healthScheduler;
    private org.pangrova.spi.coordination.MaintenanceLeaderAware maintenanceLeaderAware;
    private CatalogRegistry catalogRegistry;
    private CoordinatorRegistry quorumRegistry;
    private long workerLivenessGraceMs = DEFAULT_WORKER_LIVENESS_GRACE_MS;

    HighAvailabilityBootstrap(CoordinatorBootstrapConfig config,
                QueryManager queryManager,
                WorkerManager workerManager,
                CoordinatorTaskTracker taskTracker,
                ScheduledExecutorService schedulerExecutor,
                DistributedLease distributedLease) {
        this.config = config;
        this.queryManager = queryManager;
        this.workerManager = workerManager;
        this.taskTracker = taskTracker;
        this.schedulerExecutor = schedulerExecutor;
        this.distributedLease = distributedLease;
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

    void start() {
        startHealthScheduler();
        startHA();
    }

    void shutdown() {
        CoordinatorShutdownSequence.drainQueries(coordinator);
        CoordinatorShutdownSequence.stopHealthScheduler(healthScheduler, schedulerExecutor);
        // BUG-D24 fix: close the HA service to release the leader lease,
        // persist orphaned queries, and unregister from the coordinator registry.
        // Without this, graceful shutdown leaves a stale lease (delaying failover),
        // loses in-flight queries, and leaves a stale registry entry.
        if (haService != null) {
            haService.close();
        }
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
        haService = new HighAvailabilityCoordinationService(distributedLease,
                HighAvailabilityCoordinationService.CoordinationConfig.defaults());
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
            // F-COORD-HA-03: recover journal state on failover so the newly promoted
            // leader replays the failed leader's uncommitted queries/tasks.
            recoverJournalState();
            if (maintenanceLeaderAware != null) {
                maintenanceLeaderAware.onLeaderElected();
            }
        });
        haService.onDemotion(() -> {
            if (maintenanceLeaderAware != null) {
                maintenanceLeaderAware.onLeadershipLost();
            }
        });
        haService.start(config.coordinatorId());
        // F-COORD-HA-03: Journal recovery is driven exclusively by the failover
        // callback above. This covers all three scenarios correctly:
        //  1. Leader at startup: start() wins election → notifyFailover fires
        //     synchronously → callback runs recoverJournalState().
        //  2. Standby at startup: start() loses election → no failover callback
        //     → no recovery (correct: must not replay active leader's entries).
        //  3. Standby promoted later: ElectionScheduler detects leader failure →
        //     tryBecomeLeader wins → failover callback → recoverJournalState().
        // No explicit call here — it would cause double recovery in scenario 1.
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
     * Replays journal events to rebuild in-memory query/task state after leader promotion.
     *
     * <p>Task 51 / SI-6: Recovery is owner-scoped when a previous leader is known.
     * The new leader only replays journal entries owned by the failed coordinator,
     * preventing cross-coordinator state corruption. A worker-liveness grace window
     * delays declaring UNKNOWN workers as dead, giving them time to re-register.</p>
     */
    private void recoverJournalState() {
        try {
            QueryStateJournal journal = queryManager.getStateJournal();
            // SI-6: Use the previous leader ID for owner-scoped recovery.
            // On first startup (no previous leader), recover all (unscoped).
            String previousLeader = haService.getLeaderId();
            // If we are the leader, there is no "previous" to scope to -- unscoped recovery.
            // Owner-scoped recovery only applies when recovering from a different failed leader.
            String scopedOwner = null;
            if (previousLeader != null && !previousLeader.equals(config.coordinatorId())) {
                scopedOwner = previousLeader;
            }

            var recoveryService = new JournalRecoveryService(
                    journal, queryManager, taskTracker, workerManager,
                    scopedOwner, workerLivenessGraceMs);
            int recovered = recoveryService.recover();
            LOG.info("event=journal_recovery_completed queriesRecovered={} scopedOwner={} graceMs={}",
                    recovered, scopedOwner, workerLivenessGraceMs);
        } catch (Exception e) {
            // NEW-COORD-05: per-entry poison rows are already isolated inside JournalRecoveryService
            // (each query/task replay is try/caught + skipped with a warning). A failure reaching
            // here means the whole replay could not run (e.g. metadata DB transiently unavailable).
            // Hard-failing would crash a newly-promoted leader and leave the cluster with NO leader
            // — a worse outage than degrading. Raise a CRITICAL alarm and continue starting DEGRADED:
            // un-recovered predecessor queries are fail-safe (detected + failed by worker-liveness /
            // task-timeout checks) rather than blocking the entire cluster's recovery.
            LOG.error("event=coordinator_alarm severity=CRITICAL component=ha_journal_recovery "
                    + "message=\"journal recovery could not complete; starting DEGRADED — predecessor "
                    + "in-flight queries may not be reclaimed and will be failed by liveness/timeout\" "
                    + "error={}", e.getMessage(), e);
        }
    }

    private void startTaskTimeoutChecker() {
        long intervalMs = config.workerHealthCheckIntervalMs();
        schedulerExecutor.scheduleAtFixedRate(
                () -> CoordinatorShutdownSequence.checkTaskTimeouts(taskTracker, queryManager),
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
    org.pangrova.spi.coordination.CoordinatorRegistry coordinatorRegistry() {
        return haService != null ? haService.coordinatorRegistry() : null;
    }

    /** Sets the MaintenanceLeaderAware to be notified on leader election/demotion. */
    void setMaintenanceLeaderAware(org.pangrova.spi.coordination.MaintenanceLeaderAware aware) {
        this.maintenanceLeaderAware = aware;
    }
}
