package org.pangrova.coordinator.coordination;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.spi.coordination.CoordinatorRegistry;
import org.pangrova.spi.coordination.CoordinatorRole;
import org.pangrova.spi.coordination.DistributedLease;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HA coordination service with dual-node leader election and query ownership.
 *
 * <p>Leader election is delegated to {@link LeaseBackedLeaderElection} which
 * bridges to the unified {@link DistributedLease} SPI. This service manages
 * the election lifecycle: automatic leader acquisition on startup, periodic
 * lease renewal for the leader, and periodic leader-failure detection on
 * the standby node.</p>
 *
 * <p>Query ownership is tracked by {@link QueryOwnershipRegistry} and uses epoch-based
 * zombie fencing: claims stamped with a stale epoch are rejected to prevent split-brain
 * query execution.</p>
 *
 * <p>Concurrency strategy: {@link AtomicBoolean} for running state,
 * {@link AtomicReference} for local role tracking, {@link AtomicLong}
 * for epoch, and a thread-safe ownership registry.
 * Distributed coordination is handled by the lease backend.</p>
 */
@ThreadSafe
public class HighAvailabilityCoordinationService implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(HighAvailabilityCoordinationService.class);

    /** Node role in the HA cluster. */
    public enum Role { LEADER, STANDBY, STOPPED }

    /** Ownership entry binding a query to a coordinator at a specific epoch. */
    public record OwnershipEntry(String coordinatorId, long epoch) {}

    /** Immutable HA configuration. */
    public record CoordinationConfig(long leaseTimeoutMs, long heartbeatIntervalMs,
                                     int maxQueryOwnershipEntries) {
        /** Default upper bound on tracked query-ownership entries (A-OWN-01). */
        public static final int DEFAULT_MAX_QUERY_OWNERSHIP_ENTRIES = 100_000;

        public CoordinationConfig {
            if (leaseTimeoutMs <= 0) {
                throw new IllegalArgumentException("leaseTimeoutMs must be positive");
            }
            if (heartbeatIntervalMs <= 0) {
                throw new IllegalArgumentException("heartbeatIntervalMs must be positive");
            }
            if (heartbeatIntervalMs >= leaseTimeoutMs) {
                throw new IllegalArgumentException("heartbeatIntervalMs must be less than leaseTimeoutMs");
            }
            if (maxQueryOwnershipEntries <= 0) {
                throw new IllegalArgumentException("maxQueryOwnershipEntries must be positive");
            }
        }

        /**
         * Backward-compatible constructor applying the default query-ownership cap
         * ({@link #DEFAULT_MAX_QUERY_OWNERSHIP_ENTRIES}). A-OWN-01: prefer the 3-arg
         * constructor to size the cap for the deployment.
         */
        public CoordinationConfig(long leaseTimeoutMs, long heartbeatIntervalMs) {
            this(leaseTimeoutMs, heartbeatIntervalMs, DEFAULT_MAX_QUERY_OWNERSHIP_ENTRIES);
        }

        public static CoordinationConfig defaults() {
            return new CoordinationConfig(30_000, 10_000);
        }
    }

    private final CoordinationConfig config;
    private final LeaseBackedLeaderElection leaseElection;
    private final AtomicReference<Role> role = new AtomicReference<>(Role.STOPPED);
    private final AtomicLong epoch = new AtomicLong(0);
    private final QueryOwnershipRegistry ownership;

    private final AtomicReference<Consumer<String>> failoverCallback = new AtomicReference<>();
    private final AtomicReference<Runnable> demotionCallback = new AtomicReference<>();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicReference<String> localNodeId = new AtomicReference<>();
    private final AtomicReference<String> knownLeaderId = new AtomicReference<>();
    private volatile ElectionScheduler electionScheduler;
    private final CoordinatorRegistryClient registryBridge = new CoordinatorRegistryClient();
    private volatile CoordinatorRegistry coordinatorRegistryRef;

    public HighAvailabilityCoordinationService(DistributedLease lease) {
        this(lease, CoordinationConfig.defaults());
    }

    public HighAvailabilityCoordinationService(DistributedLease lease, CoordinationConfig config) {
        Objects.requireNonNull(lease, "lease must not be null");
        this.leaseElection = new LeaseBackedLeaderElection(lease);
        this.config = Objects.requireNonNull(config, "config must not be null");
        this.ownership = new QueryOwnershipRegistry(this.config.maxQueryOwnershipEntries());
    }

    /**
     * Sets the orphan query reclaimer for persisting running queries on shutdown.
     * When set, {@link #close()} writes all owned queries to the orphaned_queries
     * table before clearing the in-memory ownership map.
     */
    public void setOrphanReclaimer(OrphanQueryReclaimer reclaimer) {
        ownership.setReclaimer(reclaimer);
    }

    /**
     * Sets the coordinator registry for self-registration and role tracking.
     * When set, {@link #start(String)} registers this coordinator,
     * {@link #tryBecomeLeader(String)} and demotion update the role,
     * and {@link #close()} unregisters. All registry calls are guarded
     * by a null check so existing callers that never set a registry
     * experience zero behavioral change.
     *
     * @param registry the coordinator registry (must not be null)
     * @param host     the advertised hostname for this coordinator
     * @param httpPort the HTTP API port for this coordinator
     */
    public void setCoordinatorRegistry(CoordinatorRegistry registry, String host, int httpPort) {
        this.coordinatorRegistryRef = Objects.requireNonNull(registry, "registry must not be null");
        registryBridge.configure(
                registry,
                Objects.requireNonNull(host, "host must not be null"),
                httpPort);
    }

    /** Returns the coordinator registry, or null if not configured. */
    public CoordinatorRegistry coordinatorRegistry() {
        return coordinatorRegistryRef;
    }

    public boolean tryBecomeLeader(String coordinatorId) {
        Objects.requireNonNull(coordinatorId, "coordinatorId must not be null");
        long currentEpoch = epoch.get();
        boolean acquired = leaseElection.tryAcquireWithEpoch(
                coordinatorId, currentEpoch, config.leaseTimeoutMs());
        if (acquired) {
            onLeaderAcquired(coordinatorId);
        } else {
            LOG.debug("event=leader_election_rejected candidate={}", coordinatorId);
        }
        return acquired;
    }

    private void onLeaderAcquired(String coordinatorId) {
        epoch.incrementAndGet();
        syncEpochFromLease();
        String previousLeader = knownLeaderId.getAndSet(coordinatorId);
        role.set(Role.LEADER);
        localNodeId.set(coordinatorId);
        updateRegistryRole(coordinatorId, CoordinatorRole.LEADER);
        // G-HA-05 fix: fire failover on ANY new leader acquisition
        // (first-time, standby promotion, leader switchover) except same-leader re-acquire.
        if (!coordinatorId.equals(previousLeader)) {
            LOG.info("event=leader_failover newLeader={} previousLeader={} epoch={}",
                    coordinatorId, previousLeader, epoch.get());
            notifyFailover(coordinatorId);
        } else {
            LOG.info("event=leader_reacquire leader={} leaseTimeoutMs={}",
                    coordinatorId, config.leaseTimeoutMs());
        }
    }

    public boolean renewLease(String coordinatorId) {
        Objects.requireNonNull(coordinatorId, "coordinatorId must not be null");
        return leaseElection.tryRenew(coordinatorId, config.leaseTimeoutMs());
    }
    /**
     * Voluntarily relinquishes leadership.
     *
     * <p>BUG-D05 fix: Notifies demotion callbacks and persists orphaned queries
     * before clearing ownership. Without this, CDC pipelines and DT refresh
     * schedulers continued running on the demoted coordinator while the new
     * leader also started them, causing duplicate processing.</p>
     *
     * <p><b>R10.3 Timing dependency:</b> The ordering of operations below is critical:
     * <ol>
     *   <li>{@code leaseElection.release()} — relinquish leadership atomically via lease</li>
     *   <li>{@code role.set(STANDBY)} — local state reflects demotion</li>
     *   <li>{@code notifyDemotion()} — callbacks stop CDC/DT schedulers on this node</li>
     *   <li>{@code persistOrphanedQueries()} — write in-flight queries for new leader to reclaim</li>
     *   <li>{@code ownership.clear()} — release local ownership map</li>
     * </ol>
     * Steps 3-4 MUST happen before step 5: clearing ownership before persisting orphans
     * would lose in-flight query records. The lease release (step 1) happens first so
     * the new leader can acquire leadership immediately; the orphan persistence (step 4)
     * is protected by the fact that the ownership registry is still populated at that point.
     * A concurrent {@code acquireQueryOwnership} call from the new leader is safe because
     * it operates on the distributed lease, not this node's local map.</p>
     */
    public boolean stepDown(String coordinatorId) {
        Objects.requireNonNull(coordinatorId, "coordinatorId must not be null");
        boolean released = leaseElection.release(coordinatorId);
        if (released) {
            role.set(Role.STANDBY);
            // G-HA-05: Clear knownLeaderId so that re-acquiring leadership
            // after a step-down is treated as a new leader acquisition.
            // Without this, the same coordinator re-acquiring would see
            // previousLeader == coordinatorId and skip the failover callback,
            // leaving maintenance singletons stopped.
            knownLeaderId.set(null);
            updateRegistryRole(coordinatorId, CoordinatorRole.STANDBY);
            notifyDemotion();
            persistOrphanedQueries(coordinatorId);
            ownership.clear();
            LOG.info("event=leader_step_down leader={} epoch={}", coordinatorId, epoch.get());
        }
        return released;
    }

    /**
     * Acquires ownership of a query for the given coordinator at the current leader epoch.
     * See {@link QueryOwnershipRegistry#acquire} for the epoch-fencing rationale (BUG-D01).
     */
    public boolean acquireQueryOwnership(String queryId, String coordinatorId) {
        Objects.requireNonNull(queryId, "queryId must not be null");
        Objects.requireNonNull(coordinatorId, "coordinatorId must not be null");
        return ownership.acquire(queryId, coordinatorId, epoch.get());
    }

    public boolean releaseQueryOwnership(String queryId, String coordinatorId) {
        Objects.requireNonNull(queryId, "queryId must not be null");
        Objects.requireNonNull(coordinatorId, "coordinatorId must not be null");
        return ownership.release(queryId, coordinatorId);
    }

    public int transferOwnership(String fromCoordinatorId, String toCoordinatorId) {
        Objects.requireNonNull(fromCoordinatorId, "fromCoordinatorId must not be null");
        Objects.requireNonNull(toCoordinatorId, "toCoordinatorId must not be null");
        return ownership.transfer(fromCoordinatorId, toCoordinatorId, epoch.incrementAndGet());
    }

    /**
     * Checks whether any node currently holds an active (non-expired) leader lease.
     * This does NOT check if the local node is the leader — use {@link #isLeader(String)} for that.
     */
    public boolean hasActiveLeader() {
        return leaseElection.currentLeader().isPresent();
    }

    public boolean isLeader(String coordinatorId) {
        return leaseElection.currentLeader().map(id -> id.equals(coordinatorId)).orElse(false);
    }

    public String getLeaderId() {
        return leaseElection.currentLeader().orElse(null);
    }

    public Role getRole() {
        return role.get();
    }

    public long getCurrentEpoch() {
        return epoch.get();
    }

    /**
     * This coordinator's own node id (null before {@link #start}). A-FENCE-01: used to stamp the
     * owning coordinator on durable write intents and to fence commits from a demoted leader.
     */
    public String getLocalNodeId() {
        return localNodeId.get();
    }

    public boolean validateEpoch(String queryId, long queryEpoch) {
        return ownership.isCurrentEpoch(queryId, queryEpoch);
    }

    public void onFailover(Consumer<String> callback) {
        this.failoverCallback.set(callback);
    }

    /**
     * Registers a callback invoked when this node is demoted from LEADER to STANDBY
     * (e.g., lease renewal failure). The callback should stop accepting new queries
     * and persist orphaned queries for the new leader to reclaim.
     */
    public void onDemotion(Runnable callback) {
        this.demotionCallback.set(callback);
    }

    /**
     * Starts the HA lifecycle for the given node. On startup, the node
     * attempts to become leader. If successful, it starts periodic lease
     * renewal. Otherwise, it enters standby mode and periodically checks
     * for leader failure to attempt takeover.
     */
    public void start(String coordinatorId) {
        Objects.requireNonNull(coordinatorId, "coordinatorId must not be null");
        if (!running.compareAndSet(false, true)) {
            LOG.warn("event=ha_start_ignored coordinatorId={} reason=already_running", coordinatorId);
            return;
        }
        localNodeId.set(coordinatorId);
        electionScheduler = new ElectionScheduler(config, running, createSchedulerCallback());
        electionScheduler.start();
        syncEpochFromLease();
        registerWithRegistry(coordinatorId);
        if (tryBecomeLeader(coordinatorId)) {
            role.set(Role.LEADER);
            electionScheduler.scheduleLeaseRenewal(coordinatorId);
            LOG.info("event=ha_started coordinatorId={} role=LEADER", coordinatorId);
        } else {
            role.set(Role.STANDBY);
            electionScheduler.scheduleFailureDetection(coordinatorId);
            LOG.info("event=ha_started coordinatorId={} role=STANDBY", coordinatorId);
        }
    }

    private ElectionScheduler.Callback createSchedulerCallback() {
        return new SchedulerCallback();
    }

    /**
     * Bridges {@link ElectionScheduler} timer callbacks onto this service's
     * lease, registry and ownership state.
     */
    private final class SchedulerCallback implements ElectionScheduler.Callback {

        @Override
        public boolean renewLease(String coordinatorId) {
            return HighAvailabilityCoordinationService.this.renewLease(coordinatorId);
        }

        @Override
        public boolean tryBecomeLeader(String coordinatorId) {
            return HighAvailabilityCoordinationService.this.tryBecomeLeader(coordinatorId);
        }

        @Override
        public Optional<String> currentLeader() {
            return leaseElection.currentLeader();
        }

        @Override
        public void onLeaseRenewalFailed(String coordinatorId) {
            role.set(Role.STANDBY);
            updateRegistryRole(coordinatorId, CoordinatorRole.STANDBY);
            notifyDemotion();
            persistOrphanedQueries(coordinatorId);
            ownership.clear();
            LOG.info("event=lease_renewal_failed coordinator={} epoch={} ownershipCleared=true",
                    coordinatorId, epoch.get());
            // A-HA-01: lease renewal failure = this coordinator lost the single JDBC coordination
            // store (the HA SPOF). Beyond demotion, raise a CRITICAL alarm so the materialized
            // SPOF / unexpected leadership loss is paged rather than passing as a quiet INFO — in
            // a sole-coordinator deployment this is a coordination outage until the store recovers
            // or a standby takes over. (Full remedy: quorum-store migration, A-HA-01 medium-term.)
            LOG.error("event=coordinator_alarm severity=CRITICAL component=ha_lease_renewal "
                            + "message=\"lease renewal failed; coordinator demoted — coordination store "
                            + "(HA single point of failure) unreachable or contended\" coordinator={} epoch={}",
                    coordinatorId, epoch.get());
        }

        @Override
        public void onHeartbeatTick(String coordinatorId) {
            refreshRegistryHeartbeat(coordinatorId);
        }
    }

    @Override
    public void close() {
        boolean wasRunning = running.compareAndSet(true, false);
        if (wasRunning) {
            // C-HA-01: Cancel and await scheduler termination BEFORE orphan
            // persistence to prevent concurrent persistOrphanedQueries calls
            // from the scheduler thread (handleLeaseRenewal demotion path).
            ElectionScheduler sched = electionScheduler;
            if (sched != null) {
                sched.cancelScheduledTask();
                sched.shutdown();
            }
        }
        // Persist running queries as orphans before clearing ownership.
        // Safe to call now — scheduler thread is fully stopped.
        String nodeId = localNodeId.get();
        persistOrphanedQueries(nodeId);
        // Always release lease if this node is leader, regardless of start() usage
        if (nodeId != null) {
            leaseElection.release(nodeId);
            unregisterFromRegistry(nodeId);
        }
        role.set(Role.STOPPED);
        ownership.clear();
        LOG.info("event=ha_service_closed epoch={}", epoch.get());
    }

    /**
     * Persists all currently owned queries as orphans so a new leader can reclaim them.
     * Called from {@link org.pangrova.coordinator.PangrovaCoordinator#stop()} during graceful
     * shutdown when drain times out and in-flight queries must be handed off.
     *
     * <p>BUG-D25: transferOwnership released ownership without persisting orphans,
     * causing the new leader to have no record of in-flight queries.</p>
     */
    public void persistOrphanedQueriesForTransfer(String coordinatorId) {
        persistOrphanedQueries(coordinatorId);
    }

    private void persistOrphanedQueries(String coordinatorId) {
        ownership.persistOrphans(coordinatorId);
    }

    private void notifyFailover(String newLeaderId) {
        Consumer<String> callback = failoverCallback.get();
        if (callback != null) {
            try {
                callback.accept(newLeaderId);
            } catch (Exception e) {
                LOG.error("event=failover_callback_error newLeader={} error={}",
                        newLeaderId, e.getMessage(), e);
            }
        }
    }

    private void notifyDemotion() {
        Runnable callback = demotionCallback.get();
        if (callback != null) {
            try {
                callback.run();
            } catch (Exception e) {
                LOG.error("event=demotion_callback_error error={}", e.getMessage(), e);
            }
        }
    }

    /**
     * Refreshes the coordinator registry heartbeat timestamp.
     */
    private void refreshRegistryHeartbeat(String coordinatorId) {
        registryBridge.heartbeat(coordinatorId);
    }

    private void registerWithRegistry(String coordinatorId) {
        registryBridge.register(coordinatorId, epoch.get());
    }

    private void updateRegistryRole(String coordinatorId, CoordinatorRole newRole) {
        registryBridge.updateRole(coordinatorId, newRole);
    }

    private void unregisterFromRegistry(String coordinatorId) {
        registryBridge.unregister(coordinatorId);
    }

    /**
     * Synchronizes the local epoch counter from the distributed lease backend.
     * Ensures the local epoch is at least as high as the persisted epoch,
     * preventing stale-epoch operations after failover.
     */
    private void syncEpochFromLease() {
        leaseElection.currentLeaderEntry().ifPresent(entry -> {
            long persistedEpoch = entry.epoch();
            epoch.updateAndGet(current -> Math.max(current, persistedEpoch));
            LOG.debug("event=epoch_synced persistedEpoch={} localEpoch={}",
                    persistedEpoch, epoch.get());
        });
    }
}
