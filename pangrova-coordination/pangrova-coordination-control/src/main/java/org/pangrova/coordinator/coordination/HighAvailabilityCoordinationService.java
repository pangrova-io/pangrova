package org.pangrova.coordinator.coordination;

import java.util.Objects;
import java.util.Optional;
import java.util.List;
import java.util.UUID;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.persistence.coordination.api.membership.CoordinatorRegistry;
import org.pangrova.coordination.api.enums.CoordinatorRole;
import org.pangrova.persistence.coordination.api.lease.DistributedLease;
import org.pangrova.execution.api.ownership.ExecutionAnchor;
import org.pangrova.persistence.query.api.coordination.ExecutionAnchorStore;
import org.pangrova.coordination.api.lease.LeaseEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cluster-control coordination and local query ownership.
 *
 * <p>Control leadership governs reconciliation and recovery, not query admission.
 * Every healthy Coordinator dispatches independently using backend-issued terms.
 * Data rows, vectors, candidates and result fusion never enter this service.</p>
 * <p>Durable query anchors bind the process incarnation and takeover fencing token,
 * independently of singleton control leadership. Atomic state and the thread-safe
 * ownership registry govern local concurrency; the lease backend governs the cluster.</p>
 */
@ThreadSafe
public class HighAvailabilityCoordinationService implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(HighAvailabilityCoordinationService.class);

    /** Immutable HA configuration. */
    public record CoordinationConfig(long leaseTimeoutMs, long heartbeatIntervalMs,
                                     int maxQueryOwnershipEntries) {
        /** Default upper bound on tracked query-ownership entries (A-OWN-01). */
        public static final int DEFAULT_MAX_QUERY_OWNERSHIP_ENTRIES = 100_000;
        private static final long DEFAULT_LEASE_TIMEOUT_MS = 30_000;
        private static final long DEFAULT_HEARTBEAT_INTERVAL_MS = 10_000;

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

        /** Creates the standard configuration with the default ownership cap. */
        public CoordinationConfig(long leaseTimeoutMs, long heartbeatIntervalMs) {
            this(leaseTimeoutMs, heartbeatIntervalMs, DEFAULT_MAX_QUERY_OWNERSHIP_ENTRIES);
        }

        public static CoordinationConfig defaults() {
            return new CoordinationConfig(DEFAULT_LEASE_TIMEOUT_MS, DEFAULT_HEARTBEAT_INTERVAL_MS);
        }
    }

    private final CoordinationConfig config;
    private final LeaseBackedLeaderElection leaseElection;
    private final AtomicReference<CoordinatorRole> role = new AtomicReference<>(CoordinatorRole.STOPPED);
    private final AtomicLong epoch = new AtomicLong(0);
    private final AtomicReference<LeaseEntry> localLease = new AtomicReference<>();
    private final QueryOwnershipRegistry ownership;

    private final AtomicReference<Consumer<String>> failoverCallback = new AtomicReference<>();
    private final AtomicReference<Runnable> demotionCallback = new AtomicReference<>();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicReference<String> localNodeId = new AtomicReference<>();
    private final AtomicReference<String> localIncarnation = new AtomicReference<>();
    private final AtomicReference<String> knownLeaderId = new AtomicReference<>();
    private volatile ElectionScheduler electionScheduler;
    private final CoordinatorRegistryClient registryClient = new CoordinatorRegistryClient();
    private volatile CoordinatorRegistry coordinatorRegistryRef;
    private volatile CoordinatorExecutionAuthority processAuthority;

    public HighAvailabilityCoordinationService(DistributedLease lease, ExecutionAnchorStore anchors) {
        this(lease, CoordinationConfig.defaults(), anchors);
    }

    /** Creates coordination with explicit bounded query ownership and heartbeat configuration. */
    public HighAvailabilityCoordinationService(DistributedLease lease, CoordinationConfig config,
            ExecutionAnchorStore anchors) {
        Objects.requireNonNull(lease, "lease must not be null");
        this.leaseElection = new LeaseBackedLeaderElection(lease);
        this.config = Objects.requireNonNull(config, "config must not be null");
        this.ownership = new QueryOwnershipRegistry(
                this.config.maxQueryOwnershipEntries(),
                Duration.ofMillis(this.config.leaseTimeoutMs()),
                Objects.requireNonNull(anchors, "execution anchor store must be selected explicitly"));
    }

    /** Uses the process-wide backend/incarnation binding without adding a second ownership authority. */
    public void bindExecutionAuthority(CoordinatorExecutionAuthority authority) {
        Objects.requireNonNull(authority, "authority");
        if (running.get()) throw new IllegalStateException("execution authority must be bound before HA starts");
        if (processAuthority != null) {
            throw new IllegalStateException("Execution authority is already bound");
        }
        ownership.bindLeaseDuration(authority.backend(), authority.leaseDuration());
        authority.bindQueryOwnership(ownership);
        localIncarnation.set(authority.incarnation());
        processAuthority = authority;
    }

    /** Configures registry self-registration, heartbeats, role updates and shutdown. */
    public void setCoordinatorRegistry(CoordinatorRegistry registry, String host, int httpPort) {
        this.coordinatorRegistryRef = Objects.requireNonNull(registry, "registry must not be null");
        registryClient.configure(
                registry,
                Objects.requireNonNull(host, "host must not be null"),
                httpPort);
    }

    /** Returns the coordinator registry, or null if not configured. */
    public CoordinatorRegistry coordinatorRegistry() {
        return coordinatorRegistryRef;
    }

    /** Acquires the singleton control term independently of query ownership. */
    public boolean tryBecomeLeader(String coordinatorId) {
        Objects.requireNonNull(coordinatorId, "coordinatorId must not be null");
        Optional<LeaseEntry> acquired = leaseElection.tryAcquire(
                coordinatorId, config.leaseTimeoutMs());
        if (acquired.isEmpty()) {
            LOG.debug("event=leader_election_rejected candidate={}", coordinatorId);
            return false;
        }
        try {
            onLeaderAcquired(acquired.orElseThrow());
            return true;
        } catch (RuntimeException | Error failure) {
            // The grant is real even if publishing its role failed. Seal local leadership first.
            try {
                stepDown(coordinatorId);
            } catch (RuntimeException | Error cleanup) {
                if (cleanup != failure) failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    private void onLeaderAcquired(LeaseEntry acquired) {
        String coordinatorId = acquired.holderId();
        localLease.set(acquired);
        epoch.set(acquired.epoch());
        String previousLeader = knownLeaderId.getAndSet(coordinatorId);
        role.set(CoordinatorRole.LEADER);
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

    /** Renews only the current local control term. */
    public boolean renewLease(String coordinatorId) {
        Objects.requireNonNull(coordinatorId, "coordinatorId must not be null");
        LeaseEntry held = localLease.get();
        return held != null && coordinatorId.equals(held.holderId())
                && leaseElection.tryRenew(
                        coordinatorId, held.epoch(), config.leaseTimeoutMs());
    }
    /** Relinquishes control leadership without releasing active-active query ownership. */
    public boolean stepDown(String coordinatorId) {
        Objects.requireNonNull(coordinatorId, "coordinatorId must not be null");
        LeaseEntry held = localLease.get();
        if (held == null || !coordinatorId.equals(held.holderId())) return false;
        localLease.compareAndSet(held, null);
        role.set(CoordinatorRole.STANDBY);
        knownLeaderId.set(null);
        Throwable failure = cleanup(null, this::notifyDemotion);
        boolean released = false;
        try {
            released = leaseElection.release(coordinatorId, held.epoch());
        } catch (RuntimeException | Error releaseFailure) {
            if (failure == null) {
                failure = releaseFailure;
            } else if (failure != releaseFailure) {
                failure.addSuppressed(releaseFailure);
            }
        }
        failure = cleanup(failure, () -> updateRegistryRole(coordinatorId, CoordinatorRole.STANDBY));
        if (failure instanceof RuntimeException runtime) throw runtime;
        if (failure instanceof Error error) throw error;
        LOG.info("event=leader_step_down leader={} epoch={} released={}", coordinatorId, epoch.get(), released);
        return released;
    }

    /**
     * Acquires a durable execution anchor for a query. The returned ownership is
     * independent from the singleton cluster-control term.
     */
    public boolean acquireQueryOwnership(String queryId, String coordinatorId) {
        Objects.requireNonNull(queryId, "queryId must not be null");
        Objects.requireNonNull(coordinatorId, "coordinatorId must not be null");
        CoordinatorExecutionAuthority authority = processAuthority;
        if (authority != null && !authority.coordinatorId().equals(coordinatorId)) {
            return false;
        }
        return ownership.acquire(queryId, coordinatorId, requireLocalIncarnation());
    }

    /** Completes only a query fenced to this exact local process. */
    public boolean releaseQueryOwnership(String queryId, String coordinatorId) {
        Objects.requireNonNull(queryId, "queryId must not be null");
        Objects.requireNonNull(coordinatorId, "coordinatorId must not be null");
        return ownership.release(
                queryId, coordinatorId, requireLocalIncarnation());
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

    public CoordinatorRole getRole() {
        return role.get();
    }

    public long getCurrentEpoch() {
        return epoch.get();
    }

    /**
     * Returns the durable epoch of the active control leader, including when this
     * node is a standby. Metadata commits use this term, not the locally cached one.
     */
    public long getActiveLeaderEpoch() {
        return leaseElection.currentLeaderEntry().map(LeaseEntry::epoch).orElse(0L);
    }

    /**
     * Checks that this node's cached cluster term is still the active durable
     * control term. This is used only at metadata commit boundaries; the query
     * data plane must not synchronously consult the coordination backend.
     */
    public boolean isCurrentControlTerm() {
        Optional<LeaseEntry> active = leaseElection.currentLeaderEntry();
        if (active.isEmpty()) {
            return false;
        }
        long activeEpoch = active.orElseThrow().epoch();
        epoch.accumulateAndGet(activeEpoch, Math::max);
        return epoch.get() == activeEpoch;
    }

    /**
     * This coordinator's own node id (null before {@link #start}). A-FENCE-01: used to stamp the
     * owning coordinator on durable write intents.
     */
    public String getLocalNodeId() {
        return localNodeId.get();
    }

    public boolean validateExecutionToken(String queryId, long fencingToken) {
        return ownership.isCurrentToken(queryId, fencingToken);
    }

    /** Performs an authoritative store check at a side-effecting commit boundary. */
    public boolean validateDurableExecutionToken(String queryId, long fencingToken) {
        return ownership.isDurablyCurrent(queryId, fencingToken);
    }

    /** Returns this process's locally cached execution anchor for task stamping. */
    public ExecutionAnchor requireExecutionAnchor(String executionId) {
        return ownership.localAnchor(executionId).orElseThrow(() ->
                new IllegalStateException(
                        "No live local execution anchor: " + executionId));
    }

    /** Atomically claims a bounded batch of expired executions for recovery. */
    public List<ExecutionAnchor> claimExpiredExecutions(int maxBatchSize) {
        return ownership.claimExpired(
                Objects.requireNonNull(localNodeId.get(), "coordinator is not started"),
                requireLocalIncarnation(), maxBatchSize);
    }

    /** Relinquishes one failed recovery claim without marking the query complete. */
    public boolean yieldExecution(ExecutionAnchor anchor) {
        return ownership.yield(anchor);
    }

    /** Makes all executions owned by this exact process eligible for takeover. */
    public int yieldLocalExecutions() {
        String nodeId = Objects.requireNonNull(
                localNodeId.get(), "coordinator is not started");
        return ownership.yieldOwned(nodeId, requireLocalIncarnation());
    }

    /** Returns the unique identity of this Coordinator process. */
    public String getLocalIncarnation() {
        return requireLocalIncarnation();
    }

    public void onFailover(Consumer<String> callback) {
        this.failoverCallback.set(callback);
    }

    /**
     * Registers a callback invoked when this node loses singleton control leadership.
     * The callback stops only leader-exclusive control loops; query admission and
     * execution remain active on this healthy Coordinator. The callback must be idempotent:
     * shutdown may repeat cleanup after a partially failed demotion. Failures propagate to the
     * lifecycle owner and do not prevent remote release/registry cleanup from being attempted.
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
        CoordinatorExecutionAuthority authority = processAuthority;
        if (authority != null && !authority.coordinatorId().equals(coordinatorId)) {
            throw new IllegalArgumentException("HA node differs from bound process authority");
        }
        if (!running.compareAndSet(false, true)) {
            LOG.warn("event=ha_start_ignored coordinatorId={} reason=already_running", coordinatorId);
            return;
        }
        localNodeId.set(coordinatorId);
        localIncarnation.compareAndSet(null, UUID.randomUUID().toString());
        try {
            electionScheduler = new ElectionScheduler(config, running, createSchedulerCallback());
            electionScheduler.start();
            syncEpochFromLease();
            registerWithRegistry(coordinatorId);
            if (tryBecomeLeader(coordinatorId)) {
                role.set(CoordinatorRole.LEADER);
                electionScheduler.scheduleLeaseRenewal(coordinatorId);
                LOG.info("event=ha_started coordinatorId={} role=LEADER", coordinatorId);
            } else {
                role.set(CoordinatorRole.STANDBY);
                electionScheduler.scheduleFailureDetection(coordinatorId);
                LOG.info("event=ha_started coordinatorId={} role=STANDBY", coordinatorId);
            }
        } catch (RuntimeException | Error failure) {
            try {
                close();
            } catch (RuntimeException | Error cleanup) {
                if (cleanup != failure) failure.addSuppressed(cleanup);
            }
            throw failure;
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
            Optional<LeaseEntry> current = leaseElection.currentLeaderEntry();
            current.ifPresent(entry -> epoch.accumulateAndGet(entry.epoch(), Math::max));
            return current.map(LeaseEntry::holderId);
        }

        @Override
        public void onLeaseRenewalFailed(String coordinatorId) {
            localLease.set(null);
            role.set(CoordinatorRole.STANDBY);
            knownLeaderId.set(null);
            Throwable failure = cleanup(null, HighAvailabilityCoordinationService.this::notifyDemotion);
            failure = cleanup(failure, () -> updateRegistryRole(coordinatorId, CoordinatorRole.STANDBY));
            if (failure instanceof RuntimeException runtime) throw runtime;
            if (failure instanceof Error error) throw error;
            LOG.info("event=lease_renewal_failed coordinator={} epoch={} queryOwnershipRetained=true",
                    coordinatorId, epoch.get());
            LOG.error("event=coordinator_alarm severity=CRITICAL component=ha_lease_renewal "
                            + "message=\"lease renewal failed; coordinator demoted — coordination store "
                            + "(HA single point of failure) unreachable or contended\" coordinator={} epoch={}",
                    coordinatorId, epoch.get());
        }

        @Override
        public void onHeartbeatTick(String coordinatorId) {
            refreshRegistryHeartbeat(coordinatorId);
            renewExecutionAnchors(coordinatorId);
        }
    }

    @Override
    public void close() {
        running.set(false);
        role.set(CoordinatorRole.STOPPED);
        knownLeaderId.set(null);
        Throwable failure = null;
        ElectionScheduler sched = electionScheduler;
        if (sched != null) {
            failure = cleanup(failure, sched::cancelScheduledTask);
            failure = cleanup(failure, sched::shutdown);
        }
        failure = cleanup(failure, this::notifyDemotion);
        String nodeId = localNodeId.get();
        String incarnation = localIncarnation.get();
        if (nodeId != null && incarnation != null) {
            failure = cleanup(failure, this::yieldLocalExecutions);
        }
        LeaseEntry held = localLease.getAndSet(null);
        if (nodeId != null) {
            if (held != null && nodeId.equals(held.holderId())) {
                failure = cleanup(failure, () -> leaseElection.release(nodeId, held.epoch()));
            }
            failure = cleanup(failure, () -> unregisterFromRegistry(nodeId));
        }
        failure = cleanup(failure, ownership::clear);
        CoordinatorExecutionAuthority authority = processAuthority;
        if (authority != null) {
            failure = cleanup(failure, () -> authority.unbindQueryOwnership(ownership));
        }
        localIncarnation.set(null);
        LOG.info("event=ha_service_closed epoch={}", epoch.get());
        if (failure instanceof RuntimeException runtime) throw runtime;
        if (failure instanceof Error error) throw error;
    }

    private static Throwable cleanup(Throwable primary, Runnable action) {
        try {
            action.run();
        } catch (RuntimeException | Error failure) {
            if (primary == null) return failure;
            if (failure != primary) primary.addSuppressed(failure);
        }
        return primary;
    }

    private void notifyFailover(String newLeaderId) {
        Consumer<String> callback = failoverCallback.get();
        if (callback != null) callback.accept(newLeaderId);
    }

    private void notifyDemotion() {
        Runnable callback = demotionCallback.get();
        if (callback != null) callback.run();
    }

    /**
     * Refreshes the coordinator registry heartbeat timestamp.
     */
    private void refreshRegistryHeartbeat(String coordinatorId) {
        registryClient.heartbeat(coordinatorId);
    }

    private void renewExecutionAnchors(String coordinatorId) {
        if (processAuthority != null) return;
        String incarnation = requireLocalIncarnation();
        if (!ownership.renewOwned(coordinatorId, incarnation)) {
            throw new IllegalStateException(
                    "One or more execution anchors could not be renewed");
        }
    }

    private String requireLocalIncarnation() {
        String incarnation = localIncarnation.get();
        if (incarnation == null) {
            incarnation = UUID.randomUUID().toString();
            if (!localIncarnation.compareAndSet(null, incarnation)) {
                incarnation = localIncarnation.get();
            }
        }
        return incarnation;
    }

    private void registerWithRegistry(String coordinatorId) {
        registryClient.register(coordinatorId, epoch.get());
    }

    private void updateRegistryRole(String coordinatorId, CoordinatorRole newRole) {
        registryClient.updateRole(coordinatorId, newRole);
    }

    private void unregisterFromRegistry(String coordinatorId) {
        registryClient.unregister(coordinatorId);
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
