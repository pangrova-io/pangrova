package org.pangrova.coordinator.worker;

import org.pangrova.scheduler.api.placement.HealthState;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.pangrova.protocol.capability.inventory.CapabilityInventoryCodec;
import org.pangrova.common.value.MemoryPressure;
import org.pangrova.coordination.api.heartbeat.HeartbeatSnapshot;
import org.pangrova.persistence.coordination.api.membership.WorkerRegistry;
import org.pangrova.coordination.api.membership.WorkerSnapshot;
import org.pangrova.observability.api.metrics.LongCounter;

import static org.pangrova.common.util.Preconditions.checkNotNull;

import org.pangrova.common.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Registry and health evaluator for cluster Workers.
 * Manages heartbeat-driven health transitions: ACTIVE → SUSPECT → DEAD.
 *
 * <p>Concurrency strategy: ConcurrentHashMap for worker registry.
 * WorkerInfo snapshots are immutable records; state transitions
 * use ConcurrentHashMap.compute for atomicity.</p>
 */
@ThreadSafe
public final class WorkerManager {

    private static final Logger LOG = LoggerFactory.getLogger(WorkerManager.class);

    private static final long DEFAULT_SUSPECT_THRESHOLD_MS = 20_000;
    private static final long DEFAULT_DEAD_THRESHOLD_MS = 60_000;

    private final long suspectThresholdMs;
    private final long deadThresholdMs;
    private final WorkerRegistry registry;
    private final ConcurrentHashMap<String, WorkerInfo> workers = new ConcurrentHashMap<>();
    private final WorkerCapabilityInventoryPool inventoryPool = new WorkerCapabilityInventoryPool();
    private final List<TopologyListener> topologyListeners = new java.util.concurrent.CopyOnWriteArrayList<>();
    private volatile LongCounter listenerFailureCounter;
    private volatile ClusterTenantUsageAggregator tenantUsageAggregator;

    /**
     * Heartbeat payload from a Worker.
     */
    public record HeartbeatData(
            MemoryPressure pressure,
            double cpuUtilization,
            int runningTaskCount,
            long availableMemoryBytes,
            org.pangrova.coordination.api.heartbeat.CacheCatalogSummary cacheSummary,
            Map<String, org.pangrova.protocol.rest.worker.TenantUsageSnapshot> tenantUsage) {

        public HeartbeatData {
            tenantUsage = tenantUsage == null ? Map.of() : Map.copyOf(tenantUsage);
        }

        /** Convenience constructor for workers without cache reporting. */
        public HeartbeatData(MemoryPressure pressure, double cpuUtilization,
                             int runningTaskCount, long availableMemoryBytes) {
            this(pressure, cpuUtilization, runningTaskCount,
                    availableMemoryBytes, null, Map.of());
        }

        /** Convenience constructor including the cache catalog summary. */
        public HeartbeatData(MemoryPressure pressure, double cpuUtilization,
                             int runningTaskCount, long availableMemoryBytes,
                             org.pangrova.coordination.api.heartbeat.CacheCatalogSummary cacheSummary) {
            this(pressure, cpuUtilization, runningTaskCount,
                    availableMemoryBytes, cacheSummary, Map.of());
        }

    }

    /** Creates a manager with default health thresholds and an explicitly selected backend. */
    public WorkerManager(WorkerRegistry registry) {
        this(DEFAULT_SUSPECT_THRESHOLD_MS, DEFAULT_DEAD_THRESHOLD_MS, registry);
    }

    /**
     * Creates a WorkerManager with configurable health thresholds and an
     * explicit {@link WorkerRegistry} for write-through persistence.
     *
     * @param suspectThresholdMs milliseconds without heartbeat before marking SUSPECT
     * @param deadThresholdMs    milliseconds without heartbeat before marking DEAD
     * @param registry           the worker registry for write-through persistence
     */
    public WorkerManager(long suspectThresholdMs, long deadThresholdMs, WorkerRegistry registry) {
        if (suspectThresholdMs <= 0) {
            throw new IllegalArgumentException("suspectThresholdMs must be > 0");
        }
        if (deadThresholdMs <= suspectThresholdMs) {
            throw new IllegalArgumentException("deadThresholdMs must be > suspectThresholdMs");
        }
        this.suspectThresholdMs = suspectThresholdMs;
        this.deadThresholdMs = deadThresholdMs;
        this.registry = checkNotNull(registry, "registry must not be null");
    }

    /** Persists and publishes a newly admitted Worker in the topology view. */
    public void registerWorker(WorkerInfo info) {
        registerWorker(info, false);
    }

    /** Automatic re-registration may repair absence but cannot replace another process. */
    public boolean refreshWorker(WorkerInfo info) {
        return registerWorker(info, true);
    }

    private boolean registerWorker(WorkerInfo info, boolean refresh) {
        checkNotNull(info, "WorkerInfo must not be null");
        WorkerInfo registered = inventoryPool.share(info);
        AtomicBoolean accepted = new AtomicBoolean();
        workers.compute(registered.workerId(), (id, existing) -> {
            // Serialize persistence and local publication with conditional withdrawal.
            WorkerInfo next = existing != null
                    && existing.workerExecutionIdentity().equals(registered.workerExecutionIdentity())
                    && existing.healthState() == HealthState.DECOMMISSIONING
                    ? existing : registered;
            if (refresh) {
                if (!registry.refreshRegistration(toSnapshot(next))) return existing;
            } else {
                registry.register(toSnapshot(next));
            }
            accepted.set(true);
            if (existing == null || !existing.workerExecutionIdentity().equals(next.workerExecutionIdentity())) {
                dropTenantUsage(id);
            }
            return next;
        });
        if (accepted.get()) fireTopologyChanged();
        return accepted.get();
    }

    /** Updates the exact process only after durable acceptance, serialized with local withdrawal. */
    public org.pangrova.coordination.api.heartbeat.WorkerHeartbeatStatus onHeartbeat(
            org.pangrova.coordination.api.membership.WorkerRegistrationIdentity expected, HeartbeatData data) {
        checkNotNull(expected, "expected must not be null");
        checkNotNull(data, "HeartbeatData must not be null");
        var status = new AtomicReference<>(org.pangrova.coordination.api.heartbeat.WorkerHeartbeatStatus.REGISTRATION_REQUIRED);
        AtomicBoolean changed = new AtomicBoolean();
        workers.compute(expected.workerId(), (id, existing) -> {
            var accepted = registry.heartbeat(expected, toHeartbeatSnapshot(data));
            if (accepted.isEmpty()) {
                var current = registry.getWorker(id);
                if (current.isPresent() && !current.orElseThrow().registrationIdentity().equals(expected)) {
                    status.set(org.pangrova.coordination.api.heartbeat.WorkerHeartbeatStatus.FENCED);
                    if (existing == null || !toSnapshot(existing).registrationIdentity()
                            .equals(current.orElseThrow().registrationIdentity())) {
                        dropTenantUsage(id);
                        changed.set(true);
                        return inventoryPool.share(fromSnapshot(current.orElseThrow()));
                    }
                }
                return existing;
            }
            boolean same = existing != null && toSnapshot(existing).registrationIdentity().equals(expected);
            WorkerInfo base = same ? existing : inventoryPool.share(fromSnapshot(accepted.orElseThrow()));
            WorkerInfo next = buildUpdatedWorker(base, data);
            changed.set(!same || base.healthState() != next.healthState());
            ClusterTenantUsageAggregator aggregator = tenantUsageAggregator;
            if (aggregator != null) aggregator.updateWorkerUsage(id, data.tenantUsage());
            status.set(org.pangrova.coordination.api.heartbeat.WorkerHeartbeatStatus.ACCEPTED);
            return next;
        });
        if (changed.get()) fireTopologyChanged();
        return status.get();
    }

    /**
     * Sets the cluster-wide tenant usage aggregator fed by worker heartbeats (P1-5).
     */
    public void setTenantUsageAggregator(ClusterTenantUsageAggregator aggregator) {
        this.tenantUsageAggregator = aggregator;
    }

    /**
     * Returns the cluster-wide tenant usage view aggregated from worker heartbeats,
     * or {@code null} if no aggregator has been wired.
     */
    public org.pangrova.scheduler.api.quota.GlobalTenantUsageView tenantUsageView() {
        return tenantUsageAggregator;
    }

    private void dropTenantUsage(String workerId) {
        ClusterTenantUsageAggregator aggregator = this.tenantUsageAggregator;
        if (aggregator != null) {
            aggregator.removeWorker(workerId);
        }
    }

    /** Evaluates the Worker's heartbeat age and updates topology on a liveness transition. */
    public HealthState evaluateHealth(String workerId, Instant now) {
        // BUG-C04: Use computeIfPresent to atomically read + evaluate + update,
        // eliminating the TOCTOU race between get() and computeIfPresent().
        // BUG-32 fix: default to UNKNOWN instead of DEAD for non-existent workers
        // to prevent spurious topology change events.
        AtomicReference<HealthState> result = new AtomicReference<>(HealthState.UNKNOWN);
        AtomicBoolean topologyChanged = new AtomicBoolean(false);
        workers.computeIfPresent(workerId, (id, existing) -> {
            if (existing.healthState() == HealthState.DECOMMISSIONING) {
                result.set(HealthState.DECOMMISSIONING);
                return existing;
            }
            long elapsed = Duration.between(existing.lastHeartbeat(), now).toMillis();
            HealthState state = classifyHealth(elapsed);
            result.set(state);
            if (state != existing.healthState()) {
                if (state == HealthState.DEAD || existing.healthState() == HealthState.DEAD) {
                    topologyChanged.set(true);
                }
                return existing.withHealthState(state);
            }
            return existing;
        });
        if (topologyChanged.get()) {
            fireTopologyChanged();
        }
        return result.get();
    }

    public List<WorkerInfo> getActiveWorkers() {
        return workers.values().stream()
                .filter(w -> w.healthState() == HealthState.ACTIVE)
                .toList();
    }

    public List<WorkerInfo> getAllWorkers() {
        return List.copyOf(workers.values());
    }

    /** Removes dead Workers from both the local view and durable registry. */
    public void removeDeadWorkers() {
        workers.values().stream().filter(w -> w.healthState() == HealthState.DEAD)
                .toList().forEach(this::removeObservedWorker);
    }

    /** Explicit withdrawal of the currently observed registration (not a safe-shutdown permit). */
    public void removeWorker(String workerId) {
        WorkerInfo observed = workers.get(workerId);
        if (observed != null) removeObservedWorker(observed);
    }

    /** Idempotent withdrawal of a caller's exact process, not the current logical ID owner. */
    public boolean removeWorker(org.pangrova.coordination.api.membership.WorkerRegistrationIdentity expected) {
        checkNotNull(expected, "expected must not be null");
        AtomicBoolean removed = new AtomicBoolean();
        workers.compute(expected.workerId(), (id, current) -> {
            if (current != null && !toSnapshot(current).registrationIdentity().equals(expected)) return current;
            boolean persisted = registry.unregister(expected);
            if (current != null) dropTenantUsage(id);
            removed.set(persisted || current != null);
            return null;
        });
        if (removed.get()) fireTopologyChanged();
        return removed.get();
    }

    /**
     * Withdraws only the unchanged local observation and its exact persistent incarnation.
     * A heartbeat or replacement published after the decision makes that decision stale.
     * Persistence failures leave local state and usage intact for an explicit retry.
     */
    public boolean removeObservedWorker(WorkerInfo observed) {
        checkNotNull(observed, "observed must not be null");
        AtomicBoolean removed = new AtomicBoolean();
        workers.computeIfPresent(observed.workerId(), (id, current) -> {
            if (current != observed) return current;
            registry.unregister(toSnapshot(observed).registrationIdentity());
            dropTenantUsage(id);
            removed.set(true);
            return null;
        });
        if (removed.get()) fireTopologyChanged();
        return removed.get();
    }

    /**
     * Initiates graceful decommission of a worker. The worker transitions to
     * DECOMMISSIONING state, which removes it from the active placement view.
     * Heartbeats and same-incarnation registration preserve that state. This
     * transition does not itself fence dispatches selected earlier or prove that
     * completed Shuffle outputs have no readers; it is not a shutdown permit.
     *
     * @param workerId the worker to decommission
     * @return true if the worker was found and transitioned to DECOMMISSIONING
     */
    public boolean decommission(String workerId) {
        AtomicBoolean changed = new AtomicBoolean(false);
        workers.computeIfPresent(workerId, (id, existing) -> {
            if (existing.healthState() == HealthState.DECOMMISSIONING
                    || existing.healthState() == HealthState.DEAD) {
                return existing;
            }
            changed.set(true);
            LOG.info("event=worker_decommission_start workerId={} runningTasks={}",
                    workerId, existing.runningTaskCount());
            return existing.withHealthState(HealthState.DECOMMISSIONING);
        });
        if (changed.get()) {
            fireTopologyChanged();
        }
        return changed.get();
    }

    /**
     * Checks the current heartbeat's task-count drain signal. This does not prove
     * physical execution/output drain and must not be treated as a safe-shutdown permit.
     *
     * @param workerId the worker to check
     * @return true if the worker is decommissioning and has no running tasks
     */
    public boolean isDecommissionComplete(String workerId) {
        WorkerInfo info = workers.get(workerId);
        return info != null
                && info.healthState() == HealthState.DECOMMISSIONING
                && info.runningTaskCount() == 0;
    }

    public WorkerInfo getWorker(String workerId) {
        return workers.get(workerId);
    }

    /**
     * Finds the first active worker matching the given host address.
     * Returns null if no matching worker is found.
     */
    public WorkerInfo findWorkerByHost(String host) {
        return workers.values().stream()
                .filter(w -> w.host().equals(host))
                .findFirst()
                .orElse(null);
    }

    /**
     * Registers a listener to be notified when the active worker topology changes.
     */
    public void addTopologyListener(TopologyListener listener) {
        checkNotNull(listener, "TopologyListener must not be null");
        topologyListeners.add(listener);
    }

    /**
     * Sets the counter for tracking topology listener failures.
     * When set, each listener exception increments this counter.
     */
    public void setListenerFailureCounter(LongCounter counter) {
        this.listenerFailureCounter = counter;
    }

    private void fireTopologyChanged() {
        List<WorkerInfo> active = getActiveWorkers();
        for (TopologyListener listener : topologyListeners) {
            try {
                listener.onTopologyChanged(active);
            } catch (Exception e) {
                LOG.error("event=topology_listener_failed listener={}",
                        listener.getClass().getSimpleName(), e);
                LongCounter counter = listenerFailureCounter;
                if (counter != null) {
                    counter.add(1);
                }
            }
        }
    }

    private static WorkerInfo buildUpdatedWorker(WorkerInfo existing, HeartbeatData data) {
        // F-CHAIN-CACHE-02: refresh cacheSummary on every heartbeat.
        org.pangrova.coordination.api.heartbeat.CacheCatalogSummary newSummary =
                data.cacheSummary() != null ? data.cacheSummary() : existing.cacheSummary();
        return new WorkerInfo(
                existing.workerId(), existing.host(),
                existing.httpPort(), existing.transportPort(),
                existing.resultServerPort(),
                existing.healthState() == HealthState.DECOMMISSIONING
                        ? HealthState.DECOMMISSIONING : HealthState.ACTIVE, data.pressure(),
                data.cpuUtilization(), data.runningTaskCount(),
                data.availableMemoryBytes(), Instant.now(),
                existing.performanceScore(), existing.resourcePoolId(),
                existing.protocolVersion(),
                existing.rackId(), existing.dataLocality(), newSummary,
                existing.capabilityInventory(), existing.copyTaskTlsPort(), existing.workerExecutionIdentity());
    }

    /**
     * Loads active workers from the registry into the local cache.
     * Used during failover recovery so the new leader can immediately
     * schedule queries without waiting for heartbeat cycles.
     */
    public void loadFromRegistry() {
        List<WorkerSnapshot> active = registry.getActiveWorkers(Duration.ofMillis(deadThresholdMs));
        for (WorkerSnapshot snapshot : active) {
            WorkerInfo info = inventoryPool.share(fromSnapshot(snapshot));
            workers.putIfAbsent(info.workerId(), info);
        }
        fireTopologyChanged();
        LOG.info("event=registry_loaded workerCount={}", active.size());
    }

    /**
     * Attempts to recover a single worker from the persistent registry into
     * the in-memory cache. Used when a heartbeat arrives for a worker that
     * exists in the DB (written by the previous leader) but not in memory
     * (post-failover scenario).
     *
     * @param workerId the worker to recover
     * @return true if the worker was found in the registry and loaded
     */
    public boolean tryRecoverFromRegistry(String workerId) {
        checkNotNull(workerId, "workerId must not be null");
        if (workers.containsKey(workerId)) {
            return true;
        }
        return registry.getWorker(workerId)
                .map(snapshot -> {
                    WorkerInfo info = inventoryPool.share(fromSnapshot(snapshot));
                    workers.putIfAbsent(info.workerId(), info);
                    fireTopologyChanged();
                    LOG.info("event=worker_recovered_from_registry workerId={}", workerId);
                    return true;
                })
                .orElse(false);
    }

    /**
     * Returns the dead threshold in milliseconds. Package-private so that
     * {@link WorkerHealthScheduler} can derive the stale-entry expiration
     * duration without duplicating configuration.
     */
    long deadThresholdMs() {
        return deadThresholdMs;
    }

    /**
     * Delegates stale-entry expiration to the underlying registry.
     *
     * @param threshold maximum age of the last heartbeat; workers older
     *                  than this are expired and removed from the registry
     * @return the number of stale entries removed
     */
    public int expireStaleFromRegistry(Duration threshold) {
        return registry.expireStale(threshold);
    }

    private WorkerSnapshot toSnapshot(WorkerInfo info) {
        return new WorkerSnapshot(
                info.workerId(),
                info.host(),
                info.httpPort(),
                info.transportPort(),
                info.protocolVersion(),
                info.rackId(),
                info.dataLocality(),
                info.memoryPressure().name(),
                info.cpuUtilization(),
                info.runningTaskCount(),
                info.availableMemoryBytes(),
                info.lastHeartbeat().toEpochMilli(),
                info.lastHeartbeat().toEpochMilli(),
                info.resultServerPort(),
                info.cacheSummary(),
                inventoryPool.encode(info.capabilityInventory()),
                info.copyTaskTlsPort(), info.workerExecutionIdentity().admissionDomainId(),
                info.workerExecutionIdentity().incarnationId());
    }

    private static HeartbeatSnapshot toHeartbeatSnapshot(HeartbeatData data) {
        return new HeartbeatSnapshot(
                data.pressure().name(),
                data.cpuUtilization(),
                data.runningTaskCount(),
                data.availableMemoryBytes(),
                data.cacheSummary());
    }

    private static WorkerInfo fromSnapshot(WorkerSnapshot snapshot) {
        return new WorkerInfo(
                snapshot.workerId(),
                snapshot.host(),
                snapshot.httpPort(),
                snapshot.transportPort(),
                snapshot.resultServerPort(),
                HealthState.ACTIVE,
                MemoryPressure.valueOf(snapshot.memoryPressure()),
                snapshot.cpuUtilization(),
                snapshot.runningTaskCount(),
                snapshot.availableMemoryBytes(),
                Instant.ofEpochMilli(snapshot.lastHeartbeatMs()),
                0.0,
                null,
                snapshot.protocolVersion(),
                snapshot.rackId(),
                snapshot.dataLocality(),
                snapshot.cacheSummary(),
                CapabilityInventoryCodec.decode(snapshot.capabilityInventory()),
                snapshot.copyTaskTlsPort(),
                new org.pangrova.protocol.execution.identity.WorkerExecutionIdentity(
                        snapshot.admissionDomainId(), snapshot.incarnationId()));
    }

    private HealthState classifyHealth(long elapsedMs) {
        if (elapsedMs > deadThresholdMs) {
            return HealthState.DEAD;
        }
        if (elapsedMs > suspectThresholdMs) {
            return HealthState.SUSPECT;
        }
        return HealthState.ACTIVE;
    }
}
