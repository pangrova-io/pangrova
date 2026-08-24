package org.pangrova.coordinator.query.resourcegroup;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.pangrova.spi.resource.admission.AdmissionDecision;
import org.pangrova.spi.resource.admission.AdmissionRequest;
import org.pangrova.spi.resource.admission.ClusterAdmissionController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resource-group-based admission controller.
 *
 * <p>Enforces per-group concurrency limits and queue depth caps. When a query
 * arrives, the controller checks the resource group (from the {@link AdmissionRequest})
 * against configured limits:</p>
 * <ul>
 *   <li>If running queries &lt; maxConcurrency: ADMIT</li>
 *   <li>If running queries &ge; maxConcurrency but queue depth &lt; maxQueueDepth: QUEUE</li>
 *   <li>Otherwise: REJECT</li>
 * </ul>
 *
 * <p>Uses CAS-based atomic counters for concurrency tracking to avoid the TOCTOU
 * race identified in G-RES-04. Memory-aware admission uses the CBO memory estimate
 * from the request when available.</p>
 *
 * <p>Thread-safe: all state is in ConcurrentHashMap + AtomicInteger/AtomicLong.</p>
 */
public final class ResourceGroupAdmissionController implements ClusterAdmissionController {

    private static final Logger LOG = LoggerFactory.getLogger(ResourceGroupAdmissionController.class);

    /** Default config used when a resource group has no explicit configuration. */
    private static final ResourceGroupConfig DEFAULT_CONFIG =
            ResourceGroupConfig.of("default", 100, 1000);

    private final Map<String, ResourceGroupConfig> groupConfigs;
    private final ConcurrentHashMap<String, GroupState> groupStates = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> queryToGroup = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> queryToTenant = new ConcurrentHashMap<>();

    public ResourceGroupAdmissionController(Map<String, ResourceGroupConfig> groupConfigs) {
        this.groupConfigs = Map.copyOf(groupConfigs);
    }

    /** Creates a controller with a single default group. */
    public ResourceGroupAdmissionController(int defaultMaxConcurrency, int defaultMaxQueueDepth) {
        this(Map.of("default", ResourceGroupConfig.of("default", defaultMaxConcurrency, defaultMaxQueueDepth)));
    }

    @Override
    public AdmissionDecision admit(AdmissionRequest request) {
        String groupId = request.resourceGroupId();
        ResourceGroupConfig config = groupConfigs.getOrDefault(groupId, DEFAULT_CONFIG);
        GroupState state = groupStates.computeIfAbsent(groupId, k -> new GroupState());

        // CAS loop for atomic admission
        while (true) {
            int running = state.running.get();
            int queued = state.queued.get();

            if (running < config.maxConcurrency()) {
                AdmissionDecision decision = tryAdmitWithinConcurrencyLimit(config, state, request, groupId, running, queued);
                if (decision != null) {
                    return decision;
                }
                continue; // CAS failed somewhere, retry
            }

            if (queued < config.maxQueueDepth()) {
                if (state.queued.compareAndSet(queued, queued + 1)) {
                    trackQuery(request, groupId);
                    LOG.info("event=query_queued queryId={} group={} running={} queued={}",
                            request.queryId(), groupId, running, queued + 1);
                    return AdmissionDecision.queue("concurrency limit reached: "
                            + running + "/" + config.maxConcurrency());
                }
                continue; // CAS failed, retry
            }

            LOG.warn("event=query_rejected queryId={} group={} running={} queued={}",
                    request.queryId(), groupId, running, queued);
            return AdmissionDecision.reject("resource group '" + groupId
                    + "' at capacity: " + running + " running, " + queued + " queued");
        }
    }

    /**
     * Attempts admission while running is below the concurrency cap; returns null
     * when CAS contention requires the caller to retry the admission loop.
     */
    private AdmissionDecision tryAdmitWithinConcurrencyLimit(ResourceGroupConfig config, GroupState state,
                                                             AdmissionRequest request, String groupId,
                                                             int running, int queued) {
        MemoryCheckResult mem = checkMemoryLimit(config, state, request, queued);
        if (mem.action == MemoryCheckAction.REJECT) {
            return AdmissionDecision.reject("memory limit exceeded and queue full");
        }
        if (mem.action == MemoryCheckAction.QUEUE) {
            trackQuery(request, groupId);
            LOG.info("event=query_queued queryId={} group={} reason=memory_limit "
                            + "current={}B needed={}B limit={}B",
                    request.queryId(), groupId, mem.currentMem, mem.needed,
                    config.maxMemoryBytes());
            return AdmissionDecision.queue("memory limit: " + mem.currentMem
                    + "B + " + mem.needed + "B > " + config.maxMemoryBytes() + "B");
        }
        if (mem.action == MemoryCheckAction.RETRY) {
            return null;
        }
        // PROCEED — memory check passed, try CAS for running slot
        if (state.running.compareAndSet(running, running + 1)) {
            trackQuery(request, groupId);
            LOG.debug("event=query_admitted queryId={} group={} running={}",
                    request.queryId(), groupId, running + 1);
            return AdmissionDecision.admit();
        }
        return null;
    }

    /**
     * Checks whether the memory limit allows admission, and if not, attempts to
     * queue the request via CAS. Extracted from {@link #admit} to keep indent depth ≤ 4.
     */
    private MemoryCheckResult checkMemoryLimit(ResourceGroupConfig config, GroupState state,
                                               AdmissionRequest request, int queued) {
        if (config.maxMemoryBytes() <= 0 || request.estimatedMemoryBytes().isEmpty()) {
            return MemoryCheckResult.PROCEED;
        }
        long currentMem = state.reservedMemoryBytes.get();
        long needed = request.estimatedMemoryBytes().getAsLong();
        if (currentMem + needed <= config.maxMemoryBytes()) {
            state.reservedMemoryBytes.addAndGet(needed);
            return MemoryCheckResult.PROCEED;
        }
        // Over memory limit — queue or reject
        if (queued >= config.maxQueueDepth()) {
            return new MemoryCheckResult(MemoryCheckAction.REJECT, currentMem, needed);
        }
        if (state.queued.compareAndSet(queued, queued + 1)) {
            return new MemoryCheckResult(MemoryCheckAction.QUEUE, currentMem, needed);
        }
        return MemoryCheckResult.RETRY;
    }

    private enum MemoryCheckAction { PROCEED, QUEUE, REJECT, RETRY }

    private record MemoryCheckResult(MemoryCheckAction action, long currentMem, long needed) {
        static final MemoryCheckResult PROCEED = new MemoryCheckResult(MemoryCheckAction.PROCEED, 0, 0);
        static final MemoryCheckResult RETRY = new MemoryCheckResult(MemoryCheckAction.RETRY, 0, 0);
    }

    @Override
    public void release(String queryId, String tenantId) {
        String groupId = queryToGroup.remove(queryId);
        queryToTenant.remove(queryId);
        if (groupId == null) {
            return; // idempotent: query never admitted or already released
        }
        GroupState state = groupStates.get(groupId);
        if (state != null) {
            state.running.updateAndGet(v -> Math.max(0, v - 1));
            LOG.debug("event=query_released queryId={} group={}", queryId, groupId);
        }
    }

    /**
     * Promotes a queued query to running. Call when the query is dequeued.
     */
    public void promoteFromQueue(String queryId) {
        String groupId = queryToGroup.get(queryId);
        if (groupId == null) return;
        GroupState state = groupStates.get(groupId);
        if (state != null) {
            state.queued.updateAndGet(v -> Math.max(0, v - 1));
            state.running.incrementAndGet();
        }
    }

    @Override
    public int admittedCount(String tenantId) {
        return (int) queryToTenant.values().stream()
                .filter(t -> t.equals(tenantId))
                .count();
    }

    /** Returns the current running query count for a resource group. */
    public int runningCount(String groupId) {
        GroupState state = groupStates.get(groupId);
        return state != null ? state.running.get() : 0;
    }

    /** Returns the current queued query count for a resource group. */
    public int queuedCount(String groupId) {
        GroupState state = groupStates.get(groupId);
        return state != null ? state.queued.get() : 0;
    }

    /** Returns the reserved memory in bytes for a resource group. */
    public long reservedMemoryBytes(String groupId) {
        GroupState state = groupStates.get(groupId);
        return state != null ? state.reservedMemoryBytes.get() : 0;
    }

    private void trackQuery(AdmissionRequest request, String groupId) {
        queryToGroup.put(request.queryId(), groupId);
        queryToTenant.put(request.queryId(), request.tenantId());
    }

    /** Per-group mutable state (running count, queued count, reserved memory). */
    private static final class GroupState {
        final AtomicInteger running = new AtomicInteger(0);
        final AtomicInteger queued = new AtomicInteger(0);
        final AtomicLong reservedMemoryBytes = new AtomicLong(0);
    }
}
