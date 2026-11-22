package org.pangrova.scheduler.placement;

import java.util.List;
import org.pangrova.scheduler.api.placement.WorkerSelectionCandidate;
import org.pangrova.scheduler.api.placement.WorkerSelectionRequest;
import org.pangrova.scheduler.api.placement.WorkerSelectionStrategy;
import org.pangrova.scheduler.api.placement.TenantPlacementResolver;
import java.util.Optional;

import org.pangrova.scheduler.api.enums.HealthState;
import org.pangrova.scheduler.api.placement.PlacementWorker;
import org.pangrova.protocol.v2.ArtifactFormatRef;
import org.pangrova.protocol.v2.CapabilityRef;
import org.pangrova.protocol.v2.OperatorBinding;
import org.pangrova.common.enums.MemoryPressure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Enforces placement eligibility and locality before delegating ranking to a
 * supplied strategy. Core never creates or chooses a Provider implementation.
 */
public class TaskScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(TaskScheduler.class);

    private final WorkerSelectionStrategy strategy;

    private final TenantPlacementResolver tenantPlacement;

    public TaskScheduler(WorkerSelectionStrategy strategy, TenantPlacementResolver tenantPlacement) {
        this.strategy = checkNotNull(strategy, "strategy must not be null");
        this.tenantPlacement = checkNotNull(tenantPlacement, "tenantPlacement must not be null");
    }

    /** Freezes one authoritative tenant placement for a single dispatch cohort. */
    public TaskScheduler bindTenant(String tenantId) {
        var pool = checkNotNull(tenantPlacement.resourcePool(tenantId), "resolved pool must not be null");
        return new TaskScheduler(strategy, requested -> {
            if (!java.util.Objects.equals(tenantId, requested)) {
                throw new IllegalArgumentException("Dispatch cohort cannot change placement tenant");
            }
            return pool;
        });
    }

    /** Tenant identity is resolved by its authority, never interpreted as a pool ID. */
    public <W extends PlacementWorker> List<W> eligibleWorkersForTenant(
            long estimatedMemoryBytes, List<W> candidates, String tenantId) {
        var pool = checkNotNull(tenantPlacement.resourcePool(tenantId), "resolved pool must not be null");
        if (pool.isPresent()) {
            if (pool.orElseThrow().isBlank()) throw new IllegalStateException("Resolved pool must not be blank");
            return eligibleWorkers(estimatedMemoryBytes, candidates, pool.orElseThrow());
        }
        return eligibleWorkers(estimatedMemoryBytes, candidates.stream()
                .filter(worker -> worker.resourcePoolId() == null).toList(), null);
    }

    /** Selects only from the tenant's currently resolved placement scope. */
    public <W extends PlacementWorker> Optional<W> selectWorkerForTenant(
            long estimatedMemoryBytes, List<W> candidates, String tenantId) {
        return selectRanked(eligibleWorkersForTenant(estimatedMemoryBytes, candidates, tenantId));
    }

    /**
     * Selects the best active Worker that can accommodate the estimated memory.
     */
    public <W extends PlacementWorker> Optional<W> selectWorker(
            long estimatedMemoryBytes, List<W> candidates) {
        return selectFromCandidates(estimatedMemoryBytes, candidates);
    }

    /**
     * Selects the best active Worker within a tenant's resource pool.
     * When resourcePoolId is null, all resource pools are eligible.
     */
    public <W extends PlacementWorker> Optional<W> selectWorker(
            long estimatedMemoryBytes, List<W> candidates, String resourcePoolId) {
        List<W> poolCandidates = eligibleWorkers(
                estimatedMemoryBytes, candidates, resourcePoolId);
        if (poolCandidates.isEmpty()) {
            return Optional.empty();
        }
        return selectFromCandidates(estimatedMemoryBytes, poolCandidates);
    }

    /**
     * Returns the exact set eligible for placement before locality is applied.
     * Keeping this predicate in the scheduler prevents cache affinity from
     * bypassing health, memory-pressure, memory-fit, or resource-pool rules.
     */
    public <W extends PlacementWorker> List<W> eligibleWorkers(
            long estimatedMemoryBytes,
            List<W> candidates,
            String resourcePoolId) {
        List<W> scoped = candidates;
        if (resourcePoolId != null) {
            List<W> poolCandidates = candidates.stream()
                    .filter(worker -> resourcePoolId.equals(
                            worker.resourcePoolId()))
                    .toList();
            scoped = poolCandidates;
        }
        return scoped.stream()
                .filter(worker -> worker.healthState() == HealthState.ACTIVE)
                .filter(worker -> worker.memoryPressure()
                        != MemoryPressure.CRITICAL)
                .filter(worker -> isMemoryAcceptable(
                        worker, estimatedMemoryBytes))
                .toList();
    }

    /**
     * Selects the best Worker with data-locality preference and rack-aware fallback.
     *
     * <p>Priority: data-local worker → same-rack worker → load-balanced fallback.</p>
     *
     * @param estimatedMemoryBytes estimated memory for the task
     * @param candidates           all candidate workers
     * @param resourcePoolId       tenant resource pool (nullable)
     * @param preferredLocations   data file locations for locality matching
     * @return best matching worker, or empty if none eligible
     */
    public <W extends PlacementWorker> Optional<W> selectWorkerWithLocality(
            long estimatedMemoryBytes, List<W> candidates,
            String resourcePoolId, List<String> preferredLocations) {
        List<W> eligible = filterEligible(estimatedMemoryBytes, candidates, resourcePoolId);
        if (eligible.isEmpty()) {
            return Optional.empty();
        }
        if (preferredLocations == null || preferredLocations.isEmpty()) {
            return selectRanked(eligible);
        }
        return tryDataLocal(eligible, preferredLocations)
                .or(() -> tryRackLocal(eligible, preferredLocations))
                .or(() -> fallbackLoadBalanced(eligible, preferredLocations));
    }

    private <W extends PlacementWorker> List<W> filterEligible(
            long estimatedMemoryBytes, List<W> candidates, String resourcePoolId) {
        var stream = candidates.stream()
                .filter(w -> w.healthState() == HealthState.ACTIVE)
                .filter(w -> w.memoryPressure() != MemoryPressure.CRITICAL)
                .filter(w -> isMemoryAcceptable(w, estimatedMemoryBytes));
        if (resourcePoolId != null) {
            stream = stream.filter(w -> resourcePoolId.equals(w.resourcePoolId()));
        }
        return stream.toList();
    }

    private <W extends PlacementWorker> Optional<W> tryDataLocal(
            List<W> eligible, List<String> preferredLocations) {
        List<W> local = eligible.stream()
                .filter(w -> hasDataLocality(w, preferredLocations))
                .toList();
        return selectRanked(local);
    }

    private <W extends PlacementWorker> Optional<W> tryRackLocal(
            List<W> eligible, List<String> preferredLocations) {
        String targetRack = extractRackHint(preferredLocations);
        if (targetRack == null) {
            return Optional.empty();
        }
        List<W> rackLocal = eligible.stream()
                .filter(w -> targetRack.equals(w.rackId()))
                .toList();
        return selectRanked(rackLocal);
    }

    private <W extends PlacementWorker> Optional<W> fallbackLoadBalanced(
            List<W> eligible, List<String> preferredLocations) {
        LOG.warn("event=locality_fallback preferredLocations={} reason=no_local_or_rack_worker",
                preferredLocations);
        return selectRanked(eligible);
    }

    private <W extends PlacementWorker> Optional<W> selectRanked(List<W> workers) {
        if (workers.isEmpty()) return Optional.empty();
        var request = new WorkerSelectionRequest(workers.stream()
                .map(worker -> new WorkerSelectionCandidate(worker.workerId(),
                        worker.memoryPressure().getLowerBound(), worker.cpuUtilization()))
                .toList());
        var selected = checkNotNull(strategy.select(request), "strategy result must not be null");
        if (selected.isEmpty()) return Optional.empty();
        return Optional.of(workers.stream()
                .filter(worker -> worker.workerId().equals(selected.orElseThrow()))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        "Strategy selected worker outside the eligible request: " + selected.orElseThrow())));
    }

    private static boolean hasDataLocality(PlacementWorker worker, List<String> preferredLocations) {
        return worker.dataLocality().stream().anyMatch(preferredLocations::contains);
    }

    /**
     * Extracts rack hint from preferred locations using "/rack/" prefix convention.
     * Location format: "/rack/rack-1/data/..." → rack-1.
     */
    private static String extractRackHint(List<String> preferredLocations) {
        for (String loc : preferredLocations) {
            if (loc != null && loc.startsWith("/rack/")) {
                int end = loc.indexOf('/', 6);
                return end > 6 ? loc.substring(6, end) : loc.substring(6);
            }
        }
        return null;
    }

    /**
     * Selects against the complete canonical v2 inventory.
     */
    public <W extends PlacementWorker> Optional<W> selectWorkerWithCapability(
            long estimatedMemoryBytes,
            List<W> candidates,
            CapabilityRef required,
            ArtifactFormatRef requiredFormat,
            java.util.function.Function<? super W, org.pangrova.protocol.v2.CapabilityInventory> inventory) {
        List<W> capable = candidates.stream()
                .filter(worker -> WorkerCapabilityMatcher.supports(
                        inventory.apply(worker), required, requiredFormat))
                .toList();
        if (capable.isEmpty()) {
            LOG.warn("event=no_exact_capable_workers capabilityId={} semanticVersion={} "
                            + "formatId={} candidateCount={}",
                    required.getCapabilityId(), required.getSemanticVersion(),
                    requiredFormat == null ? "" : requiredFormat.getFormatId(),
                    candidates.size());
            return Optional.empty();
        }
        return selectFromCandidates(estimatedMemoryBytes, capable);
    }

    /** Selects a Worker that satisfies every capability, format and exact model binding. */
    public <W extends PlacementWorker> Optional<W> selectWorkerWithBindings(
            long estimatedMemoryBytes,
            List<W> candidates,
            List<OperatorBinding> bindings,
            java.util.function.Function<? super W, org.pangrova.protocol.v2.CapabilityInventory> inventory) {
        if (bindings == null || bindings.isEmpty()) {
            throw new IllegalArgumentException("bindings must not be empty");
        }
        List<W> capable = candidates.stream()
                .filter(worker -> WorkerCapabilityMatcher.supports(inventory.apply(worker), bindings))
                .toList();
        if (capable.isEmpty()) {
            LOG.warn("event=no_exact_capable_workers bindingCount={} candidateCount={}",
                    bindings.size(), candidates.size());
            return Optional.empty();
        }
        return selectFromCandidates(estimatedMemoryBytes, capable);
    }

    private <W extends PlacementWorker> Optional<W> selectFromCandidates(
            long estimatedMemoryBytes, List<W> candidates) {
        List<W> eligible = candidates.stream()
                .filter(w -> w.healthState() == HealthState.ACTIVE)
                .filter(w -> w.memoryPressure() != MemoryPressure.CRITICAL)
                .filter(w -> isMemoryAcceptable(w, estimatedMemoryBytes))
                .toList();
        return selectRanked(eligible);
    }

    private boolean isMemoryAcceptable(PlacementWorker worker, long estimatedMemoryBytes) {
        // BUG-X05: Primary check — estimated task memory must fit in worker's available memory.
        // Fallback: if estimatedMemoryBytes is 0 (unknown), use pressure-based heuristic.
        if (estimatedMemoryBytes > 0) {
            return estimatedMemoryBytes <= worker.availableMemoryBytes();
        }
        return worker.memoryPressure() != MemoryPressure.HIGH;
    }

}
