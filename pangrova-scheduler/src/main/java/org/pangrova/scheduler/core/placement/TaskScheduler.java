package org.pangrova.scheduler.core.placement;

import org.pangrova.scheduler.core.scoring.WorkerPerformanceScorer;

import java.util.Comparator;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.ToDoubleFunction;

import org.pangrova.scheduler.api.placement.HealthState;
import org.pangrova.scheduler.api.placement.PlacementWorker;
import org.pangrova.protocol.v2.ArtifactFormatRef;
import org.pangrova.protocol.v2.CapabilityRef;
import org.pangrova.protocol.v2.OperatorBinding;
import org.pangrova.common.value.MemoryPressure;
import org.pangrova.scheduler.resource.QueueType;
import org.pangrova.scheduler.core.resource.WeightedFairScheduler;
import org.pangrova.scheduler.core.strategy.fair.FairShareStrategy;
import org.pangrova.scheduler.core.strategy.loadbalanced.LoadBalancedStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Selects the best Worker for a task based on memory pressure and performance score.
 * Supports data-locality-first and rack-aware fallback scheduling.
 *
 * <p>When a {@link WeightedFairScheduler} is provided (tenant module enabled),
 * worker selection incorporates tenant queue weight shares to ensure fair
 * resource distribution across tenants.</p>
 */
public class TaskScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(TaskScheduler.class);

    private final WorkerPerformanceScorer scorer;
    private final WeightedFairScheduler fairScheduler;
    private final AtomicLong equalScoreCursor = new AtomicLong();

    public TaskScheduler(WorkerPerformanceScorer scorer) {
        this(scorer, null);
    }

    /**
     * Creates a scheduler with optional weighted fair scheduling for tenant isolation.
     *
     * @param scorer        performance scorer for worker ranking
     * @param fairScheduler weighted fair scheduler (nullable; when non-null, tenant
     *                      queue weights influence worker selection)
     */
    public TaskScheduler(WorkerPerformanceScorer scorer, WeightedFairScheduler fairScheduler) {
        this.scorer = checkNotNull(scorer, "scorer must not be null");
        this.fairScheduler = fairScheduler;
        if (fairScheduler != null) {
            LOG.info("event=fair_scheduler_enabled component=TaskScheduler");
        }
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
     * When resourcePoolId is null, falls back to default selection.
     *
     * <p>When a {@link WeightedFairScheduler} is active, the tenant's queue type
     * weight share is factored into the scoring to prevent a single tenant from
     * monopolizing worker resources.</p>
     */
    public <W extends PlacementWorker> Optional<W> selectWorker(
            long estimatedMemoryBytes, List<W> candidates, String resourcePoolId) {
        List<W> poolCandidates = eligibleWorkers(
                estimatedMemoryBytes, candidates, resourcePoolId);
        if (poolCandidates.isEmpty()) {
            return Optional.empty();
        }
        if (fairScheduler != null && resourcePoolId != null) {
            return selectWithFairShare(estimatedMemoryBytes, poolCandidates, resourcePoolId);
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
            if (!poolCandidates.isEmpty()) {
                scoped = poolCandidates;
            }
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
     * Selects a worker using weighted fair share scoring. The tenant's queue type
     * share is multiplied with the performance score to bias selection toward
     * workers that serve under-utilized tenant queues.
     */
    private <W extends PlacementWorker> Optional<W> selectWithFairShare(
            long estimatedMemoryBytes, List<W> candidates, String resourcePoolId) {
        QueueType queueType = resolveQueueType(resourcePoolId);
        Set<QueueType> activeQueues = fairScheduler.getActiveQueues();
        // If no active queues, use all types for share computation
        if (activeQueues.isEmpty()) {
            activeQueues = Set.of(QueueType.values());
        }
        double share = fairScheduler.computeShare(queueType, activeQueues);
        LOG.debug("event=fair_share_selection resourcePoolId={} queueType={} share={}",
                resourcePoolId, queueType, share);

        final double effectiveShare = Math.max(share, 0.1); // floor to prevent starvation
        List<W> eligible = candidates.stream()
                .filter(w -> w.healthState() == HealthState.ACTIVE)
                .filter(w -> w.memoryPressure() != MemoryPressure.CRITICAL)
                .filter(w -> isMemoryAcceptable(w, estimatedMemoryBytes))
                .toList();
        return bestByScore(eligible, worker ->
                scorer.computeScore(toWorkerStats(worker)) * effectiveShare);
    }

    /**
     * Maps a resource pool ID to a queue type for fair scheduling.
     * Convention: pool IDs starting with "interactive" map to INTERACTIVE,
     * "background" to BACKGROUND, all others to BATCH.
     */
    private static QueueType resolveQueueType(String resourcePoolId) {
        if (resourcePoolId.startsWith("interactive")) {
            return QueueType.INTERACTIVE;
        }
        if (resourcePoolId.startsWith("background")) {
            return QueueType.BACKGROUND;
        }
        return QueueType.BATCH;
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
            return bestByScore(eligible);
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
        return bestByScore(local);
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
        return bestByScore(rackLocal);
    }

    private <W extends PlacementWorker> Optional<W> fallbackLoadBalanced(
            List<W> eligible, List<String> preferredLocations) {
        LOG.warn("event=locality_fallback preferredLocations={} reason=no_local_or_rack_worker",
                preferredLocations);
        return bestByScore(eligible);
    }

    private <W extends PlacementWorker> Optional<W> bestByScore(List<W> workers) {
        return bestByScore(workers, worker -> scorer.computeScore(toWorkerStats(worker)));
    }

    private <W extends PlacementWorker> Optional<W> bestByScore(
            List<W> workers, ToDoubleFunction<? super W> score) {
        double highestScore = Double.NEGATIVE_INFINITY;
        List<W> tied = new ArrayList<>();
        for (W worker : workers) {
            double value = score.applyAsDouble(worker);
            int comparison = Double.compare(value, highestScore);
            if (comparison > 0) {
                highestScore = value;
                tied.clear();
                tied.add(worker);
            } else if (comparison == 0) {
                tied.add(worker);
            }
        }
        if (tied.size() <= 1) {
            return tied.stream().findFirst();
        }
        tied.sort(Comparator.comparing(PlacementWorker::workerId));
        int index = (int) Math.floorMod(equalScoreCursor.getAndIncrement(), tied.size());
        return Optional.of(tied.get(index));
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
            ArtifactFormatRef requiredFormat) {
        List<W> capable = candidates.stream()
                .filter(worker -> WorkerCapabilityMatcher.supports(
                        worker, required, requiredFormat))
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
            List<OperatorBinding> bindings) {
        if (bindings == null || bindings.isEmpty()) {
            throw new IllegalArgumentException("bindings must not be empty");
        }
        List<W> capable = candidates.stream()
                .filter(worker -> WorkerCapabilityMatcher.supports(worker, bindings))
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
        return bestByScore(eligible);
    }

    private WorkerPerformanceScorer.WorkerStats toWorkerStats(PlacementWorker worker) {
        return new WorkerPerformanceScorer.WorkerStats(
                worker.memoryPressure().getLowerBound(),
                worker.cpuUtilization(),
                0.0,
                0.0);
    }

    private boolean isMemoryAcceptable(PlacementWorker worker, long estimatedMemoryBytes) {
        // BUG-X05: Primary check — estimated task memory must fit in worker's available memory.
        // Fallback: if estimatedMemoryBytes is 0 (unknown), use pressure-based heuristic.
        if (estimatedMemoryBytes > 0) {
            return estimatedMemoryBytes <= worker.availableMemoryBytes();
        }
        return worker.memoryPressure() != MemoryPressure.HIGH;
    }

    /**
     * Creates the active {@link org.pangrova.scheduler.api.placement.WorkerSelectionStrategy}
     * based on whether fair scheduling is enabled.
     *
     * @param candidates the current list of candidate workers
     * @return the strategy to use for worker selection
     */
    public org.pangrova.scheduler.api.placement.WorkerSelectionStrategy createStrategy(
            List<? extends PlacementWorker> candidates) {
        if (fairScheduler != null) {
            return new FairShareStrategy(scorer, fairScheduler, candidates);
        }
        return new LoadBalancedStrategy(scorer, candidates);
    }
}
