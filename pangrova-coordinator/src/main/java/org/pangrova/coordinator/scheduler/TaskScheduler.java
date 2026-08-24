package org.pangrova.coordinator.scheduler;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.pangrova.coordinator.worker.HealthState;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.coordinator.worker.WorkerPerformanceScorer;
import org.pangrova.resource.MemoryPressure;
import org.pangrova.resource.QueueType;
import org.pangrova.resource.scheduler.WeightedFairScheduler;
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
    private static final long ONE_GB = 1L * 1024 * 1024 * 1024;

    private final WorkerPerformanceScorer scorer;
    private final WeightedFairScheduler fairScheduler;

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
    public Optional<WorkerInfo> selectWorker(long estimatedMemoryBytes, List<WorkerInfo> candidates) {
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
    public Optional<WorkerInfo> selectWorker(
            long estimatedMemoryBytes, List<WorkerInfo> candidates, String resourcePoolId) {
        if (resourcePoolId == null) {
            return selectFromCandidates(estimatedMemoryBytes, candidates);
        }
        List<WorkerInfo> poolCandidates = candidates.stream()
                .filter(w -> resourcePoolId.equals(w.resourcePoolId()))
                .toList();
        if (poolCandidates.isEmpty()) {
            return selectFromCandidates(estimatedMemoryBytes, candidates);
        }
        if (fairScheduler != null) {
            return selectWithFairShare(estimatedMemoryBytes, poolCandidates, resourcePoolId);
        }
        return selectFromCandidates(estimatedMemoryBytes, poolCandidates);
    }

    /**
     * Selects a worker using weighted fair share scoring. The tenant's queue type
     * share is multiplied with the performance score to bias selection toward
     * workers that serve under-utilized tenant queues.
     */
    private Optional<WorkerInfo> selectWithFairShare(
            long estimatedMemoryBytes, List<WorkerInfo> candidates, String resourcePoolId) {
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
        return candidates.stream()
                .filter(w -> w.healthState() == HealthState.ACTIVE)
                .filter(w -> w.memoryPressure() != MemoryPressure.CRITICAL)
                .filter(w -> isMemoryAcceptable(w, estimatedMemoryBytes))
                .max(Comparator.comparingDouble(w ->
                        scorer.computeScore(toWorkerStats(w)) * effectiveShare));
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
    public Optional<WorkerInfo> selectWorkerWithLocality(
            long estimatedMemoryBytes, List<WorkerInfo> candidates,
            String resourcePoolId, List<String> preferredLocations) {
        List<WorkerInfo> eligible = filterEligible(estimatedMemoryBytes, candidates, resourcePoolId);
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

    private List<WorkerInfo> filterEligible(
            long estimatedMemoryBytes, List<WorkerInfo> candidates, String resourcePoolId) {
        var stream = candidates.stream()
                .filter(w -> w.healthState() == HealthState.ACTIVE)
                .filter(w -> w.memoryPressure() != MemoryPressure.CRITICAL)
                .filter(w -> isMemoryAcceptable(w, estimatedMemoryBytes));
        if (resourcePoolId != null) {
            stream = stream.filter(w -> resourcePoolId.equals(w.resourcePoolId()));
        }
        return stream.toList();
    }

    private Optional<WorkerInfo> tryDataLocal(
            List<WorkerInfo> eligible, List<String> preferredLocations) {
        List<WorkerInfo> local = eligible.stream()
                .filter(w -> hasDataLocality(w, preferredLocations))
                .toList();
        return bestByScore(local);
    }

    private Optional<WorkerInfo> tryRackLocal(
            List<WorkerInfo> eligible, List<String> preferredLocations) {
        String targetRack = extractRackHint(preferredLocations);
        if (targetRack == null) {
            return Optional.empty();
        }
        List<WorkerInfo> rackLocal = eligible.stream()
                .filter(w -> targetRack.equals(w.rackId()))
                .toList();
        return bestByScore(rackLocal);
    }

    private Optional<WorkerInfo> fallbackLoadBalanced(
            List<WorkerInfo> eligible, List<String> preferredLocations) {
        LOG.warn("event=locality_fallback preferredLocations={} reason=no_local_or_rack_worker",
                preferredLocations);
        return bestByScore(eligible);
    }

    private Optional<WorkerInfo> bestByScore(List<WorkerInfo> workers) {
        return workers.stream()
                .max(Comparator.comparingDouble(w -> scorer.computeScore(toWorkerStats(w))));
    }

    private static boolean hasDataLocality(WorkerInfo worker, List<String> preferredLocations) {
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
     * Selects the best active Worker that can accommodate the estimated memory
     * and supports all required capabilities.
     *
     * @param estimatedMemoryBytes estimated memory for the task
     * @param candidates           all candidate workers
     * @param requiredCapabilities capabilities the task requires (e.g. function names)
     * @return best matching worker, or empty if none eligible
     */
    public Optional<WorkerInfo> selectWorkerWithCapabilities(
            long estimatedMemoryBytes, List<WorkerInfo> candidates,
            List<String> requiredCapabilities) {
        if (requiredCapabilities == null || requiredCapabilities.isEmpty()) {
            return selectFromCandidates(estimatedMemoryBytes, candidates);
        }
        List<WorkerInfo> capable = candidates.stream()
                .filter(w -> w.capabilities().containsAll(requiredCapabilities))
                .toList();
        if (capable.isEmpty()) {
            LOG.warn("event=no_capable_workers requiredCapabilities={} candidateCount={}",
                    requiredCapabilities, candidates.size());
            return Optional.empty();
        }
        return selectFromCandidates(estimatedMemoryBytes, capable);
    }

    private Optional<WorkerInfo> selectFromCandidates(
            long estimatedMemoryBytes, List<WorkerInfo> candidates) {
        return candidates.stream()
                .filter(w -> w.healthState() == HealthState.ACTIVE)
                .filter(w -> w.memoryPressure() != MemoryPressure.CRITICAL)
                .filter(w -> isMemoryAcceptable(w, estimatedMemoryBytes))
                .max(Comparator.comparingDouble(w -> scorer.computeScore(toWorkerStats(w))));
    }

    private WorkerPerformanceScorer.WorkerStats toWorkerStats(WorkerInfo worker) {
        return new WorkerPerformanceScorer.WorkerStats(
                worker.memoryPressure().getLowerBound(),
                worker.cpuUtilization(),
                0.0,
                0.0);
    }

    private boolean isMemoryAcceptable(WorkerInfo worker, long estimatedMemoryBytes) {
        // BUG-X05: Primary check — estimated task memory must fit in worker's available memory.
        // Fallback: if estimatedMemoryBytes is 0 (unknown), use pressure-based heuristic.
        if (estimatedMemoryBytes > 0) {
            return estimatedMemoryBytes <= worker.availableMemoryBytes();
        }
        return worker.memoryPressure() != MemoryPressure.HIGH || estimatedMemoryBytes < ONE_GB;
    }

    /**
     * Creates the active {@link org.pangrova.spi.execution.scheduling.WorkerSelectionStrategy}
     * based on whether fair scheduling is enabled.
     *
     * @param candidates the current list of candidate workers
     * @return the strategy to use for worker selection
     */
    public org.pangrova.spi.execution.scheduling.WorkerSelectionStrategy createStrategy(
            List<WorkerInfo> candidates) {
        if (fairScheduler != null) {
            return new FairShareStrategy(scorer, fairScheduler, candidates);
        }
        return new LoadBalancedStrategy(scorer, candidates);
    }
}
