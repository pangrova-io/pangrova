package org.pangrova.coordinator.query.execution.dispatch.dag;

import java.util.List;

import org.pangrova.coordinator.query.execution.split.CacheAffinityAssigner;
import org.pangrova.coordinator.query.execution.split.SplitAssignmentManager;
import org.pangrova.coordinator.scheduler.NoAvailableWorkerException;
import org.pangrova.coordinator.scheduler.TaskScheduler;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.protocol.rest.config.ResourceLimitPayload;

/**
 * Selects workers for task dispatch using load-based scheduling with optional
 * cache-affinity routing for data-local placement.
 */
public final class DefaultWorkerSelector implements WorkerSelector {

    private static final long MIN_TASK_MEMORY_BYTES = 64L * 1024 * 1024;
    private static final long MIN_WORKER_AVAILABLE_BYTES = 128L * 1024 * 1024;

    private final WorkerManager workerManager;
    private final TaskScheduler taskScheduler;
    private final SplitAssignmentManager splitAssignmentManager;
    private final SpillBudget spillBudget;

    public DefaultWorkerSelector(WorkerManager workerManager,
                                 TaskScheduler taskScheduler,
                                 SplitAssignmentManager splitAssignmentManager,
                                 SpillBudget spillBudget) {
        this.workerManager = workerManager;
        this.taskScheduler = taskScheduler;
        this.splitAssignmentManager = splitAssignmentManager;
        this.spillBudget = spillBudget;
    }

    @Override
    public WorkerInfo select(String tenantId) {
        List<WorkerInfo> candidates = workerManager.getActiveWorkers();
        return taskScheduler.selectWorker(MIN_TASK_MEMORY_BYTES, candidates, tenantId)
                .orElseThrow(NoAvailableWorkerException::new);
    }

    @Override
    public int activeWorkerCount() {
        return Math.max(workerManager.getActiveWorkers().size(), 1);
    }

    @Override
    public ResourceLimitPayload computeResourceLimit(int parallelism, int concurrentStages) {
        List<WorkerInfo> workers = workerManager.getActiveWorkers();
        int workerCount = Math.max(workers.size(), 1);
        int totalParallelism = Math.max(parallelism * concurrentStages, 1);
        long clusterMemory = workers.stream()
                .mapToLong(w -> Math.max(w.availableMemoryBytes(), MIN_WORKER_AVAILABLE_BYTES))
                .sum();
        long perTaskMemory = Math.max(clusterMemory / totalParallelism, MIN_TASK_MEMORY_BYTES);
        int perTaskThreads = (parallelism > 1)
                ? Math.max(2, workerCount * 4 / totalParallelism)
                : 2;
        return new ResourceLimitPayload(perTaskMemory, perTaskThreads,
                spillBudget.maxSpillBytes(), spillBudget.spillDirectory());
    }

    @Override
    public WorkerInfo selectWithCacheAffinity(String tenantId, List<String> splitFilePaths) {
        CacheAffinityAssigner assigner = splitAssignmentManager.cacheAffinityAssigner();
        if (assigner == null || !assigner.hasWorkers() || splitFilePaths.isEmpty()) {
            return select(tenantId);
        }
        List<WorkerInfo> active = workerManager.getActiveWorkers();

        // F-CHAIN-CACHE-02: prefer workers whose CacheCatalogSummary
        // (recently-accessed file paths reported via heartbeat) covers the
        // requested split file paths. Real cache-hit signal, not just hash
        // affinity. Falls back to consistent-hash placement when no worker
        // has the files cached yet (initial load / cold cache).
        WorkerInfo cacheHit = selectByCachedFiles(active, splitFilePaths);
        if (cacheHit != null) {
            return cacheHit;
        }

        List<String> preferred = assigner.preferredWorkers(splitFilePaths, 3);
        for (String preferredId : preferred) {
            for (WorkerInfo w : active) {
                if (w.workerId().equals(preferredId)) {
                    return w;
                }
            }
        }
        return select(tenantId);
    }

    /**
     * F-CHAIN-CACHE-02: Selects the active worker whose
     * {@link org.pangrova.spi.coordination.CacheCatalogSummary#cachedFilePaths()}
     * (reported via heartbeat) covers the most of the requested file paths.
     * Membership is evaluated per-candidate (no constant locality term that
     * cancels out), so the score actually differentiates workers. Returns
     * {@code null} when no worker has any of the requested files cached.
     */
    private static WorkerInfo selectByCachedFiles(List<WorkerInfo> active, List<String> filePaths) {
        WorkerInfo best = null;
        int bestHits = 0;
        for (WorkerInfo w : active) {
            if (w.cacheSummary() == null || w.cacheSummary().isEmpty()) {
                continue;
            }
            int hits = 0;
            for (String path : filePaths) {
                if (w.cacheSummary().mightContain(path)) {
                    hits++;
                }
            }
            if (hits > bestHits) {
                bestHits = hits;
                best = w;
            }
        }
        return best;
    }

    @Override
    public WorkerInfo selectExcluding(String tenantId, String excludeWorkerId) {
        List<WorkerInfo> candidates = workerManager.getActiveWorkers();
        if (candidates.size() > 1 && excludeWorkerId != null) {
            List<WorkerInfo> filtered = candidates.stream()
                    .filter(w -> !w.workerId().equals(excludeWorkerId))
                    .toList();
            if (!filtered.isEmpty()) {
                candidates = filtered;
            }
        }
        return taskScheduler.selectWorker(MIN_TASK_MEMORY_BYTES, candidates, tenantId)
                .orElseThrow(NoAvailableWorkerException::new);
    }
}
