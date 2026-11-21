package org.pangrova.coordinator.scheduler.placement;

import org.pangrova.scheduler.api.resource.SpillBudget;
import java.util.List;
import org.pangrova.scheduler.api.exception.NoAvailableWorkerException;
import org.pangrova.scheduler.placement.ExactModelResolver;
import org.pangrova.ai.control.model.EmbeddingModelExecutionAdmission;
import org.pangrova.ai.control.embedding.EmbeddingModelRegistry;
import org.pangrova.scheduler.placement.TaskScheduler;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.scheduler.placement.WorkerCapabilityMatcher;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.protocol.v2.StageResourceGrant;

/**
 * Selects workers for task dispatch using load-based scheduling with optional
 * cache-affinity routing for data-local placement.
 */
public final class DefaultWorkerSelector implements WorkerSelector {

    private static final long MIN_TASK_MEMORY_BYTES = 64L * 1024 * 1024;
    private static final long MAX_SHUFFLE_BUFFER_BYTES = 32L * 1024 * 1024;
    private static final long MAX_GOVERNED_MEDIA_BYTES_PER_TASK = 64L * 1024 * 1024;

    private final WorkerManager workerManager;
    private final TaskScheduler taskScheduler;
    private final CacheAffinityAssigner cacheAffinityAssigner;
    private final SpillBudget spillBudget;
    private final EmbeddingModelRegistry embeddingModelRegistry;

    /** Creates a selector without an embedding catalog; embedding execution is denied. */
    public DefaultWorkerSelector(WorkerManager workerManager,
                                 TaskScheduler taskScheduler,
                                 CacheAffinityAssigner cacheAffinityAssigner,
                                 SpillBudget spillBudget) {
        this(workerManager, taskScheduler, cacheAffinityAssigner, spillBudget, null);
    }

    /** Creates a selector backed by the control-plane embedding model catalog. */
    public DefaultWorkerSelector(
            WorkerManager workerManager,
            TaskScheduler taskScheduler,
            CacheAffinityAssigner cacheAffinityAssigner,
            SpillBudget spillBudget,
            EmbeddingModelRegistry embeddingModelRegistry) {
        this.workerManager = workerManager;
        this.taskScheduler = taskScheduler;
        this.cacheAffinityAssigner = cacheAffinityAssigner;
        this.spillBudget = spillBudget;
        this.embeddingModelRegistry = embeddingModelRegistry;
    }

    @Override
    public WorkerInfo select(String tenantId) {
        List<WorkerInfo> candidates = workerManager.getActiveWorkers();
        return taskScheduler.selectWorkerForTenant(MIN_TASK_MEMORY_BYTES, candidates, tenantId)
                .orElseThrow(NoAvailableWorkerException::new);
    }

    @Override
    public int activeWorkerCount() {
        return schedulableWorkers().size();
    }

    @Override
    public StageResourceGrant computeResourceLimit(
            int parallelism, int concurrentStages, WorkerInfo selectedWorker) {
        java.util.Objects.requireNonNull(selectedWorker, "selectedWorker must not be null");
        List<WorkerInfo> workers = schedulableWorkers();
        WorkerInfo target = workers.stream()
                .filter(worker -> worker.workerId().equals(selectedWorker.workerId()))
                .findFirst()
                .orElseThrow(() -> new NoAvailableWorkerException(
                        "Selected worker is no longer schedulable: "
                                + selectedWorker.workerId()));
        int workerCount = workers.size();
        int totalParallelism = Math.max(parallelism * concurrentStages, 1);
        long clusterMemory = workers.stream()
                .mapToLong(WorkerInfo::availableMemoryBytes)
                .sum();
        long perTaskMemory = Math.min(target.availableMemoryBytes(),
                Math.max(clusterMemory / totalParallelism, MIN_TASK_MEMORY_BYTES));
        // Split one admitted worker budget between the Velox pool and the
        // serialized producer queue; reserving both at their full size would
        // oversubscribe a worker whose available capacity equals this grant.
        long shuffleBufferBytes = Math.min(perTaskMemory / 4, MAX_SHUFFLE_BUFFER_BYTES);
        int perTaskThreads = (parallelism > 1)
                ? Math.max(2, workerCount * 4 / totalParallelism)
                : 2;
        return StageResourceGrant.newBuilder()
                .setGrantId("unbound-stage-grant")
                .setCpuSlots(perTaskThreads)
                .setNativeMemoryBytes(perTaskMemory - shuffleBufferBytes)
                .setShuffleBufferBytes(shuffleBufferBytes)
                .setSpillBytes(spillBudget.maxSpillBytes())
                .setObjectStoreReadBytes(perTaskMemory)
                .setObjectStoreWriteBytes(spillBudget.maxSpillBytes())
                .setMediaBytes(Math.min(perTaskMemory, MAX_GOVERNED_MEDIA_BYTES_PER_TASK))
                .build();
    }

    @Override
    public WorkerInfo requireLocalWorker(String tenantId, WorkerInfo original,
            List<org.pangrova.protocol.v2.OperatorBinding> bindings) {
        return taskScheduler.eligibleWorkersForTenant(MIN_TASK_MEMORY_BYTES, workerManager.getActiveWorkers(), tenantId)
                .stream().filter(current -> current.workerId().equals(original.workerId())
                        && original.workerExecutionIdentity() != null
                        && original.workerExecutionIdentity().equals(current.workerExecutionIdentity())
                        && WorkerCapabilityMatcher.supports(current.capabilityInventory(), bindings))
                .findFirst().orElseThrow(() -> new NoAvailableWorkerException(
                        "Local shuffle's original Worker incarnation cannot execute this task"));
    }

    private List<WorkerInfo> schedulableWorkers() {
        return taskScheduler.eligibleWorkers(
                MIN_TASK_MEMORY_BYTES, workerManager.getActiveWorkers(), null);
    }

    @Override
    public WorkerInfo selectWithCacheAffinity(String tenantId, List<String> splitFilePaths) {
        CacheAffinityAssigner assigner = cacheAffinityAssigner;
        if (assigner == null || !assigner.hasWorkers() || splitFilePaths.isEmpty()) {
            return select(tenantId);
        }
        List<WorkerInfo> active = taskScheduler.eligibleWorkersForTenant(
                MIN_TASK_MEMORY_BYTES,
                workerManager.getActiveWorkers(),
                tenantId);
        if (active.isEmpty()) {
            throw new NoAvailableWorkerException();
        }

        // F-CHAIN-CACHE-02: prefer workers whose CacheCatalogSummary
        // (recently-accessed file paths reported via heartbeat) covers the
        // requested split file paths. Real cache-hit signal, not just hash
        // affinity. Falls back to consistent-hash placement when no worker
        // has the files cached yet (initial load / cold cache).
        WorkerInfo cacheHit = selectByCachedFiles(active, splitFilePaths);
        if (cacheHit != null) {
            return cacheHit;
        }

        WorkerInfo preferred = findPreferred(
                active, assigner.preferredWorkers(splitFilePaths, 3));
        if (preferred != null) {
            return preferred;
        }
        return select(tenantId);
    }

    private static WorkerInfo findPreferred(
            List<WorkerInfo> active, List<String> preferredWorkerIds) {
        for (String preferredId : preferredWorkerIds) {
            for (WorkerInfo worker : active) {
                if (worker.workerId().equals(preferredId)) {
                    return worker;
                }
            }
        }
        return null;
    }

    @Override
    public WorkerInfo selectForBindings(
            String tenantId,
            List<String> splitFilePaths,
            List<org.pangrova.protocol.v2.OperatorBinding> bindings) {
        if (bindings == null || bindings.isEmpty()) {
            return selectWithCacheAffinity(
                    tenantId, splitFilePaths == null ? List.of() : splitFilePaths);
        }
        List<WorkerInfo> capable = taskScheduler.eligibleWorkersForTenant(
                        MIN_TASK_MEMORY_BYTES,
                        workerManager.getActiveWorkers(),
                        tenantId).stream()
                .filter(worker -> WorkerCapabilityMatcher.supports(worker.capabilityInventory(), bindings))
                .toList();
        if (capable.isEmpty()) {
            throw new NoAvailableWorkerException(
                    "No active worker provides every exact typed capability, "
                            + "artifact format and model required by the task");
        }
        List<String> paths = splitFilePaths == null ? List.of() : splitFilePaths;
        WorkerInfo cacheHit = selectByCachedFiles(capable, paths);
        if (cacheHit != null) {
            return cacheHit;
        }
        return taskScheduler.selectWorker(MIN_TASK_MEMORY_BYTES, capable)
                .orElseThrow(() -> new NoAvailableWorkerException(
                        "Exact-capability workers have insufficient available resources"));
    }

    @Override
    public org.pangrova.protocol.v2.ModelAvailability resolveExactModel(
            String modelName,
            org.pangrova.protocol.v2.CapabilityRef capability,
            org.pangrova.security.api.identity.UserIdentity user) {
        org.pangrova.ai.api.embedding.EmbeddingModelDescriptor descriptor = null;
        if (EmbeddingModelExecutionAdmission.isEmbeddingCapability(capability)) {
            if (embeddingModelRegistry == null) {
                throw new NoAvailableWorkerException(
                        "Embedding model catalog is unavailable; execution is denied");
            }
            descriptor = embeddingModelRegistry.find(modelName, user)
                    .orElseThrow(() -> new NoAvailableWorkerException(
                            "Embedding model is not registered in the catalog: " + modelName));
        }
        org.pangrova.protocol.v2.ModelAvailability availability = ExactModelResolver.resolve(
                workerManager.getActiveWorkers().stream()
                        .map(org.pangrova.coordinator.worker.WorkerInfo::capabilityInventory).toList(),
                                modelName, capability);
        if (descriptor != null) {
            EmbeddingModelExecutionAdmission.requireCompatible(descriptor, availability);
        }
        return availability;
    }

    /**
     * F-CHAIN-CACHE-02: Selects the active worker whose
     * {@link org.pangrova.coordination.api.heartbeat.CacheCatalogSummary#cachedFilePaths()}
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
        if (excludeWorkerId != null) {
            candidates = candidates.stream()
                    .filter(w -> !w.workerId().equals(excludeWorkerId))
                    .toList();
        }
        return taskScheduler.selectWorkerForTenant(MIN_TASK_MEMORY_BYTES, candidates, tenantId)
                .orElseThrow(NoAvailableWorkerException::new);
    }
}
