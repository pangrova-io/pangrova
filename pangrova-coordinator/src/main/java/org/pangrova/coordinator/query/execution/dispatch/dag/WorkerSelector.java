package org.pangrova.coordinator.query.execution.dispatch.dag;

import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.protocol.rest.config.ResourceLimitPayload;

/**
 * Strategy for selecting a Worker and computing resource limits for task dispatch.
 * Extracted from DAGDispatchEngine to allow cross-package usage without
 * depending on the engine class.
 */
public interface WorkerSelector {

    /** Selects a Worker for the given tenant. */
    WorkerInfo select(String tenantId);

    /**
     * Returns the number of active workers available for task dispatch.
     * Used to cap intermediate stage parallelism to cluster capacity.
     * Default returns {@link Integer#MAX_VALUE} (no cap) for implementations
     * that do not track worker count.
     */
    default int activeWorkerCount() {
        return Integer.MAX_VALUE;
    }

    /** Computes per-task resource limits based on parallelism and concurrent stages. */
    ResourceLimitPayload computeResourceLimit(int parallelism, int concurrentStages);

    /**
     * Selects a Worker with cache affinity preference for the given split files.
     * Falls back to load-based selection when no worker has cache hits.
     * Default implementation delegates to {@link #select(String)}.
     */
    default WorkerInfo selectWithCacheAffinity(String tenantId, java.util.List<String> splitFilePaths) {
        return select(tenantId);
    }

    /**
     * Selects a Worker excluding the specified worker (e.g., after task failure).
     * Default implementation delegates to {@link #select(String)}.
     */
    default WorkerInfo selectExcluding(String tenantId, String excludeWorkerId) {
        return select(tenantId);
    }
}
