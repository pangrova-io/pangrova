package org.pangrova.coordinator.query.execution.dispatch.dag;

import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.protocol.v2.StageResourceGrant;

/**
 * Strategy for selecting a Worker and computing resource limits for task dispatch.
 * Extracted from DAGDispatchEngine to allow cross-package usage without
 * depending on the engine class.
 */
public interface WorkerSelector {

    int MAX_TASKS_PER_WORKER = 4;

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

    /** Computes a task grant that fits the exact selected Worker. */
    StageResourceGrant computeResourceLimit(
            int parallelism, int concurrentStages, WorkerInfo selectedWorker);

    /**
     * Selects a Worker with cache affinity preference for the given split files.
     * Falls back to load-based selection when no worker has cache hits.
     * Default implementation delegates to {@link #select(String)}.
     */
    default WorkerInfo selectWithCacheAffinity(String tenantId, java.util.List<String> splitFilePaths) {
        return select(tenantId);
    }

    /**
     * Selects only from Workers that advertise every exact capability, artifact
     * format and model object in the task bindings.
     */
    default WorkerInfo selectForBindings(
            String tenantId,
            java.util.List<String> splitFilePaths,
            java.util.List<org.pangrova.protocol.v2.OperatorBinding> bindings) {
        return splitFilePaths == null || splitFilePaths.isEmpty()
                ? select(tenantId)
                : selectWithCacheAffinity(tenantId, splitFilePaths);
    }

    /**
     * Resolves one SQL model name to a single exact active deployment identity
     * that implements the requested generic capability.
     */
    default org.pangrova.protocol.v2.ModelAvailability resolveExactModel(
            String modelName,
            org.pangrova.protocol.v2.CapabilityRef capability,
            org.pangrova.spi.security.UserIdentity user) {
        throw new org.pangrova.scheduler.core.failure.NoAvailableWorkerException(
                "Worker selector cannot resolve exact model deployments");
    }

    /**
     * Selects a Worker excluding the specified worker (e.g., after task failure).
     * Default implementation delegates to {@link #select(String)}.
     */
    default WorkerInfo selectExcluding(String tenantId, String excludeWorkerId) {
        return select(tenantId);
    }
}
