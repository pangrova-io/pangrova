package org.pangrova.coordinator.scheduler.placement;

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

    /** Binds a stage's placement scope before any task in its cohort is submitted. */
    default WorkerSelector bindStagePlacement(String tenantId) { return this; }

    /** Selects a Worker with a registered JVM CDC backend. */
    default WorkerInfo selectForCdc(String tenantId) {
        WorkerInfo selected = select(tenantId);
        if (!selected.cdcIngestSupported()) {
            throw new org.pangrova.scheduler.api.exception.NoAvailableWorkerException(
                    "Selected Worker does not advertise a CDC backend");
        }
        return selected;
    }

    /**
     * Returns the number of active workers available for task dispatch.
     * Used to cap intermediate stage parallelism to cluster capacity.
     * Default returns {@link Integer#MAX_VALUE} (no cap) for implementations
     * that do not track worker count.
     */
    default int activeWorkerCount() {
        return Integer.MAX_VALUE;
    }

    /** Rechecks live tenant, operator and exact Worker incarnation at every Local shuffle dispatch. */
    default WorkerInfo requireLocalWorker(String tenantId, WorkerInfo original,
            java.util.List<org.pangrova.protocol.v2.OperatorBinding> bindings) {
        var selected = selectForBindings(tenantId, java.util.List.of(), bindings);
        if (original.workerExecutionIdentity() == null || !original.workerId().equals(selected.workerId())
                || !original.workerExecutionIdentity().equals(selected.workerExecutionIdentity())) {
            throw new org.pangrova.scheduler.api.exception.NoAvailableWorkerException("Local shuffle Worker changed");
        }
        return selected;
    }

    /** Estimates the greatest default Driver grant with one concurrent stage. */
    default int estimateMaximumTaskDrivers(int activeWorkers, int parallelism) { return 0; }

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
        WorkerInfo selected = splitFilePaths == null || splitFilePaths.isEmpty()
                ? select(tenantId)
                : selectWithCacheAffinity(tenantId, splitFilePaths);
        if (bindings != null && !bindings.isEmpty()
                && !org.pangrova.scheduler.placement.WorkerCapabilityMatcher.supports(
                        selected.capabilityInventory(), bindings)) {
            throw new org.pangrova.scheduler.api.exception.NoAvailableWorkerException(
                    "Selected Worker does not satisfy the exact task bindings");
        }
        return selected;
    }

    /**
     * Resolves one SQL model name to a single exact active deployment identity
     * that implements the requested generic capability.
     */
    default org.pangrova.protocol.v2.ModelAvailability resolveExactModel(
            String modelName,
            org.pangrova.protocol.v2.CapabilityRef capability,
            org.pangrova.security.api.identity.UserIdentity user) {
        throw new org.pangrova.scheduler.api.exception.NoAvailableWorkerException(
                "Worker selector cannot resolve exact model deployments");
    }

    /**
     * Selects a Worker excluding the specified worker (e.g., after task failure).
     * Default implementation validates the selected candidate and rejects an excluded Worker.
     */
    default WorkerInfo selectExcluding(String tenantId, String excludeWorkerId) {
        WorkerInfo selected = select(tenantId);
        if (excludeWorkerId != null && excludeWorkerId.equals(selected.workerId())) {
            throw new org.pangrova.scheduler.api.exception.NoAvailableWorkerException(
                    "Selected Worker is explicitly excluded: " + excludeWorkerId);
        }
        return selected;
    }
}
