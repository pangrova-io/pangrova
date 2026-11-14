package org.pangrova.coordinator.scheduler.placement;

import java.util.List;
import java.util.Objects;

import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.protocol.v2.OperatorBinding;
import org.pangrova.protocol.v2.StageResourceGrant;
import org.pangrova.scheduler.api.exception.NoAvailableWorkerException;

/** One live DAG's placement. All stages lease data from one exact process until query cleanup. */
public final class LocalShufflePlacement implements WorkerSelector {
    private final WorkerSelector cluster;
    private WorkerInfo original;

    public LocalShufflePlacement(WorkerSelector cluster) {
        this.cluster = Objects.requireNonNull(cluster);
    }

    @Override
    public WorkerInfo select(String tenant) { return selectForBindings(tenant, List.of(), List.of()); }

    @Override
    public WorkerInfo selectWithCacheAffinity(String tenant, List<String> paths) {
        return selectForBindings(tenant, paths, List.of());
    }

    @Override
    public synchronized WorkerInfo selectForBindings(String tenant, List<String> paths, List<OperatorBinding> bindings) {
        if (original != null) return cluster.requireLocalWorker(tenant, original, bindings);
        var selected = cluster.selectForBindings(tenant, paths, bindings);
        if (selected.workerExecutionIdentity() == null || selected.shuffleDeployment() == null
                || !"LOCAL".equals(selected.shuffleDeployment().mode())) {
            throw new NoAvailableWorkerException("Local shuffle requires a Worker incarnation with Local storage");
        }
        original = selected;
        return original;
    }

    @Override
    public int activeWorkerCount() { return 1; }

    @Override
    public StageResourceGrant computeResourceLimit(int parallelism, int concurrentStages, WorkerInfo selected) {
        return cluster.computeResourceLimit(parallelism, concurrentStages, selected);
    }

    @Override
    public org.pangrova.protocol.v2.ModelAvailability resolveExactModel(String model,
            org.pangrova.protocol.v2.CapabilityRef capability, org.pangrova.security.api.identity.UserIdentity user) {
        return cluster.resolveExactModel(model, capability, user);
    }

    @Override
    public WorkerInfo selectExcluding(String tenant, String excluded) {
        throw new NoAvailableWorkerException("Local shuffle cannot relocate or retry the selected producer");
    }
}
