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
    private final LocalOwner owner;

    public LocalShufflePlacement(WorkerSelector cluster) {
        this(cluster, new LocalOwner());
    }

    private LocalShufflePlacement(WorkerSelector cluster, LocalOwner owner) {
        this.cluster = Objects.requireNonNull(cluster);
        this.owner = owner;
    }

    @Override
    public WorkerSelector bindStagePlacement(String tenantId) {
        return new LocalShufflePlacement(cluster.bindStagePlacement(tenantId), owner);
    }

    @Override
    public WorkerInfo select(String tenant) { return selectForBindings(tenant, List.of(), List.of()); }

    @Override
    public WorkerInfo selectWithCacheAffinity(String tenant, List<String> paths) {
        return selectForBindings(tenant, paths, List.of());
    }

    @Override
    public WorkerInfo selectForBindings(String tenant, List<String> paths,
            List<OperatorBinding> bindings) {
        synchronized (owner) {
            if (owner.original != null) return cluster.requireLocalWorker(tenant, owner.original, bindings);
            var selected = cluster.selectForBindings(tenant, paths, bindings);
            if (selected.workerExecutionIdentity() == null || selected.shuffleDeployment() == null
                    || !"LOCAL".equals(selected.shuffleDeployment().mode())) {
                throw new NoAvailableWorkerException("Local shuffle requires a Worker incarnation with Local storage");
            }
            owner.original = selected;
            return selected;
        }
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

    private static final class LocalOwner {
        private WorkerInfo original;
    }
}
