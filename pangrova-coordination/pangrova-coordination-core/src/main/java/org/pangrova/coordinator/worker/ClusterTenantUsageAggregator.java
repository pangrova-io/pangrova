/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.coordinator.worker;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.scheduler.api.quota.GlobalTenantUsageView;
import org.pangrova.protocol.rest.worker.TenantUsageSnapshot;

/**
 * Aggregates per-tenant resource usage reported by every worker heartbeat into a
 * cluster-wide {@link GlobalTenantUsageView}.
 *
 * <p>Each worker reports its local per-tenant usage on every heartbeat; this
 * aggregator keeps the latest report per worker and sums across workers on read.
 * When a worker is removed (dead/decommissioned/deregistered) its contribution is
 * dropped so aggregates converge to the live cluster state.</p>
 *
 * <p>This resolves the P1-5 gap: {@code TenantResourceTracker} is per-node only,
 * so the same tenant could independently reach its limit on every worker and
 * oversell the cluster. This aggregator provides the missing global view.</p>
 *
 * <p>Concurrency: latest-report-per-worker is held in a {@link ConcurrentHashMap};
 * reads recompute the sum from immutable per-worker snapshots, so readers never
 * observe a partially-updated worker report.</p>
 */
@ThreadSafe
public final class ClusterTenantUsageAggregator implements GlobalTenantUsageView {

    /** workerId -> (tenantId -> latest reported usage on that worker). */
    private final ConcurrentMap<String, Map<String, TenantUsageSnapshot>> perWorkerUsage =
            new ConcurrentHashMap<>();

    /**
     * Records the latest per-tenant usage reported by a worker, replacing any
     * previous report. An empty/null report removes the worker's contribution.
     */
    public void updateWorkerUsage(String workerId, Map<String, TenantUsageSnapshot> usage) {
        if (workerId == null) {
            return;
        }
        if (usage == null || usage.isEmpty()) {
            perWorkerUsage.remove(workerId);
            return;
        }
        perWorkerUsage.put(workerId, Map.copyOf(usage));
    }

    /**
     * Drops a worker's contribution when it leaves the cluster.
     */
    public void removeWorker(String workerId) {
        if (workerId != null) {
            perWorkerUsage.remove(workerId);
        }
    }

    @Override
    public long aggregatedMemoryBytes(String tenantId) {
        long total = 0;
        for (Map<String, TenantUsageSnapshot> workerUsage : perWorkerUsage.values()) {
            TenantUsageSnapshot snapshot = workerUsage.get(tenantId);
            if (snapshot != null) {
                total += snapshot.memoryBytes();
            }
        }
        return total;
    }

    @Override
    public int aggregatedActiveQueries(String tenantId) {
        long total = 0;
        for (Map<String, TenantUsageSnapshot> workerUsage : perWorkerUsage.values()) {
            TenantUsageSnapshot snapshot = workerUsage.get(tenantId);
            if (snapshot != null) {
                total += snapshot.activeQueries();
            }
        }
        return (int) Math.min(total, Integer.MAX_VALUE);
    }

    @Override
    public Set<String> trackedTenants() {
        Set<String> tenants = new HashSet<>();
        for (Map<String, TenantUsageSnapshot> workerUsage : perWorkerUsage.values()) {
            tenants.addAll(workerUsage.keySet());
        }
        return Set.copyOf(tenants);
    }

    /**
     * Returns the number of workers currently contributing usage reports.
     */
    public int reportingWorkerCount() {
        return perWorkerUsage.size();
    }
}
