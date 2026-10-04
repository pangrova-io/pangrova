package org.pangrova.resource.tenant;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.spi.resource.tenant.TenantUsageSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks per-tenant resource usage and enforces quota limits.
 *
 * <p>Concurrency strategy: all mutable state uses atomic types with CAS loops,
 * ensuring thread-safe tracking without locks.</p>
 */
@ThreadSafe
public class TenantResourceTracker {

    private static final Logger LOG = LoggerFactory.getLogger(TenantResourceTracker.class);

    private final Map<String, TenantResourceQuota> quotas = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> memoryUsage = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> activeQueries = new ConcurrentHashMap<>();

    /**
     * Registers or updates the quota for a tenant.
     */
    public void setQuota(String tenantId, TenantResourceQuota quota) {
        quotas.put(tenantId, quota);
        LOG.info("event=tenant_quota_set tenantId={} maxMemory={} maxQueries={}",
                tenantId, quota.maxMemoryBytes(), quota.maxConcurrentQueries());
    }

    /**
     * Returns the quota for a tenant, defaulting to {@link TenantResourceQuota#UNLIMITED}.
     */
    public TenantResourceQuota getQuota(String tenantId) {
        return quotas.getOrDefault(tenantId, TenantResourceQuota.UNLIMITED);
    }

    /**
     * Checks whether the tenant can start a new query (concurrency limit).
     * If allowed, increments the active query count atomically.
     *
     * @return true if the query was admitted, false if over quota
     */
    public boolean tryAcquireQuery(String tenantId) {
        TenantResourceQuota quota = getQuota(tenantId);
        int limit = quota.hasConcurrencyLimit() ? quota.maxConcurrentQueries() : Integer.MAX_VALUE;
        AtomicInteger counter = activeQueries.computeIfAbsent(tenantId, k -> new AtomicInteger(0));
        while (true) {
            int current = counter.get();
            if (current >= limit) {
                LOG.warn("event=tenant_query_rejected tenantId={} active={} limit={}",
                        tenantId, current, limit);
                return false;
            }
            if (counter.compareAndSet(current, current + 1)) {
                return true;
            }
        }
    }

    /**
     * Releases one query slot for the tenant.
     */
    public void releaseQuery(String tenantId) {
        AtomicInteger counter = activeQueries.get(tenantId);
        if (counter != null) {
            counter.updateAndGet(v -> Math.max(0, v - 1));
        }
    }

    /**
     * Checks whether the tenant can allocate the requested memory.
     * If allowed, tracks the allocation atomically.
     *
     * @return true if memory was admitted, false if over quota
     */
    public boolean tryAllocateMemory(String tenantId, long bytes) {
        if (bytes <= 0) return true;
        TenantResourceQuota quota = getQuota(tenantId);
        long limit = quota.hasMemoryLimit() ? quota.maxMemoryBytes() : Long.MAX_VALUE;
        return tryAllocateWithLimit(tenantId, bytes, limit);
    }

    private boolean tryAllocateWithLimit(String tenantId, long bytes, long limit) {
        AtomicLong usage = memoryUsage.computeIfAbsent(tenantId, k -> new AtomicLong(0));
        while (true) {
            long current = usage.get();
            // Check before addition: wrapping the counter would falsely admit
            // bytes and turn usage snapshots into an apparent zero allocation.
            if (current > limit || bytes > limit - current) {
                LOG.warn("event=tenant_memory_rejected tenantId={} requested={} current={} limit={}",
                        tenantId, bytes, current, limit);
                return false;
            }
            if (usage.compareAndSet(current, current + bytes)) {
                return true;
            }
        }
    }

    /**
     * Releases memory for the tenant.
     */
    public void releaseMemory(String tenantId, long bytes) {
        if (bytes <= 0) {
            return;
        }
        AtomicLong usage = memoryUsage.get(tenantId);
        if (usage != null) {
            usage.updateAndGet(v -> Math.max(0, v - bytes));
        }
    }

    /**
     * Returns current memory usage for a tenant.
     */
    public long getMemoryUsage(String tenantId) {
        AtomicLong usage = memoryUsage.get(tenantId);
        return usage != null ? usage.get() : 0;
    }

    /**
     * Returns current active query count for a tenant.
     */
    public int getActiveQueryCount(String tenantId) {
        AtomicInteger counter = activeQueries.get(tenantId);
        return counter != null ? counter.get() : 0;
    }

    /**
     * Checks whether the given tenant is over its memory quota.
     */
    public boolean isOverMemoryQuota(String tenantId) {
        TenantResourceQuota quota = getQuota(tenantId);
        if (!quota.hasMemoryLimit()) {
            return false;
        }
        return getMemoryUsage(tenantId) > quota.maxMemoryBytes();
    }

    /**
     * Snapshots this node's current per-tenant usage for cross-node reporting.
     *
     * <p>Returns one {@link TenantUsageSnapshot} per tenant that has any tracked
     * memory or active-query state on this node. Workers include this in their
     * heartbeat so the coordinator can aggregate a cluster-wide view and detect
     * aggregate oversell that per-node tracking alone cannot see.</p>
     */
    public Map<String, TenantUsageSnapshot> snapshotUsage() {
        Set<String> tenantIds = new HashSet<>(memoryUsage.keySet());
        tenantIds.addAll(activeQueries.keySet());
        Map<String, TenantUsageSnapshot> snapshot = new HashMap<>(tenantIds.size());
        for (String tenantId : tenantIds) {
            snapshot.put(tenantId, new TenantUsageSnapshot(
                    tenantId, getMemoryUsage(tenantId), getActiveQueryCount(tenantId)));
        }
        return snapshot;
    }
}
