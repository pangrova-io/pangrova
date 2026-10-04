package org.pangrova.security.tenant.quota;

import org.pangrova.common.concurrent.ThreadSafe;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Single-process exact query reservations. A short monitor publishes the owner and count together;
 * there is no I/O or callback under this monitor. This is not a shared multi-process authority.
 */
@ThreadSafe
public final class InMemoryClusterConcurrencySlots implements ClusterConcurrencySlots {

    private final Map<String, Integer> active = new HashMap<>();
    private final Map<String, String> queries = new HashMap<>();

    @Override
    public synchronized boolean tryReserve(String tenantId, String queryId, int limit) {
        requireIdentity(tenantId, queryId);
        String existing = queries.get(queryId);
        if (existing != null) {
            return existing.equals(tenantId);
        }
        int current = active.getOrDefault(tenantId, 0);
        if (limit <= 0 || current >= limit) {
            return false;
        }
        queries.put(queryId, tenantId);
        active.put(tenantId, current + 1);
        return true;
    }

    @Override
    public synchronized void release(String tenantId, String queryId) {
        requireIdentity(tenantId, queryId);
        if (queries.remove(queryId, tenantId)) {
            active.compute(tenantId, (tenant, count) -> count == 1 ? null : count - 1);
        }
    }

    @Override
    public synchronized int active(String tenantId) {
        return active.getOrDefault(Objects.requireNonNull(tenantId, "tenantId"), 0);
    }

    static void requireIdentity(String tenantId, String queryId) {
        if (Objects.requireNonNull(tenantId, "tenantId").isBlank()
                || Objects.requireNonNull(queryId, "queryId").isBlank()) {
            throw new IllegalArgumentException("tenant and query identities must not be blank");
        }
    }
}
