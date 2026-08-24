package org.pangrova.security.tenant.quota;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A-ADMISSION-01: single-JVM {@link ClusterConcurrencySlots} where this coordinator's process is
 * the sole authority (single-node deployments, or when all queries for a tenant are routed to one
 * coordinator). Reservation is a lock-free CAS on a per-tenant counter.
 */
public final class InMemoryClusterConcurrencySlots implements ClusterConcurrencySlots {

    private final ConcurrentHashMap<String, AtomicInteger> active = new ConcurrentHashMap<>();

    @Override
    public boolean tryReserve(String tenantId, int limit) {
        if (limit <= 0) {
            return false;
        }
        AtomicInteger counter = active.computeIfAbsent(tenantId, k -> new AtomicInteger());
        while (true) {
            int current = counter.get();
            if (current >= limit) {
                return false;
            }
            if (counter.compareAndSet(current, current + 1)) {
                return true;
            }
        }
    }

    @Override
    public void release(String tenantId) {
        AtomicInteger counter = active.get(tenantId);
        if (counter != null && counter.decrementAndGet() < 0) {
            counter.set(0);
        }
    }

    @Override
    public int active(String tenantId) {
        AtomicInteger counter = active.get(tenantId);
        return counter != null ? Math.max(0, counter.get()) : 0;
    }
}
