package org.pangrova.security.core.tenant;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Atomic per-tenant concurrent-query admission counter.
 *
 * <p>Closes the quota TOCTOU (G-SEC-06 / F-SEC-10): the previous admission path read a
 * derived concurrent-query count and then checked it against the limit, so two requests
 * for the same tenant could both observe {@code N < max} and both be admitted, pushing the
 * tenant over its quota. This limiter performs a single atomic compare-and-set
 * reserve-or-reject, making admission race-free regardless of how many requests arrive
 * concurrently. Each successful {@link #tryAcquire} must be balanced by exactly one
 * {@link #release} at query terminal cleanup.
 */
public final class TenantConcurrencyLimiter {

    private final ConcurrentHashMap<String, AtomicInteger> inFlight = new ConcurrentHashMap<>();

    /**
     * Atomically reserves one concurrent-query slot for the tenant if it is below the limit.
     *
     * @param tenantId       the tenant whose slot is being reserved
     * @param maxConcurrent  the tenant's max concurrent queries ({@code <= 0} permits none)
     * @return {@code true} if a slot was reserved (caller must later {@link #release});
     *         {@code false} if the tenant is already at its limit and must be rejected
     */
    public boolean tryAcquire(String tenantId, int maxConcurrent) {
        if (maxConcurrent <= 0) {
            return false;
        }
        AtomicInteger counter = inFlight.computeIfAbsent(tenantId, k -> new AtomicInteger());
        while (true) {
            int current = counter.get();
            if (current >= maxConcurrent) {
                return false;
            }
            if (counter.compareAndSet(current, current + 1)) {
                return true;
            }
        }
    }

    /** Releases one previously reserved slot for the tenant; floors at zero (idempotent-safe). */
    public void release(String tenantId) {
        AtomicInteger counter = inFlight.get(tenantId);
        if (counter == null) {
            return;
        }
        counter.updateAndGet(c -> c > 0 ? c - 1 : 0);
    }

    /** Current number of reserved concurrent-query slots for the tenant (0 if none). */
    public int currentInFlight(String tenantId) {
        AtomicInteger counter = inFlight.get(tenantId);
        return counter == null ? 0 : counter.get();
    }
}
