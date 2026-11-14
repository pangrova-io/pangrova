package org.pangrova.planner.throttle;

import com.google.common.util.concurrent.RateLimiter;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.common.enums.PangrovaErrorCode;

import java.time.Instant;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;

/**
 * Three-layer query throttling: global Semaphore, tenant RateLimiter, user RateLimiter.
 * Acquisition order: global → tenant → user.
 * On rejection: release all previously acquired permits in reverse order.
 */
@ThreadSafe
public final class QueryThrottlingService {

    private final Semaphore globalConcurrency;
    private final ConcurrentMap<String, RateLimiter> tenantLimiters;
    private final ConcurrentMap<String, RateLimiter> userLimiters;
    private final Map<ThrottlePermit, Boolean> issuedPermits =
            Collections.synchronizedMap(new IdentityHashMap<>());
    private volatile double tenantRate;
    private volatile double userRate;

    /**
     * Creates a throttling service with global concurrency and per-tenant/user rate limits.
     *
     * @param globalLimit maximum number of concurrently admitted queries
     * @param tenantRate per-tenant queries per second
     * @param userRate per-user queries per second
     */
    public QueryThrottlingService(int globalLimit, double tenantRate, double userRate) {
        if (globalLimit < 0) throw new IllegalArgumentException("global concurrency limit must not be negative");
        requireRate(tenantRate);
        requireRate(userRate);
        this.globalConcurrency = new Semaphore(globalLimit);
        this.tenantLimiters = new ConcurrentHashMap<>();
        this.userLimiters = new ConcurrentHashMap<>();
        this.tenantRate = tenantRate;
        this.userRate = userRate;
    }

    /**
     * Try to acquire permits at all three layers.
     * On rejection at any layer, the global Semaphore permit is released.
     * Tenant and user layers use Guava {@link RateLimiter} (token bucket);
     * consumed tokens are non-refundable and regenerate over time.
     */
    public ThrottleResult tryAcquire(String tenantId, String userId) {
        Objects.requireNonNull(tenantId, "tenantId");
        Objects.requireNonNull(userId, "userId");
        if (!tryAcquireGlobal()) {
            return ThrottleResult.rejected(
                    PangrovaErrorCode.THROTTLE_GLOBAL_LIMIT.getDescription());
        }
        boolean retained = false;
        try {
            if (!tryAcquireTenant(tenantId)) {
                return ThrottleResult.rejected(
                        PangrovaErrorCode.THROTTLE_TENANT_LIMIT.getDescription());
            }
            if (!tryAcquireUser(userId)) {
                return ThrottleResult.rejected(
                        PangrovaErrorCode.THROTTLE_USER_LIMIT.getDescription());
            }
            ThrottlePermit permit = new ThrottlePermit(userId, tenantId, Instant.now());
            ThrottleResult result = ThrottleResult.success(permit);
            issuedPermits.put(permit, Boolean.TRUE);
            retained = true;
            return result;
        } finally {
            if (!retained) globalConcurrency.release();
        }
    }

    /**
     * Release the global concurrency permit acquired during {@link #tryAcquire}.
     * Only the exact object issued by this service can return a slot, once.
     * Unknown, copied, null and already-returned permits do not change capacity.
     *
     * <p>Design note: Only the global Semaphore permit is released because tenant and user
     * layers use Guava {@link RateLimiter}, which is a rate-based (token bucket) limiter,
     * not a permit-based limiter. {@code RateLimiter.tryAcquire()} consumes a token from
     * the bucket; there is no "release" operation — tokens regenerate over time at the
     * configured rate. Therefore, only the Semaphore-based global layer needs explicit release.</p>
     */
    public void release(ThrottlePermit permit) {
        if (issuedPermits.remove(permit) != null) globalConcurrency.release();
    }

    public int availableGlobalPermits() {
        return globalConcurrency.availablePermits();
    }

    /**
     * Remove stale tenant and user rate limiters that have not been accessed recently.
     * Should be called periodically by a scheduler to prevent unbounded map growth.
     *
     * @param maxEntries maximum number of entries to retain per map
     */
    public void evictStaleLimiters(int maxEntries) {
        evictIfExceeds(tenantLimiters, maxEntries);
        evictIfExceeds(userLimiters, maxEntries);
    }

    private static void evictIfExceeds(ConcurrentMap<String, RateLimiter> map, int maxEntries) {
        if (map.size() > maxEntries) {
            // Simple eviction: remove oldest entries until within limit.
            // RateLimiter has no last-access timestamp, so we just trim by iteration order.
            var iterator = map.entrySet().iterator();
            int toRemove = map.size() - maxEntries;
            while (iterator.hasNext() && toRemove > 0) {
                iterator.next();
                iterator.remove();
                toRemove--;
            }
        }
    }

    /**
     * Adjust tenant and user rate limits for all existing limiters.
     * Used by {@link AdaptiveThrottlingController} for adaptive throttling.
     */
    public void adjustRates(double newTenantRate, double newUserRate) {
        requireRate(newTenantRate);
        requireRate(newUserRate);
        this.tenantRate = newTenantRate;
        this.userRate = newUserRate;
        tenantLimiters.values().forEach(l -> l.setRate(newTenantRate));
        userLimiters.values().forEach(l -> l.setRate(newUserRate));
    }

    private boolean tryAcquireGlobal() {
        return globalConcurrency.tryAcquire();
    }

    private static void requireRate(double rate) {
        if (!Double.isFinite(rate) || rate <= 0) {
            throw new IllegalArgumentException("throttle rate must be finite and positive");
        }
    }

    private boolean tryAcquireTenant(String tenantId) {
        RateLimiter limiter = tenantLimiters.computeIfAbsent(
                tenantId, k -> RateLimiter.create(tenantRate));
        return limiter.tryAcquire();
    }

    private boolean tryAcquireUser(String userId) {
        RateLimiter limiter = userLimiters.computeIfAbsent(
                userId, k -> RateLimiter.create(userRate));
        return limiter.tryAcquire();
    }
}
