package org.pangrova.resilience.idempotency;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import org.pangrova.spi.annotation.ThreadSafe;

/**
 * TTL-based idempotency cache providing at-least-once execution semantics.
 *
 * <p>When {@link #executeIdempotent(String, Supplier)} is called with a key
 * that has no cached (or expired) entry, the supplier is executed and the
 * result is cached. Subsequent calls with the same key within the TTL window
 * return the cached result without re-executing the supplier.</p>
 *
 * <h3>At-least-once semantics</h3>
 * <p>The check-then-act in {@code executeIdempotent} is <em>not</em> strictly
 * atomic: concurrent calls with the same key may both observe a cache miss
 * and execute the supplier. Therefore the supplier may run more than once
 * for a given key (at-least-once), but never zero times. Callers must ensure
 * the supplier is safe to execute multiple times (i.e. idempotent).</p>
 *
 * <h3>Generic type constraint</h3>
 * <p>The cache stores entries as {@code CachedEntry<?>}. The unchecked cast
 * in {@code executeIdempotent} is safe <em>only</em> when the same key is
 * always associated with the same return type {@code T}. Mixing different
 * types under the same key will cause a {@link ClassCastException} at the
 * call site.</p>
 *
 * <p>Concurrency strategy: {@link java.util.concurrent.ConcurrentHashMap}
 * for cache entries; no external synchronization required.</p>
 */
@ThreadSafe
public class IdempotencyManager {

    private static final Logger LOG = LoggerFactory.getLogger(IdempotencyManager.class);
    private static final Duration DEFAULT_TTL = Duration.ofHours(24);

    /**
     * Type-safe cache entry wrapping a generic result.
     * Only successful executions are cached. Failed executions are not cached
     * so that transient errors (network timeouts, temporary unavailability)
     * can be retried on the next call.
     */
    private record CachedEntry<T>(T result, Instant expiry) {}

    private final ConcurrentHashMap<String, CachedEntry<?>> cache = new ConcurrentHashMap<>();
    private final Duration ttl;
    private final Clock clock;

    public IdempotencyManager() {
        this(DEFAULT_TTL, Clock.systemUTC());
    }

    public IdempotencyManager(Duration ttl, Clock clock) {
        this.ttl = ttl;
        this.clock = clock;
    }

    /**
     * Executes the supplier once per key within the TTL window, returning the cached result on repeat calls.
     */
    @SuppressWarnings("unchecked")
    public <T> T executeIdempotent(String key, Supplier<T> supplier) {
        CachedEntry<?> cached = cache.get(key);
        if (cached != null && !isExpired(cached)) {
            LOG.debug("Idempotent cache hit for key {}", key);
            return (T) cached.result();
        }
        return executeAndCache(key, supplier);
    }

    public void cleanup() {
        Instant now = clock.instant();
        cache.entrySet().removeIf(entry -> now.isAfter(entry.getValue().expiry()));
    }

    private <T> T executeAndCache(String key, Supplier<T> supplier) {
        T result = supplier.get();
        cache.put(key, new CachedEntry<>(result, clock.instant().plus(ttl)));
        return result;
    }

    private boolean isExpired(CachedEntry<?> cached) {
        return clock.instant().isAfter(cached.expiry());
    }
}
