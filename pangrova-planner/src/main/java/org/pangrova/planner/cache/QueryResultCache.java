package org.pangrova.planner.cache;

import org.pangrova.common.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

/**
 * Two-layer query result cache: L1 (JVM heap) + L2 (external store).
 * Supports SQL hint-based bypass and force-refresh.
 * Exposes hit/miss counters for operational monitoring.
 *
 * <p>Cache keys include tenant/user/session/policy dimensions to prevent
 * cross-tenant result leakage (Requirements 2.11, 2.12).</p>
 *
 * <p>L1 entries have a bounded TTL ({@link #DEFAULT_L1_TTL_MS}) to prevent
 * indefinitely stale results when external writers modify tables without
 * triggering Pangrova's invalidation path (F-PLANNER-02).</p>
 *
 * <p>Invalidation uses metadata-indexed lookup (by table set, by user, by policy version)
 * instead of fragile {@code key.contains(...)} string matching (Requirements 2.31, 2.32).</p>
 *
 * <p>Thread safety: PERF-T12 — relies solely on ConcurrentHashMap's native
 * lock-free reads and segmented writes. ReadWriteLock removed to eliminate
 * contention between concurrent gets and puts.</p>
 */
@ThreadSafe
public class QueryResultCache implements AutoCloseable {

    /** Default maximum L1 cache entries to prevent unbounded memory growth. */
    private static final int DEFAULT_MAX_L1_ENTRIES = 10_000;

    /** Default L1 TTL in milliseconds (5 minutes). Prevents indefinitely stale results
     *  from external writes that bypass Pangrova's invalidation (F-PLANNER-02). */
    private static final long DEFAULT_L1_TTL_MS = 5 * 60 * 1000L;

    private final ConcurrentHashMap<CacheKey, CachedResult> l1 = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<CacheKey, CacheEntryMetadata> metadataIndex = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<CacheKey, Long> l1PutTime = new ConcurrentHashMap<>();
    private final L2CacheStore l2;
    private final java.util.concurrent.atomic.AtomicBoolean closed =
            new java.util.concurrent.atomic.AtomicBoolean();
    private final int maxL1Entries;
    private final long l1TtlMs;
    private final AtomicLong hitCount = new AtomicLong();
    private final AtomicLong missCount = new AtomicLong();
    private final AtomicLong evictionCount = new AtomicLong();

    public QueryResultCache(L2CacheStore l2) {
        this(l2, DEFAULT_MAX_L1_ENTRIES, DEFAULT_L1_TTL_MS);
    }

    public QueryResultCache(L2CacheStore l2, int maxL1Entries) {
        this(l2, maxL1Entries, DEFAULT_L1_TTL_MS);
    }

    public QueryResultCache(L2CacheStore l2, int maxL1Entries, long l1TtlMs) {
        this.l2 = l2;
        this.maxL1Entries = maxL1Entries > 0 ? maxL1Entries : DEFAULT_MAX_L1_ENTRIES;
        this.l1TtlMs = l1TtlMs > 0 ? l1TtlMs : DEFAULT_L1_TTL_MS;
    }

    /** Releases the process-owned external store exactly once. */
    @Override
    public void close() throws Exception {
        if (closed.compareAndSet(false, true)) {
            l1.clear();
            metadataIndex.clear();
            l1PutTime.clear();
            if (l2 instanceof AutoCloseable resource) {
                resource.close();
            }
        }
    }

    /**
     * Get cached result with hint handling.
     * NO_CACHE → empty; REFRESH_CACHE → empty (caller re-executes then puts);
     * NONE → L1 hit → return; L1 miss → L2 hit → promote to L1 → return.
     */
    public Optional<CachedResult> get(CacheKey key, CacheHint hint) {
        if (hint == CacheHint.NO_CACHE || hint == CacheHint.REFRESH_CACHE) {
            missCount.incrementAndGet();
            return Optional.empty();
        }
        // PERF-T12: ConcurrentHashMap.get() is lock-free; no read lock needed.
        CachedResult l1Hit = l1.get(key);
        if (l1Hit != null) {
            // F-PLANNER-02: Check TTL to prevent indefinitely stale results
            Long putTime = l1PutTime.get(key);
            if (putTime != null && (System.currentTimeMillis() - putTime) > l1TtlMs) {
                // Entry expired — treat as miss; lazy eviction
                missCount.incrementAndGet();
                return Optional.empty();
            }
            hitCount.incrementAndGet();
            return Optional.of(l1Hit);
        }
        return queryL2AndPromote(key);
    }

    /**
     * Query L2 cache and promote to L1 on hit.
     * Updates hit/miss counters accordingly.
     */
    private Optional<CachedResult> queryL2AndPromote(CacheKey key) {
        Optional<CachedResult> l2Hit = l2.get(key.keyString());
        if (l2Hit.isPresent()) {
            hitCount.incrementAndGet();
            // PERF-T12: ConcurrentHashMap.put() is thread-safe without write lock.
            if (l1.size() >= maxL1Entries && !l1.containsKey(key)) {
                evictOldestEntry();
            }
            l2Hit.ifPresent(result -> l1.put(key, result));
        } else {
            missCount.incrementAndGet();
        }
        return l2Hit;
    }

    /**
     * Write to both L1 and L2 with associated metadata for precise invalidation.
     */
    public void put(CacheKey key, CachedResult result, CacheEntryMetadata metadata) {
        // PERF-T12: No write lock needed; ConcurrentHashMap handles concurrent puts.
        if (l1.size() >= maxL1Entries && !l1.containsKey(key)) {
            evictOldestEntry();
        }
        l1.put(key, result);
        l1PutTime.put(key, System.currentTimeMillis());
        metadataIndex.put(key, metadata);
        l2.put(key.keyString(), result);
    }

    /**
     * Evicts the first entry found in L1 when capacity is exceeded.
     * Best-effort under concurrent access (soft capacity cap).
     */
    private void evictOldestEntry() {
        var iterator = l1.keySet().iterator();
        if (iterator.hasNext()) {
            CacheKey evicted = iterator.next();
            l1.remove(evicted);
            l1PutTime.remove(evicted);
            metadataIndex.remove(evicted);
            evictionCount.incrementAndGet();
        }
    }

    /**
     * Invalidate entries by table name from both layers using metadata index.
     * Precisely matches entries whose metadata references the given table.
     */
    public void invalidateByTable(String tableName) {
        String lowerTable = tableName.toLowerCase();
        invalidateByMetadata(m -> m.referencesTable(lowerTable));
    }

    /**
     * Invalidate entries by user ID from both layers using metadata index.
     * Precisely matches entries whose metadata user equals the given userId.
     */
    public void invalidateByUser(String userId) {
        invalidateByMetadata(m -> m.user().equals(userId));
    }

    /**
     * Invalidate entries by policy version from both layers using metadata index.
     * Precisely matches entries whose metadata policyVersion equals the given version.
     */
    public void invalidateByPolicyVersion(String policyVersion) {
        invalidateByMetadata(m -> m.policyVersion().equals(policyVersion));
    }

    /**
     * Shared invalidation logic: removes L1 entries whose metadata matches the
     * predicate, cleans up the metadata index, and removes corresponding L2 entries.
     */
    private void invalidateByMetadata(Predicate<CacheEntryMetadata> matcher) {
        List<String> l2KeysToRemove = new ArrayList<>();
        // PERF-T12: Use ConcurrentHashMap.entrySet().removeIf() — no write lock.
        l1.entrySet().removeIf(entry -> {
            CacheEntryMetadata metadata = metadataIndex.get(entry.getKey());
            if (metadata != null && matcher.test(metadata)) {
                l2KeysToRemove.add(entry.getKey().keyString());
                metadataIndex.remove(entry.getKey());
                l1PutTime.remove(entry.getKey());
                return true;
            }
            return false;
        });
        for (String l2Key : l2KeysToRemove) {
            l2.remove(l2Key);
        }
    }

    /**
     * Invalidate all entries from both layers.
     * Used for DDL/DML operations that may affect any cached query.
     */
    public void invalidateAll() {
        // PERF-T12: No write lock; ConcurrentHashMap operations are atomic.
        List<String> l2KeysToRemove = l1.keySet().stream()
                .map(CacheKey::keyString)
                .toList();
        l1.clear();
        metadataIndex.clear();
        l1PutTime.clear();
        for (String l2Key : l2KeysToRemove) {
            l2.remove(l2Key);
        }
    }


    /** Returns the L1 store for assertions. */
    @org.pangrova.common.VisibleForTesting
    ConcurrentHashMap<CacheKey, CachedResult> l1Store() {
        return l1;
    }

    /** Returns the metadata index for assertions. */
    @org.pangrova.common.VisibleForTesting
    ConcurrentHashMap<CacheKey, CacheEntryMetadata> metadataIndex() {
        return metadataIndex;
    }

    /** Returns the total number of cache hits (L1 + L2). */
    public long hitCount() {
        return hitCount.get();
    }

    /** Returns the total number of cache misses. */
    public long missCount() {
        return missCount.get();
    }

    /** Returns the total number of L1 evictions due to capacity limits. */
    public long evictionCount() {
        return evictionCount.get();
    }
}
