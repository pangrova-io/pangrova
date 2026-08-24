package org.pangrova.connector.paimon;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.paimon.table.source.Split;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * In-process registry for Paimon SDK Split objects.
 *
 * <p>Decouples the SDK Split lifecycle from the {@code PaimonSplit} record (which
 * implements the SPI {@code ConnectorSplit} contract). The SDK Split is registered
 * at split-planning time and consumed (removed) at read time, preventing SDK type
 * leakage across module boundaries.</p>
 *
 * <p>Uses Caffeine LRU cache with 30-minute TTL to prevent unbounded memory growth
 * from splits that are never consumed (e.g., due to query cancellation or failure).
 *
 * <p>Thread-safe: backed by Caffeine concurrent cache.</p>
 */
public final class PaimonSplitRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonSplitRegistry.class);

    private static final int DEFAULT_MAX_SIZE = 100_000;
    private static final Duration DEFAULT_TTL = Duration.ofMinutes(30);

    private static final AtomicLong ID_GENERATOR = new AtomicLong();
    private final Cache<Long, Split> splits;
    private final ConcurrentHashMap<String, Set<Long>> queryIndex = new ConcurrentHashMap<>();

    public PaimonSplitRegistry() {
        this(DEFAULT_MAX_SIZE, DEFAULT_TTL);
    }

    PaimonSplitRegistry(int maxSize, Duration ttl) {
        this.splits = Caffeine.newBuilder()
                .maximumSize(maxSize)
                .expireAfterAccess(ttl)
                .evictionListener((key, value, cause) ->
                        LOG.debug("event=split_evicted splitId={} cause={}", key, cause))
                .build();
    }

    /**
     * Registers an SDK Split and returns a unique identifier for later retrieval.
     */
    long register(Split split) {
        long id = ID_GENERATOR.incrementAndGet();
        splits.put(id, split);
        return id;
    }

    /**
     * Registers an SDK Split associated with a query for lifecycle tracking.
     */
    long register(Split split, String queryId) {
        long id = register(split);
        if (queryId != null) {
            queryIndex.computeIfAbsent(queryId, k -> ConcurrentHashMap.newKeySet()).add(id);
        }
        return id;
    }

    /**
     * Releases all splits associated with the given query, freeing memory.
     */
    void releaseAll(String queryId) {
        Set<Long> ids = queryIndex.remove(queryId);
        if (ids != null && !ids.isEmpty()) {
            for (Long id : ids) {
                splits.invalidate(id);
            }
            LOG.debug("event=splits_released queryId={} count={}", queryId, ids.size());
        }
    }

    /**
     * Retrieves the SDK Split associated with the given identifier.
     * First call for a given ID returns the Split; subsequent calls return null
     * (consumed semantics for deduplication when multiple expanded splits share one SDK Split).
     */
    org.apache.paimon.table.source.Split getAndMarkConsumed(long splitId) {
        return splits.asMap().remove(splitId);
    }

    /**
     * Retrieves the SDK Split without consuming it. Used when the split must remain
     * available for multiple accesses (e.g., native dispatch path).
     *
     * @throws IllegalStateException if no split is registered for the given id
     */
    org.apache.paimon.table.source.Split get(long splitId) {
        org.apache.paimon.table.source.Split split = splits.getIfPresent(splitId);
        if (split == null) {
            throw new IllegalStateException(
                    "No Paimon SDK Split registered for id=" + splitId
                            + ". Split may have been evicted or was never registered.");
        }
        return split;
    }

    /**
     * Returns the number of splits currently held in the registry.
     * Useful for monitoring and leak detection.
     */
    int size() {
        return (int) splits.estimatedSize();
    }

    /**
     * Removes all registered splits. Called during connector shutdown.
     */
    void clear() {
        splits.invalidateAll();
    }
}
