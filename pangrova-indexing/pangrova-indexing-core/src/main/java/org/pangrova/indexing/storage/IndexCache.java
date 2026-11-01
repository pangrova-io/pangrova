package org.pangrova.indexing.storage;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import org.pangrova.common.bloom.SplitBlockBloomFilter;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.spi.indexing.data.BloomFilterIndexData;
import org.pangrova.spi.indexing.data.IndexData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * High-concurrency cache for loaded index data, keyed by (indexId, dataFilePath, generation).
 *
 * <p>Backed by Caffeine with {@code maximumWeight} + {@code weigher} for byte-based
 * eviction (W-TinyLFU policy). Replaces the previous hand-written LRU implementation
 * that used {@code ConcurrentLinkedDeque} with manual compaction.</p>
 *
 * <p>Caffeine handles all eviction, concurrency, and access-order tracking internally.
 * No locks are held during eviction callbacks. Two secondary indexes enable O(k)
 * invalidation: {@code indexIdIndex} by indexId, {@code dataFileIndex} by dataFilePath.</p>
 *
 * <p>Concurrency strategy: Caffeine cache handles thread-safe access internally.
 * Both secondary indexes use ConcurrentHashMap with ConcurrentHashMap.newKeySet().</p>
 */
@ThreadSafe
public final class IndexCache {

    private static final Logger LOG = LoggerFactory.getLogger(IndexCache.class);

    private final Cache<CacheKey, IndexData> cache;
    private final ConcurrentHashMap<String, Set<CacheKey>> indexIdIndex;
    private final ConcurrentHashMap<String, Set<CacheKey>> dataFileIndex;
    private final long maxSizeBytes;

    /** Creates a cache bounded by the given maximum size in bytes. */
    public IndexCache(long maxSizeBytes) {
        if (maxSizeBytes <= 0) {
            throw new IllegalArgumentException("maxSizeBytes must be positive");
        }
        this.maxSizeBytes = maxSizeBytes;
        this.indexIdIndex = new ConcurrentHashMap<>();
        this.dataFileIndex = new ConcurrentHashMap<>();
        this.cache = Caffeine.newBuilder()
                .maximumWeight(maxSizeBytes)
                .weigher((CacheKey key, IndexData data) -> {
                    long sizeBytes = data.estimatedSizeInBytes();
                    if (sizeBytes <= Integer.MAX_VALUE) {
                        return (int) sizeBytes;
                    }
                    return Integer.MAX_VALUE;
                })
                .evictionListener((CacheKey key, IndexData value, RemovalCause cause) -> {
                    if (key != null) {
                        Set<CacheKey> idKeys = indexIdIndex.get(key.indexId());
                        if (idKeys != null) {
                            idKeys.remove(key);
                        }
                        Set<CacheKey> fileKeys = dataFileIndex.get(key.dataFilePath());
                        if (fileKeys != null) {
                            fileKeys.remove(key);
                        }
                        LOG.debug("event=index_cache_eviction indexId={} dataFilePath={} "
                                        + "generation={} cause={} sizeBytes={}",
                                key.indexId(), key.dataFilePath(), key.generation(),
                                cause, value != null ? value.estimatedSizeInBytes() : 0);
                    }
                })
                .recordStats()
                .build();
    }

    /**
     * Retrieves cached index data, or null if not present.
     *
     * <p>On cache miss, invalidates any older-generation entries for the same
     * indexId+dataFilePath to prevent stale data from persisting.</p>
     */
    public IndexData get(String indexId, String dataFilePath, int generation) {
        CacheKey key = new CacheKey(indexId, dataFilePath, generation);
        IndexData result = cache.getIfPresent(key);
        if (result == null) {
            invalidateOlderGenerations(indexId, dataFilePath, generation);
        }
        return result;
    }

    /**
     * Inserts or updates index data in the cache.
     *
     * <p>When the data is a {@link BloomFilterIndexData}, pre-deserializes the
     * {@link SplitBlockBloomFilter} and wraps it in a {@link CachedBloomFilterIndexData}
     * to eliminate per-evaluation deserialization on the hot query path.</p>
     *
     * <p>Caffeine handles eviction automatically via maximumWeight policy.</p>
     */
    public void put(String indexId, String dataFilePath, int generation, IndexData data) {
        if (data.estimatedSizeInBytes() > Integer.MAX_VALUE) {
            LOG.warn("event=index_cache_skip_oversized indexId={} sizeBytes={} maxWeighable={}",
                    indexId, data.estimatedSizeInBytes(), Integer.MAX_VALUE);
            return;
        }
        CacheKey key = new CacheKey(indexId, dataFilePath, generation);
        IndexData enriched = enrichBloomFilter(data);
        indexIdIndex.computeIfAbsent(indexId,
                ignored -> ConcurrentHashMap.newKeySet()).add(key);
        dataFileIndex.computeIfAbsent(dataFilePath,
                ignored -> ConcurrentHashMap.newKeySet()).add(key);
        cache.put(key, enriched);
    }

    /**
     * Wraps {@link BloomFilterIndexData} with a pre-deserialized filter.
     * Returns the original data unchanged for all other index types or
     * if deserialization fails (e.g., malformed filter bytes).
     */
    private static IndexData enrichBloomFilter(IndexData data) {
        if (data instanceof CachedBloomFilterIndexData) {
            return data; // already enriched
        }
        if (data instanceof BloomFilterIndexData bloom) {
            try {
                SplitBlockBloomFilter filter = SplitBlockBloomFilter.readFrom(bloom.filterData());
                return new CachedBloomFilterIndexData(bloom, filter);
            } catch (IllegalArgumentException e) {
                LOG.debug("event=bloom_filter_pre_deserialize_skip reason={}", e.getMessage());
                return data;
            }
        }
        return data;
    }

    /**
     * Removes all cached entries for the given index.
     * O(k) where k = entries for this indexId, via secondary index lookup.
     * Also cleans up {@code dataFileIndex} references for removed keys.
     */
    public void invalidateIndex(String indexId) {
        Set<CacheKey> keys = indexIdIndex.remove(indexId);
        if (keys == null) {
            return;
        }
        cache.invalidateAll(keys);
        for (CacheKey key : keys) {
            Set<CacheKey> fileKeys = dataFileIndex.get(key.dataFilePath());
            if (fileKeys != null) {
                fileKeys.remove(key);
            }
        }
    }

    /**
     * Removes all cached entries for the given data file path.
     * O(k) where k = entries for this dataFilePath, via secondary index lookup.
     * Also cleans up {@code indexIdIndex} references for removed keys.
     */
    public void invalidateDataFile(String dataFilePath) {
        Set<CacheKey> keys = dataFileIndex.remove(dataFilePath);
        if (keys == null) {
            return;
        }
        cache.invalidateAll(keys);
        for (CacheKey key : keys) {
            Set<CacheKey> idKeys = indexIdIndex.get(key.indexId());
            if (idKeys != null) {
                idKeys.remove(key);
            }
        }
    }

    /** Returns the current number of cached entries. */
    public int size() {
        cache.cleanUp();
        return (int) cache.estimatedSize();
    }

    /**
     * Returns the current total estimated weight in bytes.
     * Note: Caffeine eviction is asynchronous, so this is an approximation.
     */
    public long currentSizeBytes() {
        cache.cleanUp();
        return cache.policy().eviction()
                .map(eviction -> eviction.weightedSize().orElse(0L))
                .orElse(0L);
    }

    /**
     * Returns the access-order deque size. With Caffeine, this is equivalent
     * to the estimated entry count (kept for API compatibility with existing
     * property tests).
     */
    public int accessOrderSize() {
        cache.cleanUp();
        return (int) cache.estimatedSize();
    }

    /** Returns the current cache hit rate (0.0–1.0). */
    public double hitRate() {
        return cache.stats().hitRate();
    }

    /** Returns the total number of cache hits. */
    public long hitCount() {
        return cache.stats().hitCount();
    }

    /** Returns the total number of cache misses. */
    public long missCount() {
        return cache.stats().missCount();
    }

    /** Returns the total number of evictions. */
    public long evictionCount() {
        return cache.stats().evictionCount();
    }

    /**
     * Returns the Caffeine CacheStats snapshot for observability integration.
     */
    public CacheStats stats() {
        return cache.stats();
    }

    /**
     * Removes entries for the same indexId+dataFilePath with generation older
     * than currentGeneration. Called on cache miss to clean up stale entries.
     */
    private void invalidateOlderGenerations(String indexId, String dataFilePath, int currentGeneration) {
        Set<CacheKey> keys = indexIdIndex.get(indexId);
        if (keys == null) {
            return;
        }
        List<CacheKey> toRemove = collectOlderKeys(keys, dataFilePath, currentGeneration);
        removeStaleEntries(toRemove, keys);
    }

    private static List<CacheKey> collectOlderKeys(Set<CacheKey> keys, String dataFilePath,
                                                    int currentGeneration) {
        List<CacheKey> toRemove = new ArrayList<>();
        for (CacheKey candidate : keys) {
            if (candidate.dataFilePath().equals(dataFilePath)
                    && candidate.generation() < currentGeneration) {
                toRemove.add(candidate);
            }
        }
        return toRemove;
    }

    private void removeStaleEntries(List<CacheKey> toRemove, Set<CacheKey> indexKeys) {
        if (toRemove.isEmpty()) {
            return;
        }
        cache.invalidateAll(toRemove);
        indexKeys.removeAll(toRemove);
        for (CacheKey removed : toRemove) {
            Set<CacheKey> fileKeys = dataFileIndex.get(removed.dataFilePath());
            if (fileKeys != null) {
                fileKeys.remove(removed);
            }
        }
    }

    record CacheKey(String indexId, String dataFilePath, int generation) {}
}
