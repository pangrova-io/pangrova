package org.pangrova.spi.cache;

import org.pangrova.spi.annotation.Experimental;

import java.util.Optional;

/**
 * SPI for a distributed (shared) data cache store that allows multiple
 * workers to share cached data blocks via an external backend.
 *
 * <h3>Motivation</h3>
 * <p>The current Pangrova cache architecture is strictly per-worker local:
 * each worker's native async data cache (RAM) and SSD cache are invisible to
 * other workers. This means:</p>
 * <ul>
 *   <li>When a worker is replaced, its entire cache is lost and must be
 *       re-warmed from remote storage.</li>
 *   <li>Multiple workers scanning overlapping data each pay the full I/O
 *       cost independently.</li>
 *   <li>Cache-aware scheduling ({@link CacheTopology#SHARED_NOTHING}) helps
 *       but cannot prevent cache loss on node failure.</li>
 * </ul>
 *
 * <h3>Design</h3>
 * <p>This SPI abstracts a shared cache tier that sits between the local
 * worker cache and remote object storage. Implementations may back onto
 * Redis, a shared filesystem (NFS/Lustre), an object store prefix used
 * as a cache tier, or any other shared-state medium.</p>
 *
 * <p>The cache store is a simple key-value interface. Keys use the
 * {@link CacheKey} model (file path + byte range) to match the native async
 * data cache addressing scheme. Values are opaque byte blobs
 * wrapped in {@link CacheEntry}.</p>
 *
 * <h3>Consistency model</h3>
 * <p>The cache is best-effort. Implementations are not required to be
 * strongly consistent across all readers; eventual visibility is
 * acceptable. Stale entries are tolerable because Iceberg/Paimon file
 * paths are immutable -- a file path always refers to the same content.
 * The {@link #invalidate(CacheKey)} and {@link #invalidateAll(String)}
 * operations handle cases where files are physically removed (snapshot
 * expiry, compaction).</p>
 *
 * <h3>Thread safety</h3>
 * <p>Implementations must be thread-safe. Multiple worker threads will
 * call {@code get}/{@code put} concurrently from I/O threads.</p>
 *
 * <h3>Backend examples</h3>
 * <ul>
 *   <li><b>Redis:</b> Low-latency, bounded memory. Suitable for hot
 *       metadata and small blocks. TTL-based eviction.</li>
 *   <li><b>Shared filesystem (NFS/Lustre):</b> Large capacity, moderate
 *       latency. Suitable for data blocks and SSD-tier overflow.</li>
 *   <li><b>Object store tiering (S3/GCS/HDFS):</b> Unlimited capacity,
 *       higher latency. Suitable for pre-computed results or large
 *       materialized views.</li>
 * </ul>
 *
 * <p>G-CACHE-08: distributed/detached cache layer SPI.</p>
 *
 * @see CacheTopology#SHARED_STORE
 * @see CacheKey
 * @see CacheEntry
 */
@Experimental(reason = "G-CACHE-08: distributed cache store SPI")
public interface DistributedCacheStore extends AutoCloseable {

    /**
     * Retrieves a cached entry by key.
     *
     * <p>Returns {@link Optional#empty()} on cache miss, backend
     * unavailability, or deserialization error. Implementations must
     * never throw on a miss -- they should log and return empty.</p>
     *
     * @param key the cache key identifying the data block
     * @return the cached entry, or empty if not present
     */
    Optional<CacheEntry> get(CacheKey key);

    /**
     * Stores or overwrites a cache entry.
     *
     * <p>Implementations may apply size limits, TTL policies, or LRU
     * eviction internally. A put that exceeds the store's capacity
     * should be silently dropped (not thrown).</p>
     *
     * @param key   the cache key
     * @param entry the entry to store
     */
    void put(CacheKey key, CacheEntry entry);

    /**
     * Removes a single cache entry by key.
     *
     * <p>No-op if the key is not present. Implementations must not
     * throw if the key does not exist.</p>
     *
     * @param key the cache key to invalidate
     */
    void invalidate(CacheKey key);

    /**
     * Removes all cache entries associated with the given table.
     *
     * <p>This is the bulk-invalidation path used when a table undergoes
     * snapshot expiry, compaction, or schema-breaking DDL. The
     * {@code tableId} matches the {@link CacheEntry#tableId()} recorded
     * at put time.</p>
     *
     * <p>Implementations may scan keys or maintain a secondary index
     * by table. Best-effort is acceptable -- missing a few entries is
     * tolerable because stale reads on immutable file paths do not
     * cause correctness issues.</p>
     *
     * @param tableId logical table identifier
     *                (e.g. {@code "catalog.schema.table"})
     */
    void invalidateAll(String tableId);

    /**
     * Returns approximate statistics about the cache store for
     * monitoring and diagnostics.
     */
    default CacheStoreStats stats() {
        return CacheStoreStats.UNKNOWN;
    }

    /**
     * Releases resources held by this cache store (connections, threads).
     * Called during worker shutdown.
     */
    @Override
    void close();

    /**
     * A no-op store that always misses and discards writes.
     * Used as the default when {@link CacheTopology#LOCAL_ONLY} or
     * {@link CacheTopology#SHARED_NOTHING} is active.
     */
    DistributedCacheStore NOOP = new DistributedCacheStore() {
        @Override
        public Optional<CacheEntry> get(CacheKey key) {
            return Optional.empty();
        }

        @Override
        public void put(CacheKey key, CacheEntry entry) { }

        @Override
        public void invalidate(CacheKey key) { }

        @Override
        public void invalidateAll(String tableId) { }

        @Override
        public void close() { }

        @Override
        public String toString() {
            return "DistributedCacheStore.NOOP";
        }
    };
}
