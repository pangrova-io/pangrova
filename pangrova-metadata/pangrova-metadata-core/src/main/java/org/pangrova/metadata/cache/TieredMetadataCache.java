package org.pangrova.metadata.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import org.pangrova.common.concurrent.GuardedBy;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.metadata.api.event.MetadataEvent;
import org.pangrova.metadata.spi.event.MetadataEventListener;
import org.pangrova.metadata.api.object.TableMetadata;
import org.pangrova.metadata.codec.table.TableMetadataSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.VarHandle;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.pangrova.metadata.api.cache.CacheStats;
import org.pangrova.metadata.spi.cache.L2CacheStore;
import org.pangrova.metadata.api.cache.MetadataCache;

/**
 * Two-tier metadata cache: L1 Caffeine (in-process) + pluggable L2 (e.g., Redis).
 * Implements MetadataEventListener to invalidate cache entries on catalog lifecycle events.
 *
 * <p>Both {@code l1Cache} and {@code catalogIndex} are Caffeine caches with
 * {@code recordStats()} enabled. Hit/miss/eviction counts are derived from
 * Caffeine's built-in statistics rather than manual counters.</p>
 *
 * <h3>Concurrency strategy for invalidatingCatalogs</h3>
 * <p>{@code invalidatingCatalogs} tracks catalogs currently being invalidated.
 * Concurrent readers in {@link #lookupL2AndBackfill} must see a consistent view:
 * if a catalog is being invalidated, the reader must NOT register entries in
 * {@code catalogIndex} (they would leak). We use:</p>
 * <ul>
 *   <li>{@code ConcurrentHashMap.computeIfAbsent} for atomic check-and-create on
 *       the catalog index registration path</li>
 *   <li>A volatile fence ({@code invalidationEpoch}) written after adding to
 *       {@code invalidatingCatalogs} and read before checking it, ensuring
 *       happens-before ordering between the invalidation flag and catalog index
 *       mutations</li>
 * </ul>
 *
 * @see #invalidateCatalog(String)
 * @see #lookupL2AndBackfill(String)
 */
@ThreadSafe
public class TieredMetadataCache implements MetadataCache, MetadataEventListener {

    private static final Logger LOG = LoggerFactory.getLogger(TieredMetadataCache.class);

    private static final char KEY_SEPARATOR = '\u001F'; // Unit Separator
    private static final long DEFAULT_L1_MAX_SIZE = 10_000;
    private static final long DEFAULT_L1_TTL_SECONDS = 60;
    private static final long DEFAULT_L2_TTL_SECONDS = 300;
    private static final long DEFAULT_CATALOG_INDEX_MAX_SIZE = 1_000;

    /**
     * VarHandle for volatile access to {@code invalidationEpoch}.
     * Provides acquire/release semantics for cross-thread visibility.
     */
    private static final VarHandle INVALIDATION_EPOCH;

    static {
        try {
            INVALIDATION_EPOCH = java.lang.invoke.MethodHandles.lookup()
                    .findVarHandle(TieredMetadataCache.class, "invalidationEpoch", long.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final Cache<String, L1Entry> l1Cache;
    private final Cache<String, Set<String>> catalogIndex;
    private final L2CacheStore l2Adapter;
    private final TableMetadataSerializer serializer;

    /**
     * A-CACHE-01: monotonic per-table version, bumped by {@link #invalidateTable} (and table
     * alter/drop events). {@link #lookupL1} validates the version an entry was cached at against the
     * current version and evicts on mismatch (validate-on-hit) — closing the put-after-invalidate
     * race that eager L1 removal alone leaves open, and never serving a stale schema.
     */
    private final ConcurrentHashMap<String, Long> tableVersions = new ConcurrentHashMap<>();

    /** A-CACHE-01: an L1 value together with the table version it was cached at. */
    private record L1Entry(TableMetadata metadata, long versionAtCache) {}

    /**
     * Catalogs currently being invalidated. Uses ConcurrentHashMap for atomic
     * computeIfAbsent semantics. Value is always {@code Boolean.TRUE}.
     *
     * <p>Writers call {@code computeIfAbsent(catalog, k -> TRUE)} to atomically
     * mark a catalog as invalidating. Readers call {@code containsKey(catalog)}
     * after an acquire fence to ensure visibility.</p>
     */
    @GuardedBy("invalidationEpoch volatile fence")
    private final ConcurrentHashMap<String, Boolean> invalidatingCatalogs = new ConcurrentHashMap<>();

    /**
     * Volatile fence counter. Incremented (release) after modifying
     * {@code invalidatingCatalogs}; read (acquire) before checking it.
     * Ensures that a reader who sees the latest epoch also sees all prior
     * writes to {@code invalidatingCatalogs} and {@code catalogIndex}.
     */
    @SuppressWarnings("unused") // accessed via VarHandle
    private volatile long invalidationEpoch;

    /**
     * Creates the tiered cache with the given L1 size/TTL bounds and L2 adapter.
     */
    public TieredMetadataCache(long l1MaxSize, long l1TtlSeconds,
                               long catalogIndexMaxSize, L2CacheStore l2Adapter) {
        this.l1Cache = Caffeine.newBuilder()
                .maximumSize(l1MaxSize)
                .expireAfterWrite(Duration.ofSeconds(l1TtlSeconds))
                .recordStats()
                .build();
        this.catalogIndex = Caffeine.newBuilder()
                .maximumSize(catalogIndexMaxSize)
                .expireAfterAccess(Duration.ofHours(1))
                .recordStats()
                .removalListener(this::onCatalogIndexEviction)
                .build();
        this.l2Adapter = l2Adapter;
        this.serializer = new TableMetadataSerializer();
    }

    public TieredMetadataCache(long l1MaxSize, long l1TtlSeconds, L2CacheStore l2Adapter) {
        this(l1MaxSize, l1TtlSeconds, DEFAULT_CATALOG_INDEX_MAX_SIZE, l2Adapter);
    }

    public TieredMetadataCache() {
        this(DEFAULT_L1_MAX_SIZE, DEFAULT_L1_TTL_SECONDS,
                DEFAULT_CATALOG_INDEX_MAX_SIZE, new DisabledL2CacheStore());
    }

    /**
     * Removal listener for catalogIndex. When a catalog entry is evicted by
     * Caffeine's size policy, invalidate the corresponding L1 entries to
     * prevent orphaned cache entries.
     */
    private void onCatalogIndexEviction(String catalog, Set<String> keys, RemovalCause cause) {
        if (cause.wasEvicted() && keys != null) {
            LOG.debug("event=catalog_index_eviction catalog={} keyCount={} cause={}",
                    catalog, keys.size(), cause);
            for (String key : keys) {
                l1Cache.invalidate(key);
            }
        }
    }

    @Override
    public Optional<TableMetadata> getTable(String catalog, String schema, String table) {
        String key = cacheKey(catalog, schema, table);
        return lookupL1(key).or(() -> lookupL2AndBackfill(key));
    }

    @Override
    public void putTable(String catalog, String schema, String table, TableMetadata metadata) {
        String key = cacheKey(catalog, schema, table);
        l1Cache.put(key, new L1Entry(metadata, currentVersion(key)));
        catalogIndex.asMap()
                .computeIfAbsent(catalog, k -> ConcurrentHashMap.newKeySet())
                .add(key);
        l2Adapter.put(key, serializer.serialize(metadata), DEFAULT_L2_TTL_SECONDS);
    }

    @Override
    public void invalidateTable(String catalog, String schema, String table) {
        String key = cacheKey(catalog, schema, table);
        // A-CACHE-01: bump the table version BEFORE removing L1, so a concurrent putTable() that
        // captured the old version is caught by validate-on-hit even if it stores its entry after
        // this invalidation (the put-after-invalidate race).
        tableVersions.merge(key, 1L, Long::sum);
        // BUG-09 fix: delete L1 first to prevent concurrent getTable() from
        // hitting stale L1 during the window between L2 and L1 deletion.
        l1Cache.invalidate(key);
        l2Adapter.delete(key);
        l2Adapter.publishInvalidation(key);
        // BUG-005 fix: Use computeIfPresent for atomic check-then-act on the
        // catalog index. Removes the key from the Set and cleans up empty Sets
        // to prevent unbounded growth. Eliminates TOCTOU race with
        // invalidateCatalog() which calls catalogIndex.invalidate(catalog).
        catalogIndex.asMap().computeIfPresent(catalog, (cat, keys) -> {
            keys.remove(key);
            return keys.isEmpty() ? null : keys;
        });
    }

    /**
     * A-CACHE-01: marks a table changed by bumping its monotonic version WITHOUT removing the L1
     * entry; the now-stale entry is lazily evicted on its next {@link #getTable} (validate-on-hit).
     * An O(1) alternative to {@link #invalidateTable} for high-frequency / cross-coordinator
     * version-propagation signals where an immediate scan/removal is undesirable.
     */
    public void markTableChanged(String catalog, String schema, String table) {
        tableVersions.merge(cacheKey(catalog, schema, table), 1L, Long::sum);
    }

    @Override
    public void invalidateCatalog(String catalog) {
        // Mark catalog as invalidating BEFORE removing index entries.
        // computeIfAbsent provides atomic check-and-create semantics.
        invalidatingCatalogs.computeIfAbsent(catalog, k -> Boolean.TRUE);
        // Release fence: ensures the write to invalidatingCatalogs is visible
        // to any thread that subsequently performs an acquire on invalidationEpoch.
        INVALIDATION_EPOCH.getAndAdd(this, 1L);
        try {
            Set<String> keys = catalogIndex.asMap().remove(catalog);
            String prefix = catalog + KEY_SEPARATOR;
            l2Adapter.deleteByPrefix(prefix);
            if (keys != null) {
                for (String key : keys) {
                    l1Cache.invalidate(key);
                }
            }
            l1Cache.asMap().keySet().removeIf(k -> k.startsWith(prefix));
            catalogIndex.invalidate(catalog);
        } finally {
            invalidatingCatalogs.remove(catalog);
            // Release fence after removal: readers will see the catalog is no longer
            // invalidating and can safely register new entries.
            INVALIDATION_EPOCH.getAndAdd(this, 1L);
        }
    }

    @Override
    public void onEvent(MetadataEvent event) {
        if (event instanceof MetadataEvent.CatalogDroppedEvent e) {
            invalidateCatalog(e.catalogName());
        } else if (event instanceof MetadataEvent.CatalogStateChangedEvent e) {
            invalidateCatalog(e.catalogName());
        } else if (event instanceof MetadataEvent.MetadataSyncedEvent e) {
            invalidateCatalog(e.catalogName());
        } else {
            handleTableEvent(event);
        }
    }

    private void handleTableEvent(MetadataEvent event) {
        if (event instanceof MetadataEvent.TableDroppedEvent e) {
            invalidateTable(e.tableRef().catalog(), e.tableRef().schema(), e.tableRef().table());
        } else if (event instanceof MetadataEvent.TableAlteredEvent e) {
            invalidateTable(e.tableRef().catalog(), e.tableRef().schema(), e.tableRef().table());
        }
    }

    @Override
    public CacheStats getStats() {
        com.github.benmanes.caffeine.cache.stats.CacheStats l1Stats = l1Cache.stats();
        return new CacheStats(
                l1Stats.hitCount(),
                l1Stats.missCount(),
                l1Stats.evictionCount(),
                l1Cache.estimatedSize());
    }

    private long currentVersion(String key) {
        return tableVersions.getOrDefault(key, 0L);
    }

    private Optional<TableMetadata> lookupL1(String key) {
        L1Entry cached = l1Cache.getIfPresent(key);
        if (cached == null) {
            return Optional.empty();
        }
        // A-CACHE-01: validate-on-hit — if the table's version advanced since this entry was cached
        // (e.g. a put that landed just after an invalidate), the cached schema is stale; evict + miss.
        if (cached.versionAtCache() != currentVersion(key)) {
            l1Cache.invalidate(key);
            return Optional.empty();
        }
        return Optional.of(cached.metadata());
    }

    private Optional<TableMetadata> lookupL2AndBackfill(String key) {
        Optional<String> json = l2Adapter.get(key);
        if (json.isPresent()) {
            TableMetadata metadata = serializer.deserialize(json.get());
            l1Cache.put(key, new L1Entry(metadata, currentVersion(key)));
            // Keep catalogIndex in sync so invalidateCatalog() can find backfilled entries.
            // Acquire fence before checking invalidatingCatalogs: ensures we see the
            // latest state written by any concurrent invalidateCatalog() call.
            int sep = key.indexOf(KEY_SEPARATOR);
            if (sep > 0) {
                String catalog = key.substring(0, sep);
                // Acquire fence: establishes happens-before with the release fence
                // in invalidateCatalog(), ensuring we see the most recent state of
                // invalidatingCatalogs.
                INVALIDATION_EPOCH.getAcquire(this);
                if (!invalidatingCatalogs.containsKey(catalog)) {
                    // computeIfAbsent provides atomic check-and-create for the catalog
                    // index entry, preventing lost updates from concurrent putTable calls.
                    catalogIndex.asMap()
                            .computeIfAbsent(catalog, k -> ConcurrentHashMap.newKeySet())
                            .add(key);
                }
            }
            return Optional.of(metadata);
        }
        return Optional.empty();
    }

    /**
     * Forces eviction of expired entries from L1 and catalog index caches.
     * Caffeine performs maintenance lazily during reads/writes; this method
     * triggers immediate cleanup of entries past their TTL window.
     */
    public void performMaintenance() {
        l1Cache.cleanUp();
        catalogIndex.cleanUp();
    }

    /**
     * Returns statistics for the catalog index cache (catalog name → key set mapping).
     */
    public CacheStats getCatalogIndexStats() {
        com.github.benmanes.caffeine.cache.stats.CacheStats indexStats = catalogIndex.stats();
        return new CacheStats(
                indexStats.hitCount(),
                indexStats.missCount(),
                indexStats.evictionCount(),
                catalogIndex.estimatedSize());
    }

    private String cacheKey(String catalog, String schema, String table) {
        Objects.requireNonNull(catalog, "catalog must not be null");
        Objects.requireNonNull(schema, "schema must not be null");
        Objects.requireNonNull(table, "table must not be null");
        return catalog + KEY_SEPARATOR + schema + KEY_SEPARATOR + table;
    }
}
