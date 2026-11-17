package org.pangrova.metadata.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.pangrova.cache.api.CacheKey;
import org.pangrova.cache.api.CacheLookup;
import org.pangrova.cache.api.CacheStore;
import org.pangrova.metadata.api.cache.CacheStats;
import org.pangrova.metadata.api.cache.MetadataCache;
import org.pangrova.metadata.api.catalog.CatalogDeploymentIdentity;
import org.pangrova.metadata.api.event.MetadataEvent;
import org.pangrova.metadata.api.event.MetadataEventListener;
import org.pangrova.metadata.api.object.TableMetadata;
import org.pangrova.metadata.codec.table.TableMetadataSerializer;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

/** L1 owns decoded projections. The selected L2 owns generation-fenced, serialized projections. */
public final class TieredMetadataCache implements MetadataCache, MetadataEventListener, AutoCloseable {
    private static final long DEFAULT_MAX_ENTRIES = 10_000;
    private static final long DEFAULT_TTL_SECONDS = 60;

    private static final Duration L2_TTL = Duration.ofMinutes(5);

    private record LocalProjection(Object catalogGeneration, TableMetadata metadata) { }

    private final Cache<CacheKey, LocalProjection> l1;
    private final Cache<String, Object> catalogs;
    private final CacheStore l2;
    private final TableMetadataSerializer serializer = new TableMetadataSerializer();
    private long invalidationSequence;
    private boolean closed;

    public TieredMetadataCache(long maxEntries, long ttlSeconds, long maxCatalogs, CacheStore store) {
        this(maxEntries, Duration.ofSeconds(ttlSeconds), maxCatalogs, store);
    }

    public TieredMetadataCache(long maxEntries, Duration ttl, CacheStore store) {
        this(maxEntries, ttl, 1000, store);
    }

    private TieredMetadataCache(long maxEntries, Duration ttl, long maxCatalogs, CacheStore store) {
        if (maxEntries <= 0 || ttl.isZero() || ttl.isNegative() || maxCatalogs <= 0) {
            throw new IllegalArgumentException("Metadata cache bounds must be positive");
        }
        l1 = Caffeine.newBuilder().maximumSize(maxEntries).expireAfterWrite(ttl)
                .recordStats().build();
        catalogs = Caffeine.newBuilder().maximumSize(maxCatalogs).expireAfterAccess(Duration.ofHours(1))
                .executor(Runnable::run).recordStats().build();
        l2 = store;
    }

    public TieredMetadataCache(long maxEntries, long ttlSeconds, CacheStore store) {
        this(maxEntries, ttlSeconds, 1000, store);
    }

    /** Local L1 only. No backend is discovered implicitly. */
    public TieredMetadataCache() {
        this(DEFAULT_MAX_ENTRIES, DEFAULT_TTL_SECONDS, null);
    }

    @Override public synchronized Optional<TableMetadata> getTable(String catalog, String schema, String table) {
        requireOpen();
        return Optional.ofNullable(lookup(key(catalog, schema, table, "")));
    }

    @Override public synchronized void putTable(String catalog, String schema, String table, TableMetadata metadata) {
        requireOpen();
        CacheKey key = key(catalog, schema, table, "");
        write(key, l2 == null ? null : l2.read(key), Objects.requireNonNull(metadata));
    }

    /** A retired deployment has a distinct key. An invalidation during a source read rejects its fill. */
    public TableMetadata loadTable(CatalogDeploymentIdentity deployment, String schema, String table,
            Supplier<TableMetadata> source) {
        String variant = deployment.revision() + ":" + deployment.incarnationId();
        CacheKey key = key(deployment.catalogName(), schema, table, variant);
        CacheLookup observed;
        long before;
        synchronized (this) {
            requireOpen();
            TableMetadata local = local(key);
            if (local != null) return local;
            observed = l2 == null ? null : l2.read(key);
            if (observed != null && observed.value().isPresent()) return decode(observed);
            before = invalidationSequence;
        }
        TableMetadata loaded = source.get();
        synchronized (this) {
            if (!closed && loaded != null && before == invalidationSequence) write(key, observed, loaded);
        }
        return loaded;
    }

    private TableMetadata lookup(CacheKey key) {
        TableMetadata value = local(key);
        if (value != null || l2 == null) return value;
        var observed = l2.read(key);
        return observed.value().isPresent() ? decode(observed) : null;
    }

    private TableMetadata local(CacheKey key) {
        var value = l1.getIfPresent(key);
        if (value != null && catalogs.getIfPresent(key.group()) != value.catalogGeneration()) {
            l1.invalidate(key);
            return null;
        }
        return value == null ? null : value.metadata();
    }

    private TableMetadata decode(CacheLookup observed) {
        TableMetadata value = serializer.deserialize(observed.value().orElseThrow());
        cacheLocally(observed.key(), value);
        return value;
    }

    private void write(CacheKey key, CacheLookup observed, TableMetadata value) {
        if (l2 == null || l2.putIfCurrent(observed, serializer.serialize(value), L2_TTL)) cacheLocally(key, value);
    }

    private void cacheLocally(CacheKey key, TableMetadata value) {
        Object generation = catalogs.get(key.group(), unused -> new Object());
        l1.put(key, new LocalProjection(generation, value));
    }

    @Override public synchronized void invalidateTable(String catalog, String schema, String table) {
        requireOpen();
        CacheKey key = key(catalog, schema, table, "");
        invalidationSequence++;
        l1.asMap().keySet().removeIf(k -> k.group().equals(catalog) && k.entry().equals(key.entry()));
        if (l2 != null) l2.invalidate(catalog, key.entry());
    }

    @Override public synchronized void invalidateCatalog(String catalog) {
        requireOpen();
        invalidationSequence++;
        l1.asMap().keySet().removeIf(k -> k.group().equals(catalog));
        catalogs.invalidate(catalog);
        if (l2 != null) l2.invalidateGroup(catalog);
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
        com.github.benmanes.caffeine.cache.stats.CacheStats l1Stats = l1.stats();
        return new CacheStats(
                l1Stats.hitCount(),
                l1Stats.missCount(),
                l1Stats.evictionCount(),
                l1.estimatedSize());
    }

    private void requireOpen() {
        if (closed) throw new IllegalStateException("Metadata cache is closed");
    }

    @Override public synchronized void close() {
        if (closed) return;
        closed = true;
        l1.invalidateAll();
        catalogs.invalidateAll();
        if (l2 != null) l2.close();
    }

    public synchronized void performMaintenance() {
        l1.cleanUp();
        catalogs.cleanUp();
    }

    public synchronized CacheStats getCatalogIndexStats() {
        var stats = catalogs.stats();
        return new CacheStats(stats.hitCount(), stats.missCount(), stats.evictionCount(), catalogs.estimatedSize());
    }

    private static CacheKey key(String catalog, String schema, String table, String variant) {
        return new CacheKey(catalog, schema.length() + ":" + schema + Objects.requireNonNull(table), variant);
    }
}
