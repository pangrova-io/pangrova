package org.pangrova.metadata.catalog.handle;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.pangrova.metadata.api.cache.MetadataCache;
import org.pangrova.metadata.api.object.TableMetadata;
import org.pangrova.connector.api.metadata.table.TableStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.pangrova.metadata.api.catalog.FederatedCatalog;
import org.pangrova.metadata.api.catalog.ChangeListener;

/**
 * Decorator that adds {@link MetadataCache}-backed memoization to a
 * {@link FederatedCatalog}.
 *
 * <p>Resolves F-CHAIN-META-06: {@code FederationConnectorMetadata} calls
 * {@code adapter.getTable()} from {@code getTableHandle}, {@code getTableMetadata},
 * and {@code getColumnHandles} — three round-trips to an external metastore per query
 * — and {@code adapter.getTableStatistics()} on top of that. Wrapping the adapter
 * folds those into a single fetch (cache miss), with subsequent lookups served from
 * the shared cache until the configured TTL expires or a DDL invalidation event fires.
 *
 * <h3>Caching layout</h3>
 * <ul>
 *   <li><b>Table metadata</b> goes through the shared {@link MetadataCache} so it
 *       participates in cross-coordinator invalidation (JdbcInvalidationPoller) and
 *       benefits from the same TTL/L1/L2 layering as connector reads.</li>
 *   <li><b>Statistics</b> are kept in a small Caffeine cache local to this wrapper
 *       with a short TTL (default 30s). Stats drift with every write, so they have
 *       a different lifecycle than schema metadata, and decoupling them lets DML
 *       skip metadata invalidation. The local cache is invalidated explicitly when
 *       {@link #invalidate(String, String)} is called from a DML callback.</li>
 *   <li><b>Schema discovery</b> ({@code listSchemas}, {@code listTables}) and
 *       <b>CDC</b> ({@code readChangesSince}) are not cached — they are inherently
 *       dynamic and used outside the hot resolution path.</li>
 * </ul>
 */
public final class CachingFederatedCatalog implements FederatedCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(CachingFederatedCatalog.class);

    private static final long DEFAULT_STATS_MAX_SIZE = 10_000;
    private static final Duration DEFAULT_STATS_TTL = Duration.ofSeconds(30);

    private final FederatedCatalog delegate;
    private final MetadataCache cache;
    private final String catalogName;
    /**
     * Local statistics cache, used as a fallback when the shared
     * {@link MetadataCache} does not implement statistics caching (default no-op).
     * Keyed by "schema/table". Stats use a shorter TTL than table metadata
     * because row counts and data sizes change with every write.
     */
    private final Cache<String, TableStatistics> localStatsCache;

    public CachingFederatedCatalog(FederatedCatalog delegate, MetadataCache cache, String catalogName) {
        this(delegate, cache, catalogName, DEFAULT_STATS_MAX_SIZE, DEFAULT_STATS_TTL);
    }

    /** Creates a caching wrapper with custom local statistics cache bounds. */
    public CachingFederatedCatalog(FederatedCatalog delegate, MetadataCache cache, String catalogName,
                                   long statsMaxSize, Duration statsTtl) {
        this.delegate = Objects.requireNonNull(delegate, "delegate must not be null");
        this.cache = Objects.requireNonNull(cache, "cache must not be null");
        this.catalogName = Objects.requireNonNull(catalogName, "catalogName must not be null");
        this.localStatsCache = Caffeine.newBuilder()
                .maximumSize(statsMaxSize)
                .expireAfterWrite(statsTtl)
                .build();
    }

    @Override
    public List<String> listSchemas() {
        return delegate.listSchemas();
    }

    @Override
    public List<String> listTables(String schema) {
        return delegate.listTables(schema);
    }

    /**
     * Returns table metadata, hitting the shared {@link MetadataCache} first.
     * On miss, fetches once from the external metastore and populates the cache;
     * subsequent calls within the TTL window are served without I/O.
     */
    @Override
    public TableMetadata getTable(String schema, String table) {
        Optional<TableMetadata> cached = cache.getTable(catalogName, schema, table);
        if (cached.isPresent()) {
            LOG.trace("event=federation_cache_hit kind=table catalog={} schema={} table={}",
                    catalogName, schema, table);
            return cached.get();
        }
        TableMetadata fetched = delegate.getTable(schema, table);
        if (fetched != null) {
            cache.putTable(catalogName, schema, table, fetched);
            LOG.trace("event=federation_cache_miss kind=table catalog={} schema={} table={}",
                    catalogName, schema, table);
        }
        return fetched;
    }

    /**
     * Returns table statistics, served from a local Caffeine cache scoped to this
     * adapter. Stats use a shorter TTL than {@link #getTable} since row counts
     * and data sizes drift with every write — they would otherwise pollute the
     * shared L1 cache lifetime.
     *
     * <p>The shared {@link MetadataCache} interface is intentionally kept minimal
     * (table metadata only); statistics live next to the federation adapter so DML
     * doesn't need to invalidate the structural schema cache.
     */
    @Override
    public TableStatistics getTableStatistics(String schema, String table) {
        String key = statsKey(schema, table);
        TableStatistics local = localStatsCache.getIfPresent(key);
        if (local != null) {
            LOG.trace("event=federation_cache_hit kind=stats catalog={} schema={} table={}",
                    catalogName, schema, table);
            return local;
        }
        TableStatistics fetched = delegate.getTableStatistics(schema, table);
        if (fetched != null) {
            localStatsCache.put(key, fetched);
            LOG.trace("event=federation_cache_miss kind=stats catalog={} schema={} table={}",
                    catalogName, schema, table);
        }
        return fetched;
    }

    @Override
    public org.pangrova.metadata.api.catalog.MetadataChangeBatch readChangesSince(Instant since) {
        return delegate.readChangesSince(since);
    }

    @Override
    public void subscribeToChanges(ChangeListener listener) {
        delegate.subscribeToChanges(listener);
    }

    @Override
    public void close() {
        delegate.close();
    }

    /**
     * Drops cached metadata and statistics for a single table. Called from
     * federation DML completion hooks so the next read sees fresh state.
     */
    public void invalidate(String schema, String table) {
        cache.invalidateTable(catalogName, schema, table);
        localStatsCache.invalidate(statsKey(schema, table));
    }

    /**
     * Drops every cached entry for this catalog. Called when the catalog is
     * dropped or fully resynced.
     */
    public void invalidateAll() {
        cache.invalidateCatalog(catalogName);
        localStatsCache.invalidateAll();
    }

    /**
     * Returns the underlying adapter — primarily for tests and diagnostics.
     */
    public FederatedCatalog delegate() {
        return delegate;
    }

    private static String statsKey(String schema, String table) {
        return schema + "/" + table;
    }
}
