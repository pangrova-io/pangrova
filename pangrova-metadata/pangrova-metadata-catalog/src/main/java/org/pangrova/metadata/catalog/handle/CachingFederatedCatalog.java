package org.pangrova.metadata.catalog.handle;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.pangrova.metadata.api.catalog.CatalogDeploymentIdentity;
import org.pangrova.metadata.cache.TieredMetadataCache;
import org.pangrova.metadata.api.object.TableMetadata;
import org.pangrova.connector.api.metadata.table.TableStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.List;
import java.util.Objects;
import org.pangrova.metadata.api.catalog.FederatedCatalog;
import org.pangrova.metadata.api.catalog.ChangeListener;

/**
 * Federation query reads share table projections for an immutable catalog deployment. Statistics
 * have a separate short-lived per-handle cache. The handle retains the raw source for sync and health.
 */
public final class CachingFederatedCatalog implements FederatedCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(CachingFederatedCatalog.class);

    private static final long DEFAULT_STATS_MAX_SIZE = 10_000;
    private static final Duration DEFAULT_STATS_TTL = Duration.ofSeconds(30);

    private final FederatedCatalog delegate;
    private final TieredMetadataCache cache;
    private final CatalogDeploymentIdentity deployment;
    private final Cache<String, TableStatistics> localStatsCache;

    public CachingFederatedCatalog(FederatedCatalog delegate, TieredMetadataCache cache, CatalogDeploymentIdentity deployment) {
        this(delegate, cache, deployment, DEFAULT_STATS_MAX_SIZE, DEFAULT_STATS_TTL);
    }

    /** Creates a caching wrapper with custom local statistics cache bounds. */
    public CachingFederatedCatalog(FederatedCatalog delegate, TieredMetadataCache cache, CatalogDeploymentIdentity deployment,
                                   long statsMaxSize, Duration statsTtl) {
        this.delegate = Objects.requireNonNull(delegate, "delegate must not be null");
        this.cache = Objects.requireNonNull(cache, "cache must not be null");
        this.deployment = Objects.requireNonNull(deployment);
        this.localStatsCache = Caffeine.newBuilder()
                .maximumSize(statsMaxSize)
                .expireAfterWrite(statsTtl)
                .build();
    }

    @Override
    public Optional<TableMetadata> observeTable(String schema, String table) {
        return delegate.observeTable(schema, table);
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
     * Returns table metadata, hitting the shared {@link TieredMetadataCache} first.
     * On miss, fetches once from the external metastore and populates the cache;
     * subsequent calls within the TTL window are served without I/O.
     */
    @Override
    public TableMetadata getTable(String schema, String table) {
        return cache.loadTable(deployment, schema, table, () -> delegate.getTable(schema, table));
    }

    /**
     * Returns table statistics, served from a local Caffeine cache scoped to this
     * adapter. Stats use a shorter TTL than {@link #getTable} since row counts
     * and data sizes drift with every write — they would otherwise pollute the
     * shared L1 cache lifetime.
     *
     * <p>The shared {@link TieredMetadataCache} interface is intentionally kept minimal
     * (table metadata only); statistics live next to the federation adapter so DML
     * doesn't need to invalidate the structural schema cache.
     */
    @Override
    public TableStatistics getTableStatistics(String schema, String table) {
        String key = statsKey(schema, table);
        TableStatistics local = localStatsCache.getIfPresent(key);
        if (local != null) {
            LOG.trace("event=federation_cache_hit kind=stats catalog={} schema={} table={}",
                    deployment.catalogName(), schema, table);
            return local;
        }
        TableStatistics fetched = delegate.getTableStatistics(schema, table);
        if (fetched != null) {
            localStatsCache.put(key, fetched);
            LOG.trace("event=federation_cache_miss kind=stats catalog={} schema={} table={}",
                    deployment.catalogName(), schema, table);
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
        cache.invalidateTable(deployment.catalogName(), schema, table);
        localStatsCache.invalidate(statsKey(schema, table));
    }

    /**
     * Drops every cached entry for this catalog. Called when the catalog is
     * dropped or fully resynced.
     */
    public void invalidateAll() {
        cache.invalidateCatalog(deployment.catalogName());
        localStatsCache.invalidateAll();
    }

    /**
     * Returns the underlying adapter — primarily for tests and diagnostics.
     */
    public FederatedCatalog delegate() {
        return delegate;
    }

    private static String statsKey(String schema, String table) {
        return schema.length() + ":" + schema + table;
    }
}
