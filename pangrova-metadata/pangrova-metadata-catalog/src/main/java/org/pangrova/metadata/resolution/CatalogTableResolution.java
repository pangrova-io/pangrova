package org.pangrova.metadata.resolution;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.connector.api.metadata.ConnectorMetadata;
import org.pangrova.connector.api.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.connector.api.metadata.table.TableStatistics;
import org.pangrova.connector.api.metadata.table.TableVersionToken;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.metadata.api.cache.MetadataCache;
import org.pangrova.metadata.api.catalog.CatalogHandle;
import org.pangrova.metadata.api.exception.MetadataException;
import org.pangrova.metadata.api.object.ColumnMetadata;
import org.pangrova.metadata.api.object.ColumnType;
import org.pangrova.metadata.api.object.QualifiedTableRef;
import org.pangrova.metadata.api.object.TableMetadata;
import org.pangrova.metadata.api.resolution.ResolvedTable;
import org.pangrova.metadata.api.session.MetadataSessionContext;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.metadata.connector.DefaultConnectorSession;
import org.pangrova.security.api.exception.AccessDeniedException;
import org.pangrova.table.api.identity.CatalogDeploymentIdentity;
import org.pangrova.table.api.enums.OwnershipMode;
import org.pangrova.table.api.enums.TableControlState;
import org.pangrova.table.api.ownership.TableControlView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/** Resolves authenticated table handles and owns their scoped cache and invalidation. */
public final class CatalogTableResolution {
    private static final Logger LOG = LoggerFactory.getLogger(CatalogTableResolution.class);
    private static final Duration OBSERVED_RESOLUTION_CACHE_TTL = Duration.ofSeconds(30);

    private final CatalogRegistry catalogRegistry;
    private final MetadataCache cache;
    private final Cache<MetadataResolutionCacheKey, ResolvedTable> resolvedTableCache;

    public CatalogTableResolution(CatalogRegistry catalogRegistry, MetadataCache cache,
            long resolutionCacheMaxSize, Duration resolutionCacheTtl) {
        this.catalogRegistry = catalogRegistry;
        this.cache = cache;
        long controlledTtlNanos = Math.max(0L, resolutionCacheTtl.toNanos());
        long observedTtlNanos = Math.min(
                controlledTtlNanos, OBSERVED_RESOLUTION_CACHE_TTL.toNanos());
        this.resolvedTableCache = Caffeine.newBuilder()
                .maximumSize(resolutionCacheMaxSize)
                .expireAfter(new Expiry<MetadataResolutionCacheKey, ResolvedTable>() {
                    @Override
                    public long expireAfterCreate(MetadataResolutionCacheKey key,
                            ResolvedTable table, long currentTime) {
                        return table.ownershipMode()
                                == OwnershipMode.OBSERVED
                                ? observedTtlNanos : controlledTtlNanos;
                    }

                    @Override
                    public long expireAfterUpdate(MetadataResolutionCacheKey key,
                            ResolvedTable table, long currentTime, long currentDuration) {
                        return currentDuration;
                    }

                    @Override
                    public long expireAfterRead(MetadataResolutionCacheKey key,
                            ResolvedTable table, long currentTime, long currentDuration) {
                        return currentDuration;
                    }
                })
                .recordStats()
                .build();
    }

    public ResolvedTable resolveTable(MetadataSessionContext session, QualifiedTableRef tableRef) {
        CatalogDeploymentIdentity deployment = catalogRegistry.requireCurrentCatalogIdentity(tableRef.catalog());
        TableControlView admitted = ownership(tableRef);
        MetadataResolutionCacheKey cacheKey = MetadataResolutionCacheKey.of(session, tableRef, deployment);
        ResolvedTable cached = resolvedTableCache.getIfPresent(cacheKey);
        if (cached != null && isCacheEntryCurrent(admitted, cached)) {
            LOG.debug("event=resolve_cache_hit catalog={} schema={} table={}",
                    tableRef.catalog(), tableRef.schema(), tableRef.table());
            return cached;
        }
        if (cached != null) {
            resolvedTableCache.invalidate(cacheKey);
        }
        LOG.debug("event=resolve_cache_miss catalog={} schema={} table={}",
                tableRef.catalog(), tableRef.schema(), tableRef.table());
        ResolvedTable resolved = resolveFromCatalog(session, tableRef, deployment, admitted);
        resolvedTableCache.put(cacheKey, resolved);
        return resolved;
    }

    /**
     * Reuse a resolution only while its ownership, stable table id, revision and
     * version still match the authority. ADOPT and same-name replacement must
     * invalidate cached entries even when the numeric revision is unchanged.
     */
    private static boolean isCacheEntryCurrent(TableControlView owner, ResolvedTable cached) {
        return cached.ownershipMode() == owner.ownershipMode()
                && cached.tableId().equals(owner.tableId())
                && cached.controlRevision() == owner.metadataRevision()
                && (owner.ownershipMode() == OwnershipMode.OBSERVED || cached.versionToken().equals(owner.versionToken()));
    }

    private TableControlView ownership(QualifiedTableRef tableRef) {
        var owner = catalogRegistry.tableControl().describe(tableRef.catalog(), tableRef.schema(), tableRef.table());
        if (owner.state().filter(state -> state == TableControlState.RENAMING).isPresent()) {
            throw new AccessDeniedException("Table rename is incomplete for " + tableRef
                    + "; wait for completion or reconcile the DDL before querying");
        }
        return owner;
    }

    private ResolvedTable resolveFromCatalog(MetadataSessionContext session, QualifiedTableRef tableRef,
            CatalogDeploymentIdentity deployment, TableControlView admitted) {
        CatalogHandle handle = catalogRegistry.getCatalog(tableRef.catalog());
        var descriptor = handle.getDescriptor();
        if (!deployment.incarnationId().equals(descriptor.incarnationId()) || deployment.revision() != descriptor.revision()) {
            throw new AccessDeniedException("Catalog deployment changed during table resolution for " + tableRef);
        }
        ConnectorSession connSession = DefaultConnectorSession.fromSessionContext(
                session, tableRef.catalog(), tableRef.schema());
        ConnectorMetadata connMetadata = handle.getMetadata(connSession);
        SchemaTableName schemaTable = new SchemaTableName(tableRef.schema(), tableRef.table());
        ConnectorTableHandle tableHandle = connMetadata.getTableHandle(connSession, schemaTable);
        if (tableHandle == null) {
            throw new MetadataException(
                    PangrovaErrorCode.METADATA_TABLE_NOT_FOUND,
                    "Table not found: catalog=%s, schema=%s, table=%s".formatted(
                            tableRef.catalog(), tableRef.schema(), tableRef.table()));
        }
        ConnectorTableMetadata tableMeta = connMetadata.getTableMetadata(connSession, tableHandle);
        TableStatistics stats = connMetadata.getTableStatistics(connSession, tableHandle);
        // A controlled table's version is the registry's value: that is what
        // admission and cache revalidation compare against. An OBSERVED table has
        // no persisted version, so capture the live one for diagnostics while the
        // short TTL keeps the cached resolution honest.
        java.util.Optional<String> versionToken = admitted.versionToken();
        if (admitted.ownershipMode() == OwnershipMode.OBSERVED) {
            versionToken = connMetadata.observeVersion(connSession, tableHandle)
                    .map(TableVersionToken::token);
        }
        if (!deployment.equals(catalogRegistry.requireCurrentCatalogIdentity(tableRef.catalog()))
                || !admitted.equals(ownership(tableRef))) {
            throw new AccessDeniedException("Catalog deployment or table ownership changed during resolution for "
                    + tableRef + "; resolve the table again");
        }
        return new ResolvedTable(tableRef, tableHandle, tableMeta, stats,
                admitted.ownershipMode(), admitted.tableId(), versionToken, admitted.metadataRevision());
    }

    public TableMetadata currentMetadata(MetadataSessionContext session, QualifiedTableRef tableRef) {
        // The tiered store has no authenticated scope. Never use it as authority
        // for tenant-specific DDL validation, or to read back an ALTER result.
        CatalogDeploymentIdentity deployment = catalogRegistry.requireCurrentCatalogIdentity(tableRef.catalog());
        return toTableMetadata(tableRef, resolveFromCatalog(session, tableRef, deployment, ownership(tableRef)).tableMetadata());
    }

    private static TableMetadata toTableMetadata(QualifiedTableRef ref, ConnectorTableMetadata connMeta) {
        List<ColumnMetadata> columns = connMeta.typedColumns().stream()
                .map(c -> new ColumnMetadata(c.name(), ColumnType.of(c.type().toString()), true, "", Map.of()))
                .toList();
        return new TableMetadata(
                ref.catalog(), ref.schema(), ref.table(),
                columns, connMeta.properties(), List.of(), Map.of(), null, null);
    }

    /**
     * Invalidates all cached metadata for the given table across both the resolution
     * cache and the tiered metadata cache. Called on DDL operations.
     */
    public void invalidateTableCaches(QualifiedTableRef tableRef) {
        resolvedTableCache.asMap().keySet().removeIf(key -> key.tableRef().equals(tableRef));
        cache.invalidateTable(tableRef.catalog(), tableRef.schema(), tableRef.table());
    }

    /**
     * Invalidates all cached metadata for the given catalog. Called on catalog-level DDL.
     */
    public void invalidateCatalogCaches(String catalogName) {
        resolvedTableCache.asMap().keySet()
                .removeIf(key -> key.tableRef().catalog().equals(catalogName));
        cache.invalidateCatalog(catalogName);
    }

}
