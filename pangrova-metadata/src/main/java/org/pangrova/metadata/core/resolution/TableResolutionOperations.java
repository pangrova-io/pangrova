package org.pangrova.metadata.core.resolution;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.metadata.api.binding.ResolvedTable;
import org.pangrova.metadata.api.control.OwnershipMode;
import org.pangrova.metadata.api.error.MetadataException;
import org.pangrova.metadata.api.object.QualifiedTableRef;
import org.pangrova.metadata.api.schema.ColumnMetadata;
import org.pangrova.metadata.api.schema.ColumnType;
import org.pangrova.metadata.api.schema.TableMetadata;
import org.pangrova.metadata.api.session.MetadataSessionContext;
import org.pangrova.metadata.core.DefaultConnectorSession;
import org.pangrova.metadata.core.cache.MetadataCache;
import org.pangrova.metadata.core.catalog.CatalogHandle;
import org.pangrova.metadata.core.catalog.CatalogRegistry;
import org.pangrova.metadata.core.control.TableControlState;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.connector.metadata.table.TableStatistics;
import org.pangrova.spi.connector.metadata.table.TableVersionToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Owns normal table/schema resolution and the authenticated resolution cache. */
public final class TableResolutionOperations {

    private static final Logger LOG = LoggerFactory.getLogger(TableResolutionOperations.class);
    private static final long DEFAULT_CACHE_MAX_SIZE = 10_000;
    private static final Duration DEFAULT_CACHE_TTL = Duration.ofMinutes(5);
    private static final Duration OBSERVED_TTL = Duration.ofSeconds(30);

    private final CatalogRegistry catalogRegistry;
    private final MetadataCache cache;
    private final Cache<MetadataResolutionCacheKey, ResolvedTable> resolvedTableCache;

    /** Creates resolution operations with the default local cache bounds. */
    public TableResolutionOperations(CatalogRegistry catalogRegistry, MetadataCache cache) {
        this(catalogRegistry, cache, DEFAULT_CACHE_MAX_SIZE, DEFAULT_CACHE_TTL);
    }

    /** Creates resolution operations with custom local cache size and TTL bounds. */
    public TableResolutionOperations(CatalogRegistry catalogRegistry, MetadataCache cache,
            long maxSize, Duration ttl) {
        this.catalogRegistry = Objects.requireNonNull(catalogRegistry, "catalogRegistry");
        this.cache = Objects.requireNonNull(cache, "cache");
        long controlled = Math.max(0L, ttl.toNanos());
        long observed = Math.min(controlled, OBSERVED_TTL.toNanos());
        this.resolvedTableCache = Caffeine.newBuilder()
                .maximumSize(maxSize)
                .expireAfter(new Expiry<MetadataResolutionCacheKey, ResolvedTable>() {
                    @Override
                    public long expireAfterCreate(MetadataResolutionCacheKey key,
                            ResolvedTable table, long currentTime) {
                        return table.ownershipMode() == OwnershipMode.OBSERVED ? observed : controlled;
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

    /** Lists schema names visible in one catalog for the authenticated session. */
    public List<String> listSchemas(MetadataSessionContext session, String catalogName) {
        CatalogHandle handle = catalogRegistry.getCatalog(catalogName);
        ConnectorSession connSession = DefaultConnectorSession.fromSessionContext(session, catalogName, null);
        ConnectorMetadata metadata = handle.getMetadata(connSession);
        return metadata.listSchemaNames(connSession);
    }

    /** Resolves a table against the control plane, fencing cached bindings against live authority. */
    public ResolvedTable resolveTable(MetadataSessionContext session, QualifiedTableRef tableRef) {
        var control = catalogRegistry.tableControl();
        if (control.enabled()) {
            control.find(tableRef.catalog(), tableRef.schema(), tableRef.table())
                    .ifPresent(record -> control.requireCurrentCatalog(record, "Table resolution"));
        }
        if (control.enabled() && control.find(tableRef.catalog(), tableRef.schema(), tableRef.table())
                .filter(record -> record.state() == TableControlState.RENAMING)
                .isPresent()) {
            throw new org.pangrova.spi.security.AccessDeniedException(
                    "Table rename is incomplete for " + tableRef
                            + "; wait for completion or reconcile the DDL before querying");
        }
        MetadataResolutionCacheKey cacheKey = MetadataResolutionCacheKey.of(session, tableRef);
        ResolvedTable cached = resolvedTableCache.getIfPresent(cacheKey);
        if (cached != null && isCacheEntryCurrent(tableRef, cached)) {
            LOG.debug("event=resolve_cache_hit catalog={} schema={} table={}",
                    tableRef.catalog(), tableRef.schema(), tableRef.table());
            return cached;
        }
        if (cached != null) {
            resolvedTableCache.invalidate(cacheKey);
        }
        LOG.debug("event=resolve_cache_miss catalog={} schema={} table={}",
                tableRef.catalog(), tableRef.schema(), tableRef.table());
        ResolvedTable resolved = resolveFromCatalog(session, tableRef);
        resolvedTableCache.put(cacheKey, resolved);
        return resolved;
    }

    private boolean isCacheEntryCurrent(QualifiedTableRef tableRef, ResolvedTable cached) {
        var record = catalogRegistry.tableControl()
                .find(tableRef.catalog(), tableRef.schema(), tableRef.table());
        if (cached.ownershipMode() == OwnershipMode.OBSERVED) {
            return record.isEmpty();
        }
        return record.isPresent()
                && cached.tableId().filter(record.get().tableId()::equals).isPresent()
                && record.get().ownershipMode() == cached.ownershipMode()
                && record.get().metadataRevision() == cached.controlRevision()
                && Objects.equals(record.get().versionToken(), cached.versionToken().orElse(null));
    }

    /** Resolves a table directly from the connector, bypassing the local resolution cache. */
    public ResolvedTable resolveFromCatalog(MetadataSessionContext session, QualifiedTableRef tableRef) {
        CatalogHandle handle = catalogRegistry.getCatalog(tableRef.catalog());
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
        var control = catalogRegistry.tableControl()
                .describe(tableRef.catalog(), tableRef.schema(), tableRef.table());
        Optional<String> versionToken = control.versionToken();
        if (control.ownershipMode() == OwnershipMode.OBSERVED) {
            versionToken = connMetadata.observeVersion(connSession, tableHandle)
                    .map(TableVersionToken::token);
        }
        return new ResolvedTable(tableRef, tableHandle, tableMeta, stats,
                control.ownershipMode(), control.tableId(), versionToken, control.metadataRevision());
    }

    /** Resolves the full typed table metadata, translating connector types to API columns. */
    public TableMetadata resolveTableMetadata(MetadataSessionContext session, QualifiedTableRef tableRef) {
        ConnectorTableMetadata metadata = resolveFromCatalog(session, tableRef).tableMetadata();
        List<ColumnMetadata> columns = metadata.typedColumns().stream()
                .map(c -> new ColumnMetadata(c.name(), ColumnType.of(c.type().toString()), true, "", Map.of()))
                .toList();
        return new TableMetadata(tableRef.catalog(), tableRef.schema(), tableRef.table(),
                columns, metadata.properties(), List.of(), Map.of(), null, null);
    }

    public void invalidateTableCaches(QualifiedTableRef tableRef) {
        resolvedTableCache.asMap().keySet().removeIf(key -> key.tableRef().equals(tableRef));
        cache.invalidateTable(tableRef.catalog(), tableRef.schema(), tableRef.table());
    }

    public void invalidateCatalogCaches(String catalogName) {
        resolvedTableCache.asMap().keySet()
                .removeIf(key -> key.tableRef().catalog().equals(catalogName));
        cache.invalidateCatalog(catalogName);
    }
}
