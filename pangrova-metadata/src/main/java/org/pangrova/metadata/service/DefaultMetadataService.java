package org.pangrova.metadata.service;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import org.pangrova.governance.classification.ColumnTagService;
import org.pangrova.metadata.cache.MetadataCache;
import org.pangrova.metadata.catalog.CatalogDescriptor;
import org.pangrova.metadata.catalog.CatalogHandle;
import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.metadata.event.MetadataEvent;
import org.pangrova.metadata.event.MetadataEventDispatcher;
import org.pangrova.metadata.exception.MetadataException;
import org.pangrova.metadata.exception.MetadataResolutionException;
import org.pangrova.metadata.model.ColumnMetadata;
import org.pangrova.metadata.model.ColumnType;
import org.pangrova.metadata.model.TableMetadata;
import org.pangrova.metadata.model.QualifiedTableRef;
import org.pangrova.metadata.schema.SchemaChange;
import org.pangrova.metadata.schema.SchemaEvolutionValidator;
import org.pangrova.metadata.schema.SchemaValidationResult;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.data.type.PangrovaType;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.connector.metadata.table.TableStatistics;
import org.pangrova.spi.connector.metadata.table.TableVersionToken;
import org.pangrova.spi.execution.QueryLifecycleListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Default metadata service: catalog lifecycle, table resolution with caching,
 * schema evolution, and query-level metadata transaction management.
 *
 * <p>Table resolution results are cached in a TTL-based local cache (default 5 minutes)
 * to avoid repeated JDBC round-trips. Cache entries are invalidated on DDL operations
 * (CREATE/ALTER/DROP TABLE) and on catalog lifecycle events.</p>
 *
 * <p>Query-level transactions ensure that all metadata reads within a single
 * query see a consistent snapshot. {@link #beginQuery(MetadataSessionContext)} delegates to
 * each authenticated connector instance, which is retained with the exact issued lifetime
 * until cleanup succeeds. Completed begin and cleanup operations are idempotent.</p>
 *
 * <p>Partial catalog failures are aggregated into {@link QueryLifecycleReport}
 * and exposed via metrics counters. {@code beginQuery} throws on partial failure;
 * failed cleanup remains owned and throws so the actual lifecycle owner can retry.</p>
 */
public class DefaultMetadataService implements MetadataService, QueryLifecycleListener {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultMetadataService.class);
    // F-META-01: a TTL of 0 makes the resolved-table cache permanently inert,
    // forcing every query to re-resolve via JDBC and amplifying coordinator load
    // by 10-100x. Defaults below match the docstring above ("default 5 minutes")
    // and provide a 10K-entry ceiling so memory cannot grow unbounded.
    private static final long DEFAULT_RESOLUTION_CACHE_MAX_SIZE = 10_000;
    private static final Duration DEFAULT_RESOLUTION_CACHE_TTL = Duration.ofMinutes(5);
    // OBSERVED tables have no persisted version to compare against, so a cached
    // resolution can only be bounded by time. A controlled entry is revalidated
    // against the registry on every hit instead.
    private static final Duration OBSERVED_RESOLUTION_CACHE_TTL = Duration.ofSeconds(30);

    private final CatalogRegistry catalogRegistry;
    private final MetadataCache cache;
    private final MetadataEventDispatcher eventDispatcher;
    private final SchemaEvolutionValidator schemaValidator;
    private final Cache<MetadataResolutionCacheKey, ResolvedTable> resolvedTableCache;
    private final MetadataQueryLifecycles queryLifecycles;
    private final MetadataTagOperations tagOperations = new MetadataTagOperations();

    /** Guards alterTable against concurrent schema modifications on the same table. */
    private final Object alterTableLock = new Object();

    private final AtomicLong beginQuerySuccessTotal = new AtomicLong();
    private final AtomicLong beginQueryFailureTotal = new AtomicLong();
    private final AtomicLong cleanupQuerySuccessTotal = new AtomicLong();
    private final AtomicLong cleanupQueryFailureTotal = new AtomicLong();

    public DefaultMetadataService(
            CatalogRegistry catalogRegistry,
            MetadataCache cache,
            MetadataEventDispatcher eventDispatcher,
            SchemaEvolutionValidator schemaValidator) {
        this(catalogRegistry, cache, eventDispatcher, schemaValidator,
                DEFAULT_RESOLUTION_CACHE_MAX_SIZE, DEFAULT_RESOLUTION_CACHE_TTL);
    }

    /** Creates the service with custom resolution-cache size and TTL bounds. */
    public DefaultMetadataService(
            CatalogRegistry catalogRegistry,
            MetadataCache cache,
            MetadataEventDispatcher eventDispatcher,
            SchemaEvolutionValidator schemaValidator,
            long resolutionCacheMaxSize,
            Duration resolutionCacheTtl) {
        this.catalogRegistry = catalogRegistry;
        this.queryLifecycles = new MetadataQueryLifecycles(catalogRegistry);
        this.cache = cache;
        this.eventDispatcher = eventDispatcher;
        this.schemaValidator = schemaValidator;
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
                                == org.pangrova.metadata.control.OwnershipMode.OBSERVED
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

    /**
     * Injects a {@link ColumnTagService} so {@link #setTag} and {@link #unsetTag}
     * emit {@code TagChangedEvent} to downstream governance listeners.
     * Tag persistence is owned by the installed {@link org.pangrova.metadata.tag.ColumnTagStore}.
     */
    public void setColumnTagService(ColumnTagService columnTagService) {
        tagOperations.setColumnTagService(columnTagService);
    }

    /**
     * Re-emits every persisted tag after wiring the governance tag service, so
     * derived policy (for example auto-masks) is rebuilt after a restart.
     *
     * @return the number of tag entries replayed
     */
    public int replayColumnTags() {
        return tagOperations.replayAll();
    }

    /**
     * Installs a persistent {@link org.pangrova.metadata.tag.ColumnTagStore}
     * (e.g. JDBC-backed) so column tags survive coordinator restarts. Defaults to
     * an in-memory store. Wired by the coordinator when a metadata datasource is
     * configured.
     */
    public void setColumnTagStore(org.pangrova.metadata.tag.ColumnTagStore store) {
        tagOperations.setColumnTagStore(store);
    }

    @Override
    public void setTag(MetadataSessionContext session, QualifiedTableRef tableRef,
                       String columnName, String tagKey, String value) {
        tagOperations.setTag(tableRef, columnName, tagKey, value);
    }

    @Override
    public void unsetTag(MetadataSessionContext session, QualifiedTableRef tableRef,
                         String columnName, String tagKey) {
        tagOperations.unsetTag(tableRef, columnName, tagKey);
    }

    @Override
    public Map<String, String> listTags(MetadataSessionContext session,
                                        QualifiedTableRef tableRef, String columnName) {
        return tagOperations.listTags(tableRef, columnName);
    }

    @Override
    public int clearTableTags(MetadataSessionContext session, QualifiedTableRef tableRef) {
        return tagOperations.clearTable(tableRef);
    }

    @Override
    public int renameTableTags(MetadataSessionContext session, QualifiedTableRef from,
            QualifiedTableRef to) {
        return tagOperations.renameTable(from, to);
    }

    @Override
    public void createCatalog(MetadataSessionContext session, CatalogDescriptor descriptor) {
        catalogRegistry.register(descriptor);
    }

    @Override
    public void dropCatalog(MetadataSessionContext session, String catalogName, boolean cascade) {
        catalogRegistry.unregister(catalogName);
        invalidateCatalogCaches(catalogName);
    }

    @Override
    public List<String> listCatalogs(MetadataSessionContext session) {
        return catalogRegistry.listCatalogNames();
    }

    @Override
    public java.util.Optional<CatalogDescriptor> getCatalogDescriptor(String catalogName) {
        return java.util.Optional.of(catalogRegistry.getCatalog(catalogName).getDescriptor());
    }

    @Override
    public java.util.Optional<CatalogDescriptor> findCatalogDescriptorRevision(
            String catalogName, String incarnationId, long revision) {
        return catalogRegistry.findCatalogDescriptorRevision(catalogName, incarnationId, revision);
    }

    @Override
    public List<String> listSchemas(MetadataSessionContext session, String catalogName) {
        CatalogHandle handle = catalogRegistry.getCatalog(catalogName);
        ConnectorSession connSession = DefaultConnectorSession.fromSessionContext(session, catalogName, null);
        ConnectorMetadata metadata = handle.getMetadata(connSession);
        return metadata.listSchemaNames(connSession);
    }

    @Override
    public ResolvedTable resolveTable(MetadataSessionContext session, QualifiedTableRef tableRef) {
        var control = catalogRegistry.tableControl();
        if (control.enabled()) {
            control.find(tableRef.catalog(), tableRef.schema(), tableRef.table())
                    .ifPresent(record -> control.requireCurrentCatalog(record, "Table resolution"));
        }
        if (control.enabled() && control.find(tableRef.catalog(), tableRef.schema(), tableRef.table())
                .filter(record -> record.state() == org.pangrova.metadata.control.TableControlState.RENAMING)
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

    /**
     * Reuse a resolution only while its ownership, stable table id, revision and
     * version still match the authority. ADOPT and same-name replacement must
     * invalidate cached entries even when the numeric revision is unchanged.
     */
    private boolean isCacheEntryCurrent(QualifiedTableRef tableRef, ResolvedTable cached) {
        var record = catalogRegistry.tableControl()
                .find(tableRef.catalog(), tableRef.schema(), tableRef.table());
        if (cached.ownershipMode() == org.pangrova.metadata.control.OwnershipMode.OBSERVED) {
            return record.isEmpty();
        }
        return record.isPresent()
                && cached.tableId().filter(record.get().tableId()::equals).isPresent()
                && record.get().ownershipMode() == cached.ownershipMode()
                && record.get().metadataRevision() == cached.controlRevision()
                && java.util.Objects.equals(
                        record.get().versionToken(), cached.versionToken().orElse(null));
    }

    @Override
    public void createTable(MetadataSessionContext session, QualifiedTableRef tableRef, TableMetadata metadata) {
        CatalogHandle catalogHandle = catalogRegistry.getCatalog(tableRef.catalog());
        ConnectorSession connSession = DefaultConnectorSession.fromSessionContext(
                session, tableRef.catalog(), tableRef.schema());
        ConnectorMetadata connMetadata = catalogHandle.getMetadata(connSession);
        SchemaTableName schemaTable = new SchemaTableName(tableRef.schema(), tableRef.table());
        List<String> columnNames = metadata.columns().stream().map(ColumnMetadata::name).toList();
        ConnectorTableMetadata connTableMeta = new ConnectorTableMetadata(
                schemaTable, columnNames, metadata.properties());
        connMetadata.createTable(connSession, connTableMeta);
        registerCreatedTable(connMetadata, connSession, tableRef, session.user());
        invalidateTableCaches(tableRef);
        eventDispatcher.dispatch(new MetadataEvent.TableCreatedEvent(tableRef, metadata));
        LOG.info("event=table_created catalog={} schema={} table={}",
                tableRef.catalog(), tableRef.schema(), tableRef.table());
    }

    @Override
    public void dropTable(MetadataSessionContext session, QualifiedTableRef tableRef) {
        catalogRegistry.tableControl().requireDroppable(
                tableRef.catalog(), tableRef.schema(), tableRef.table(), "DROP TABLE");
        CatalogHandle catalogHandle = catalogRegistry.getCatalog(tableRef.catalog());
        ConnectorSession connSession = DefaultConnectorSession.fromSessionContext(
                session, tableRef.catalog(), tableRef.schema());
        ConnectorMetadata connMetadata = catalogHandle.getMetadata(connSession);
        SchemaTableName schemaTable = new SchemaTableName(tableRef.schema(), tableRef.table());
        ConnectorTableHandle tableHandle = connMetadata.getTableHandle(connSession, schemaTable);
        if (tableHandle == null) {
            throw new MetadataException(
                    PangrovaErrorCode.METADATA_TABLE_NOT_FOUND,
                    "Table not found: " + tableRef);
        }
        if (catalogRegistry.tableControl().enforcing()) {
            catalogRegistry.tableControl().requireDroppable(
                    tableRef.catalog(), tableRef.schema(), tableRef.table(), "DROP TABLE",
                    connMetadata.getTableIdentity(connSession, tableHandle));
        }
        var record = catalogRegistry.tableControl().find(
                tableRef.catalog(), tableRef.schema(), tableRef.table());
        connMetadata.dropTable(connSession, tableHandle);
        record.ifPresent(catalogRegistry.tableControl()::markDropped);
        invalidateTableCaches(tableRef);
        eventDispatcher.dispatch(new MetadataEvent.TableDroppedEvent(tableRef));
        LOG.info("event=table_dropped catalog={} schema={} table={}",
                tableRef.catalog(), tableRef.schema(), tableRef.table());
    }

    @Override
    public void alterTable(MetadataSessionContext session, QualifiedTableRef tableRef, List<SchemaChange> changes) {
        synchronized (alterTableLock) {
            catalogRegistry.tableControl().requireManagedForDdl(
                    tableRef.catalog(), tableRef.schema(), tableRef.table(), "ALTER TABLE");
            TableMetadata current = resolveTableMetadata(session, tableRef);
            validateSchemaChanges(current, changes);
            applySchemaChangesToConnector(session, tableRef, changes);
            // Re-read connector authority, not the unscoped inventory projection.
            resolveTableMetadata(session, tableRef);
            invalidateTableCaches(tableRef);
            eventDispatcher.dispatch(new MetadataEvent.TableAlteredEvent(tableRef, changes));
        }
    }

    private void registerCreatedTable(ConnectorMetadata metadata, ConnectorSession connSession,
            QualifiedTableRef tableRef, String owner) {
        var tableControl = catalogRegistry.tableControl();
        if (!tableControl.enabled()) {
            return;
        }
        try {
            tableControl.registerCreatedFromConnector(metadata, connSession,
                    tableRef.catalog(), tableRef.schema(), tableRef.table(), owner);
        } catch (RuntimeException failure) {
            tableControl.registerOrphanedManaged(tableRef.catalog(), tableRef.schema(),
                    tableRef.table(), owner, java.util.Optional.empty());
            throw new MetadataException(PangrovaErrorCode.METADATA_METASTORE_ERROR,
                    "Table was created but its control record could not be registered: " + tableRef,
                    failure);
        }
    }

    private void applySchemaChangesToConnector(
            MetadataSessionContext session, QualifiedTableRef tableRef, List<SchemaChange> changes) {
        CatalogHandle catalogHandle = catalogRegistry.getCatalog(tableRef.catalog());
        ConnectorSession connSession = DefaultConnectorSession.fromSessionContext(
                session, tableRef.catalog(), tableRef.schema());
        ConnectorMetadata connMetadata = catalogHandle.getMetadata(connSession);
        SchemaTableName schemaTable = new SchemaTableName(tableRef.schema(), tableRef.table());
        ConnectorTableHandle tableHandle = connMetadata.getTableHandle(connSession, schemaTable);
        if (tableHandle == null) {
            throw new MetadataException(
                    PangrovaErrorCode.METADATA_TABLE_NOT_FOUND,
                    "Table not found: " + tableRef);
        }
        for (SchemaChange change : changes) {
            if (catalogRegistry.tableControl().enforcing()) {
                catalogRegistry.tableControl().requireManagedForDdl(
                        tableRef.catalog(), tableRef.schema(), tableRef.table(), "ALTER TABLE",
                        connMetadata.getTableIdentity(connSession, tableHandle));
            }
            applySchemaChange(connMetadata, connSession, tableHandle, change);
        }
        LOG.info("event=alter_table_connector_propagated catalog={} schema={} table={} changes={}",
                tableRef.catalog(), tableRef.schema(), tableRef.table(), changes.size());
    }

    private static void applySchemaChange(
            ConnectorMetadata metadata, ConnectorSession session,
            ConnectorTableHandle table, SchemaChange change) {
        if (change instanceof SchemaChange.AddColumn add) {
            metadata.addColumn(session, table, add.name(), PangrovaType.fromSqlTypeName(add.type().baseType()));
        } else if (change instanceof SchemaChange.DropColumn drop) {
            metadata.dropColumn(session, table, drop.name());
        } else if (change instanceof SchemaChange.RenameColumn rename) {
            metadata.renameColumn(session, table, rename.oldName(), rename.newName());
        } else if (change instanceof SchemaChange.ChangeType changeType) {
            metadata.changeColumnType(session, table, changeType.name(),
                    PangrovaType.fromSqlTypeName(changeType.newType().baseType()));
        }
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

    @Override
    public void beginQuery(MetadataSessionContext session) {
        QueryLifecycleReport report = queryLifecycles.begin(session);
        recordBeginQueryMetrics(report);
        if (report.hasFailures()) {
            throw new MetadataResolutionException(session.queryId(), "beginQuery", report.failedCatalogs());
        }
        LOG.debug("beginQuery completed for queryId={}", session.queryId());
    }

    @Override
    public void cleanupQuery(org.pangrova.spi.connector.ConnectorQueryAuthority authority) {
        QueryLifecycleReport report = queryLifecycles.cleanup(authority);
        recordCleanupQueryMetrics(report);
        if (report.hasFailures()) {
            throw new MetadataResolutionException(authority.queryId(), "cleanupQuery", report.failedCatalogs());
        }
        LOG.debug("cleanupQuery completed for queryId={}", authority.queryId());
    }

    public long beginQuerySuccessTotal() {
        return beginQuerySuccessTotal.get();
    }

    public long beginQueryFailureTotal() {
        return beginQueryFailureTotal.get();
    }

    public long cleanupQuerySuccessTotal() {
        return cleanupQuerySuccessTotal.get();
    }

    public long cleanupQueryFailureTotal() {
        return cleanupQueryFailureTotal.get();
    }

    @Override
    public void onQueryStarted(String queryId, String sql) {
        LOG.debug("event=query_lifecycle_started queryId={}", queryId);
    }

    @Override
    public void onQueryCompleted(String queryId) {
        onQueryEnd(queryId);
        LOG.debug("event=query_lifecycle_completed queryId={}", queryId);
    }

    @Override
    public void onQueryFailed(String queryId, Throwable error) {
        onQueryEnd(queryId);
        LOG.debug("event=query_lifecycle_failed queryId={}", queryId);
    }

    /**
     * Looks up the actual retained lifetime; a listener cannot issue a new query authority.
     */
    @Override
    public void onQueryEnd(String queryId) {
        var authority = queryLifecycles.retainedAuthority(queryId);
        if (authority != null) cleanupQuery(authority);
    }

    /**
     * Returns the number of currently tracked active queries.
     * Exposed for monitoring and testing purposes.
     */
    public int activeQueryCount() {
        return queryLifecycles.size();
    }


    private void recordBeginQueryMetrics(QueryLifecycleReport report) {
        beginQuerySuccessTotal.addAndGet(report.succeededCount());
        beginQueryFailureTotal.addAndGet(report.failedCount());
    }

    private void recordCleanupQueryMetrics(QueryLifecycleReport report) {
        cleanupQuerySuccessTotal.addAndGet(report.succeededCount());
        cleanupQueryFailureTotal.addAndGet(report.failedCount());
    }

    private ResolvedTable resolveFromCatalog(MetadataSessionContext session, QualifiedTableRef tableRef) {
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
        // A controlled table's version is the registry's value: that is what
        // admission and cache revalidation compare against. An OBSERVED table has
        // no persisted version, so capture the live one for diagnostics while the
        // short TTL keeps the cached resolution honest.
        java.util.Optional<String> versionToken = control.versionToken();
        if (control.ownershipMode() == org.pangrova.metadata.control.OwnershipMode.OBSERVED) {
            versionToken = connMetadata.observeVersion(connSession, tableHandle)
                    .map(TableVersionToken::token);
        }
        return new ResolvedTable(tableRef, tableHandle, tableMeta, stats,
                control.ownershipMode(), control.tableId(), versionToken,
                control.metadataRevision());
    }

    private TableMetadata resolveTableMetadata(MetadataSessionContext session, QualifiedTableRef tableRef) {
        // The tiered store has no authenticated scope. Never use it as authority
        // for tenant-specific DDL validation, or to read back an ALTER result.
        return toTableMetadata(tableRef, resolveFromCatalog(session, tableRef).tableMetadata());
    }

    private static TableMetadata toTableMetadata(QualifiedTableRef ref, ConnectorTableMetadata connMeta) {
        List<ColumnMetadata> columns = connMeta.typedColumns().stream()
                .map(c -> new ColumnMetadata(c.name(), ColumnType.of(c.type().toString()), true, "", Map.of()))
                .toList();
        return new TableMetadata(
                ref.catalog(), ref.schema(), ref.table(),
                columns, connMeta.properties(), List.of(), Map.of(), null, null);
    }

    private void validateSchemaChanges(TableMetadata current, List<SchemaChange> changes) {
        for (SchemaChange change : changes) {
            if (change instanceof SchemaChange.AddColumn add && !add.nullable()) {
                throw new MetadataException(
                        PangrovaErrorCode.METADATA_SCHEMA_INCOMPATIBLE,
                        "Adding a NOT NULL column is not supported by the connector metadata SPI: " + add.name());
            }
            if (change instanceof SchemaChange.ChangeNullability nullability) {
                throw new MetadataException(
                        PangrovaErrorCode.METADATA_SCHEMA_INCOMPATIBLE,
                        "Changing column nullability is not supported by the connector metadata SPI: "
                                + nullability.name());
            }
        }
        SchemaValidationResult result = schemaValidator.validate(current, changes);
        if (!result.compatible()) {
            throw new MetadataException(
                    PangrovaErrorCode.METADATA_SCHEMA_INCOMPATIBLE,
                    "Schema changes incompatible: " + result.errors());
        }
    }
}
