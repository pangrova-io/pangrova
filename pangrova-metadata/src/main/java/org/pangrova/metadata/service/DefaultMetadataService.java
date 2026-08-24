package org.pangrova.metadata.service;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
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
import org.pangrova.spi.execution.QueryLifecycleListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
 * query see a consistent snapshot. {@link #beginQuery(String)} delegates to
 * each registered connector's {@code beginQuery}, and {@link #cleanupQuery(String)}
 * releases pinned resources. Both operations are idempotent.</p>
 *
 * <p>Partial catalog failures are aggregated into {@link QueryLifecycleReport}
 * and exposed via metrics counters. {@code beginQuery} throws on partial failure;
 * {@code cleanupQuery} logs but does not throw (best-effort cleanup).</p>
 */
public class DefaultMetadataService implements MetadataService, QueryLifecycleListener {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultMetadataService.class);
    // F-META-01: a TTL of 0 makes the resolved-table cache permanently inert,
    // forcing every query to re-resolve via JDBC and amplifying coordinator load
    // by 10-100x. Defaults below match the docstring above ("default 5 minutes")
    // and provide a 10K-entry ceiling so memory cannot grow unbounded.
    private static final long DEFAULT_RESOLUTION_CACHE_MAX_SIZE = 10_000;
    private static final Duration DEFAULT_RESOLUTION_CACHE_TTL = Duration.ofMinutes(5);

    private final CatalogRegistry catalogRegistry;
    private final MetadataCache cache;
    private final MetadataEventDispatcher eventDispatcher;
    private final SchemaEvolutionValidator schemaValidator;
    private final Cache<QualifiedTableRef, ResolvedTable> resolvedTableCache;
    private final Set<String> activeQueries = ConcurrentHashMap.newKeySet();
    private volatile ColumnTagService columnTagService;
    /** Pluggable column tag store: in-memory by default, JDBC-backed in production. */
    private volatile org.pangrova.metadata.tag.ColumnTagStore tagStore =
            new org.pangrova.metadata.tag.InMemoryColumnTagStore();

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
        this.cache = cache;
        this.eventDispatcher = eventDispatcher;
        this.schemaValidator = schemaValidator;
        this.resolvedTableCache = Caffeine.newBuilder()
                .maximumSize(resolutionCacheMaxSize)
                .expireAfterWrite(resolutionCacheTtl)
                .recordStats()
                .build();
    }

    /**
     * Injects a {@link ColumnTagService} so {@link #setTag} and {@link #unsetTag}
     * emit {@code TagChangedEvent} to downstream governance listeners.
     * Tag persistence remains local to this service (in-memory store).
     */
    public void setColumnTagService(ColumnTagService columnTagService) {
        this.columnTagService = columnTagService;
    }

    /**
     * Installs a persistent {@link org.pangrova.metadata.tag.ColumnTagStore}
     * (e.g. JDBC-backed) so column tags survive coordinator restarts. Defaults to
     * an in-memory store. Wired by the coordinator when a metadata datasource is
     * configured.
     */
    public void setColumnTagStore(org.pangrova.metadata.tag.ColumnTagStore store) {
        this.tagStore = java.util.Objects.requireNonNull(store, "store");
        LOG.info("event=column_tag_store_installed type={}", store.getClass().getSimpleName());
    }

    @Override
    public void setTag(MetadataSessionContext session, QualifiedTableRef tableRef,
                       String columnName, String tagKey, String value) {
        java.util.Objects.requireNonNull(tableRef, "tableRef");
        java.util.Objects.requireNonNull(columnName, "columnName");
        java.util.Objects.requireNonNull(tagKey, "tagKey");
        java.util.Objects.requireNonNull(value, "value");
        String storeKey = tagStoreKey(tableRef, columnName);
        String oldValue = tagStore.put(storeKey, tagKey, value);
        ColumnTagService svc = columnTagService;
        if (svc != null) {
            svc.assignTag(tableRef.catalog(), tableRef.schema(), tableRef.table(),
                    columnName, tagKey, oldValue, value);
        }
        LOG.info("event=column_tag_set catalog={} schema={} table={} column={} tagKey={}",
                tableRef.catalog(), tableRef.schema(), tableRef.table(), columnName, tagKey);
    }

    @Override
    public void unsetTag(MetadataSessionContext session, QualifiedTableRef tableRef,
                         String columnName, String tagKey) {
        java.util.Objects.requireNonNull(tableRef, "tableRef");
        java.util.Objects.requireNonNull(columnName, "columnName");
        java.util.Objects.requireNonNull(tagKey, "tagKey");
        String storeKey = tagStoreKey(tableRef, columnName);
        String removed = tagStore.remove(storeKey, tagKey);
        if (removed == null) {
            return;
        }
        ColumnTagService svc = columnTagService;
        if (svc != null) {
            svc.removeTag(tableRef.catalog(), tableRef.schema(), tableRef.table(),
                    columnName, tagKey, removed);
        }
        LOG.info("event=column_tag_unset catalog={} schema={} table={} column={} tagKey={}",
                tableRef.catalog(), tableRef.schema(), tableRef.table(), columnName, tagKey);
    }

    @Override
    public Map<String, String> listTags(MetadataSessionContext session,
                                        QualifiedTableRef tableRef, String columnName) {
        java.util.Objects.requireNonNull(tableRef, "tableRef");
        java.util.Objects.requireNonNull(columnName, "columnName");
        return tagStore.list(tagStoreKey(tableRef, columnName));
    }

    private static String tagStoreKey(QualifiedTableRef ref, String columnName) {
        return ref.catalog() + "|" + ref.schema() + "|" + ref.table() + "|" + columnName;
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
    public List<String> listSchemas(MetadataSessionContext session, String catalogName) {
        CatalogHandle handle = catalogRegistry.getCatalog(catalogName);
        ConnectorSession connSession = DefaultConnectorSession.fromSessionContext(session);
        ConnectorMetadata metadata = handle.getMetadata(connSession);
        return metadata.listSchemaNames(connSession);
    }

    @Override
    public ResolvedTable resolveTable(MetadataSessionContext session, QualifiedTableRef tableRef) {
        ResolvedTable cached = resolvedTableCache.getIfPresent(tableRef);
        if (cached != null) {
            LOG.debug("event=resolve_cache_hit catalog={} schema={} table={}",
                    tableRef.catalog(), tableRef.schema(), tableRef.table());
            return cached;
        }
        LOG.debug("event=resolve_cache_miss catalog={} schema={} table={}",
                tableRef.catalog(), tableRef.schema(), tableRef.table());
        ResolvedTable resolved = resolveFromCatalog(session, tableRef);
        resolvedTableCache.put(tableRef, resolved);
        TableMetadata metadata = toTableMetadata(tableRef, resolved.tableMetadata());
        cache.putTable(tableRef.catalog(), tableRef.schema(), tableRef.table(), metadata);
        return resolved;
    }

    @Override
    public void createTable(MetadataSessionContext session, QualifiedTableRef tableRef, TableMetadata metadata) {
        CatalogHandle catalogHandle = catalogRegistry.getCatalog(tableRef.catalog());
        ConnectorSession connSession = DefaultConnectorSession.fromSessionContext(session);
        ConnectorMetadata connMetadata = catalogHandle.getMetadata(connSession);
        SchemaTableName schemaTable = new SchemaTableName(tableRef.schema(), tableRef.table());
        List<String> columnNames = metadata.columns().stream().map(ColumnMetadata::name).toList();
        ConnectorTableMetadata connTableMeta = new ConnectorTableMetadata(
                schemaTable, columnNames, metadata.properties());
        connMetadata.createTable(connSession, connTableMeta);
        cache.putTable(tableRef.catalog(), tableRef.schema(), tableRef.table(), metadata);
        eventDispatcher.dispatch(new MetadataEvent.TableCreatedEvent(tableRef, metadata));
        LOG.info("event=table_created catalog={} schema={} table={}",
                tableRef.catalog(), tableRef.schema(), tableRef.table());
    }

    @Override
    public void dropTable(MetadataSessionContext session, QualifiedTableRef tableRef) {
        CatalogHandle catalogHandle = catalogRegistry.getCatalog(tableRef.catalog());
        ConnectorSession connSession = DefaultConnectorSession.fromSessionContext(session);
        ConnectorMetadata connMetadata = catalogHandle.getMetadata(connSession);
        SchemaTableName schemaTable = new SchemaTableName(tableRef.schema(), tableRef.table());
        ConnectorTableHandle tableHandle = connMetadata.getTableHandle(connSession, schemaTable);
        if (tableHandle == null) {
            throw new MetadataException(
                    PangrovaErrorCode.METADATA_TABLE_NOT_FOUND,
                    "Table not found: " + tableRef);
        }
        connMetadata.dropTable(connSession, tableHandle);
        invalidateTableCaches(tableRef);
        eventDispatcher.dispatch(new MetadataEvent.TableDroppedEvent(tableRef));
        LOG.info("event=table_dropped catalog={} schema={} table={}",
                tableRef.catalog(), tableRef.schema(), tableRef.table());
    }

    @Override
    public void alterTable(MetadataSessionContext session, QualifiedTableRef tableRef, List<SchemaChange> changes) {
        synchronized (alterTableLock) {
            TableMetadata current = resolveTableMetadata(session, tableRef);
            validateSchemaChanges(current, changes);
            applySchemaChangesToConnector(session, tableRef, changes);
            // F-META-05: Re-resolve and persist updated metadata so offline catalog reads
            // return the post-alter schema after restart.
            TableMetadata updatedMetadata = resolveTableMetadata(session, tableRef);
            cache.putTable(tableRef.catalog(), tableRef.schema(), tableRef.table(), updatedMetadata);
            invalidateTableCaches(tableRef);
            eventDispatcher.dispatch(new MetadataEvent.TableAlteredEvent(tableRef, changes));
        }
    }

    private void applySchemaChangesToConnector(
            MetadataSessionContext session, QualifiedTableRef tableRef, List<SchemaChange> changes) {
        CatalogHandle catalogHandle = catalogRegistry.getCatalog(tableRef.catalog());
        ConnectorSession connSession = DefaultConnectorSession.fromSessionContext(session);
        ConnectorMetadata connMetadata = catalogHandle.getMetadata(connSession);
        SchemaTableName schemaTable = new SchemaTableName(tableRef.schema(), tableRef.table());
        ConnectorTableHandle tableHandle = connMetadata.getTableHandle(connSession, schemaTable);
        if (tableHandle == null) {
            throw new MetadataException(
                    PangrovaErrorCode.METADATA_TABLE_NOT_FOUND,
                    "Table not found: " + tableRef);
        }
        for (SchemaChange change : changes) {
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
        } else if (change instanceof SchemaChange.ChangeNullability) {
            LOG.debug("event=change_nullability_skipped reason=not_supported_by_connector_spi");
        }
    }

    /**
     * Invalidates all cached metadata for the given table across both the resolution
     * cache and the tiered metadata cache. Called on DDL operations.
     */
    public void invalidateTableCaches(QualifiedTableRef tableRef) {
        resolvedTableCache.invalidate(tableRef);
        cache.invalidateTable(tableRef.catalog(), tableRef.schema(), tableRef.table());
    }

    /**
     * Invalidates all cached metadata for the given catalog. Called on catalog-level DDL.
     */
    public void invalidateCatalogCaches(String catalogName) {
        resolvedTableCache.asMap().keySet()
                .removeIf(ref -> ref.catalog().equals(catalogName));
        cache.invalidateCatalog(catalogName);
    }

    @Override
    public void beginQuery(String queryId) {
        if (!activeQueries.add(queryId)) {
            LOG.debug("beginQuery already called for queryId={}, idempotent no-op", queryId);
            return;
        }
        QueryLifecycleReport report = executeLifecycleAcrossCatalogs(
                queryId, QueryLifecycleReport.Operation.BEGIN);
        recordBeginQueryMetrics(report);
        if (report.hasFailures()) {
            throw new MetadataResolutionException(queryId, "beginQuery", report.failedCatalogs());
        }
        LOG.debug("beginQuery completed for queryId={}", queryId);
    }

    @Override
    public void cleanupQuery(String queryId) {
        if (!activeQueries.remove(queryId)) {
            LOG.debug("cleanupQuery called for unknown/already-cleaned queryId={}, idempotent no-op", queryId);
            return;
        }
        QueryLifecycleReport report = executeLifecycleAcrossCatalogs(
                queryId, QueryLifecycleReport.Operation.CLEANUP);
        recordCleanupQueryMetrics(report);
        if (report.hasFailures()) {
            LOG.warn("cleanupQuery partial failure, queryId={}, failedCatalogs={}, succeededCatalogs={}",
                    queryId, report.failedCatalogs().keySet(), report.succeededCatalogs());
        }
        LOG.debug("cleanupQuery completed for queryId={}", queryId);
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
     * Removes the query from activeQueries on any termination path (success, failure,
     * or abnormal termination). This prevents the Set from growing unboundedly when
     * queries terminate without calling {@link #cleanupQuery(String)}.
     */
    @Override
    public void onQueryEnd(String queryId) {
        if (activeQueries.remove(queryId)) {
            LOG.debug("event=active_query_removed_on_end queryId={}", queryId);
            executeLifecycleAcrossCatalogs(queryId, QueryLifecycleReport.Operation.CLEANUP);
        }
    }

    /**
     * Returns the number of currently tracked active queries.
     * Exposed for monitoring and testing purposes.
     */
    public int activeQueryCount() {
        return activeQueries.size();
    }

    private QueryLifecycleReport executeLifecycleAcrossCatalogs(String queryId, QueryLifecycleReport.Operation op) {
        ConnectorSession session = DefaultConnectorSession.forQueryLifecycle(queryId);
        List<String> succeeded = new ArrayList<>();
        Map<String, Exception> failed = new LinkedHashMap<>();
        for (String catalogName : catalogRegistry.listCatalogNames()) {
            Optional<Exception> error = executeForCatalog(catalogName, queryId, session, op);
            if (error.isPresent()) {
                failed.put(catalogName, error.get());
            } else {
                succeeded.add(catalogName);
            }
        }
        return new QueryLifecycleReport(queryId, op, succeeded, failed);
    }

    private Optional<Exception> executeForCatalog(
            String catalogName, String queryId, ConnectorSession session, QueryLifecycleReport.Operation op) {
        try {
            CatalogHandle handle = catalogRegistry.getCatalog(catalogName);
            ConnectorMetadata metadata = handle.getMetadata(session);
            if (op == QueryLifecycleReport.Operation.BEGIN) {
                metadata.beginQuery(queryId);
            } else {
                metadata.cleanupQuery(queryId);
            }
            return Optional.empty();
        } catch (Exception e) {
            LOG.warn("{} failed for catalog={}, queryId={}", op, catalogName, queryId, e);
            return Optional.of(e);
        }
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
        ConnectorSession connSession = DefaultConnectorSession.fromSessionContext(session);
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
        return new ResolvedTable(tableRef, tableHandle, tableMeta, stats);
    }

    private TableMetadata resolveTableMetadata(MetadataSessionContext session, QualifiedTableRef tableRef) {
        return cache.getTable(tableRef.catalog(), tableRef.schema(), tableRef.table())
                .orElseGet(() -> toTableMetadata(tableRef, resolveFromCatalog(session, tableRef).tableMetadata()));
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
        SchemaValidationResult result = schemaValidator.validate(current, changes);
        if (!result.compatible()) {
            throw new MetadataException(
                    PangrovaErrorCode.METADATA_SCHEMA_INCOMPATIBLE,
                    "Schema changes incompatible: " + result.errors());
        }
    }
}
