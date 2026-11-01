package org.pangrova.metadata.core;

import org.pangrova.metadata.api.session.MetadataSessionContext;
import org.pangrova.metadata.api.binding.QueryMetadataLifecyclePort;
import org.pangrova.metadata.api.binding.ResolvedTable;
import org.pangrova.metadata.api.binding.TableResolutionPort;
import org.pangrova.metadata.api.catalog.CatalogLifecyclePort;
import org.pangrova.metadata.api.control.ColumnTagPort;
import org.pangrova.metadata.api.mutation.TableDdlPort;

import org.pangrova.governance.classification.ColumnTagService;
import org.pangrova.metadata.core.cache.MetadataCache;
import org.pangrova.metadata.core.resolution.TableResolutionOperations;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.core.catalog.CatalogHandle;
import org.pangrova.metadata.core.catalog.CatalogRegistry;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.metadata.core.event.MetadataEvent;
import org.pangrova.metadata.core.event.MetadataEventDispatcher;
import org.pangrova.metadata.api.error.MetadataException;
import org.pangrova.metadata.api.error.MetadataResolutionException;
import org.pangrova.metadata.api.schema.ColumnMetadata;
import org.pangrova.metadata.api.schema.TableMetadata;
import org.pangrova.metadata.api.object.QualifiedTableRef;
import org.pangrova.metadata.api.schema.SchemaChange;
import org.pangrova.metadata.core.schema.SchemaEvolutionValidator;
import org.pangrova.metadata.core.schema.SchemaValidationResult;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.data.type.PangrovaType;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
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
public class DefaultMetadataService implements CatalogLifecyclePort, TableDdlPort,
        TableResolutionPort, QueryMetadataLifecyclePort, ColumnTagPort, QueryLifecycleListener {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultMetadataService.class);
    private static final long DEFAULT_RESOLUTION_CACHE_MAX_SIZE = 10_000L;
    private static final Duration DEFAULT_RESOLUTION_CACHE_TTL = Duration.ofMinutes(5);
    private final CatalogRegistry catalogRegistry;
    private final TableResolutionOperations resolution;
    private final MetadataEventDispatcher eventDispatcher;
    private final SchemaEvolutionValidator schemaValidator;
    private final MetadataQueryLifecycles queryLifecycles;
    private final MetadataTagOperations tagOperations;

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
        this.tagOperations = new MetadataTagOperations(catalogRegistry);
        this.queryLifecycles = new MetadataQueryLifecycles(catalogRegistry);
        this.resolution = new TableResolutionOperations(
                catalogRegistry, cache, resolutionCacheMaxSize, resolutionCacheTtl);
        this.eventDispatcher = eventDispatcher;
        this.schemaValidator = schemaValidator;
    }

    /**
     * Injects a {@link ColumnTagService} so {@link #setTag} and {@link #unsetTag}
     * emit {@code TagChangedEvent} to downstream governance listeners.
     * Tag persistence is owned by the registry's stable-identity governance bindings.
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

    @Override
    public void setTag(MetadataSessionContext session, QualifiedTableRef tableRef,
                       String columnName, String tagKey, String value) {
        var target = MetadataColumnTagTarget.resolve(catalogRegistry, session, tableRef, columnName);
        target.requireTagType(tagKey);
        tagOperations.setTag(target.owner(), target.column(), tagKey, value, session.user());
    }

    @Override
    public void unsetTag(MetadataSessionContext session, QualifiedTableRef tableRef,
                         String columnName, String tagKey) {
        var target = MetadataColumnTagTarget.resolve(catalogRegistry, session, tableRef, columnName);
        tagOperations.unsetTag(target.owner(), target.column(), tagKey);
    }

    @Override
    public Map<String, String> listTags(MetadataSessionContext session,
                                        QualifiedTableRef tableRef, String columnName) {
        var target = MetadataColumnTagTarget.resolve(catalogRegistry, session, tableRef, columnName);
        return tagOperations.listTags(target.owner(), target.column());
    }

    @Override
    public int clearTableTags(MetadataSessionContext session, java.util.UUID tableId) {
        return tagOperations.clearTable(tableId);
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
        return resolution.listSchemas(session, catalogName);
    }

    @Override
    public ResolvedTable resolveTable(MetadataSessionContext session, QualifiedTableRef tableRef) {
        return resolution.resolveTable(session, tableRef);
    }

    @Override
    public void createTable(MetadataSessionContext session, QualifiedTableRef tableRef, TableMetadata metadata) {
        catalogRegistry.tableControl().requireCreatableName(tableRef.catalog(), tableRef.schema(), tableRef.table());
        CatalogHandle catalogHandle = catalogRegistry.getCatalog(tableRef.catalog());
        ConnectorSession connSession = DefaultConnectorSession.fromSessionContext(
                session, tableRef.catalog(), tableRef.schema());
        ConnectorMetadata connMetadata = catalogHandle.getMetadata(connSession);
        SchemaTableName schemaTable = new SchemaTableName(tableRef.schema(), tableRef.table());
        List<String> columnNames = metadata.columns().stream().map(ColumnMetadata::name).toList();
        var typedColumns = metadata.columns().stream().map(MetadataColumnDeclarations::connectorColumn).toList();
        ConnectorTableMetadata connTableMeta = new ConnectorTableMetadata(
                schemaTable, columnNames, typedColumns, metadata.properties());
        connMetadata.createTable(connSession, connTableMeta);
        registerCreatedTable(connMetadata, connSession, tableRef, session.user());
        invalidateTableCaches(tableRef);
        eventDispatcher.dispatch(new MetadataEvent.TableCreatedEvent(tableRef, metadata));
        LOG.info("event=table_created catalog={} schema={} table={}",
                tableRef.catalog(), tableRef.schema(), tableRef.table());
    }

    @Override
    public void dropTable(MetadataSessionContext session, QualifiedTableRef tableRef) {
        var tableControl = catalogRegistry.tableControl();
        tableControl.requireDroppable(
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
        if (tableControl.enforcing()) {
            tableControl.requireDroppable(
                    tableRef.catalog(), tableRef.schema(), tableRef.table(), "DROP TABLE",
                    connMetadata.getTableIdentity(connSession, tableHandle));
        }
        var record = tableControl.find(
                tableRef.catalog(), tableRef.schema(), tableRef.table());
        if (tableControl.enabled()) {
            record.ifPresent(captured -> tableControl.requireCurrentCatalog(captured, "DROP TABLE"));
        }
        connMetadata.dropTable(connSession, tableHandle);
        record.ifPresent(tableControl::markDropped);
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
            TableMetadata current = resolution.resolveTableMetadata(session, tableRef);
            validateSchemaChanges(current, changes);
            applySchemaChangesToConnector(session, tableRef, changes);
            // Re-read connector authority, not the unscoped inventory projection.
            resolution.resolveTableMetadata(session, tableRef);
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
        resolution.invalidateTableCaches(tableRef);
    }

    public void invalidateCatalogCaches(String catalogName) {
        resolution.invalidateCatalogCaches(catalogName);
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
