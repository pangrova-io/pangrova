package org.pangrova.connector.paimon;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.spi.connector.base.ConnectorException;
import org.pangrova.connector.paimon.handle.PaimonColumnHandle;
import org.pangrova.connector.paimon.handle.PaimonOutputTableHandle;
import org.pangrova.connector.paimon.handle.PaimonTableHandle;
import org.pangrova.connector.paimon.handle.PaimonTableHandleFactory;
import org.pangrova.connector.paimon.type.PaimonTypeMapper;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.data.type.PangrovaType;
import org.pangrova.spi.connector.metadata.ConnectorOutputTableHandle;
import org.pangrova.spi.connector.metadata.table.ColumnHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.connector.metadata.table.TableStatistics;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Paimon metadata implementation delegating to real Paimon Catalog API.
 * Acts as anti-corruption layer: Paimon SDK types stay within this module.
 */
public class PaimonMetadata implements ConnectorMetadata {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonMetadata.class);

    private final Catalog catalog;
    private final String catalogName;
    private final PaimonProcedureExecutor procedures;
    private final PaimonSnapshotPins snapshotPins;
    private final PaimonCommitNotifier commitNotifier;
    private final org.pangrova.connector.paimon.index.PaimonNativeIndexEvaluator nativeIndexEvaluator =
            new org.pangrova.connector.paimon.index.PaimonNativeIndexEvaluator();

    /** Creates a metadata instance bound to the given Paimon catalog. */
    public PaimonMetadata(Catalog catalog, String catalogName) {
        this.catalog = catalog;
        this.catalogName = catalogName;
        this.procedures = new PaimonProcedureExecutor(catalog);
        this.snapshotPins = new PaimonSnapshotPins(catalogName);
        this.commitNotifier = new PaimonCommitNotifier(catalogName);
    }

    /** Pins the current snapshot for the duration of a query. */
    @Override
    public void beginQuery(String queryId) {
        snapshotPins.beginQuery(queryId);
    }

    /**
     * Pins the real snapshot ID for a query on first table access, preserving
     * the first-access snapshot consistency guarantee. Package-private for tests.
     */
    void pinSnapshotForQuery(String queryId, long snapshotId) {
        snapshotPins.pin(queryId, snapshotId);
    }

    /**
     * Unpins the snapshot for the query and clears the boundsCache
     * to release memory held for file-level statistics.
     */
    @Override
    public void cleanupQuery(String queryId) {
        Long removed = snapshotPins.remove(queryId);
        nativeIndexEvaluator.clearCache();
        LOG.debug("event=cleanup_query queryId={} catalog={} hadPinnedSnapshot={}",
                queryId, catalogName, removed != null);
    }

    /**
     * Returns the pinned snapshot ID for the given query, or empty if none pinned.
     * Package-private for testability.
     */
    Optional<Long> getPinnedSnapshot(String queryId) {
        return snapshotPins.getPinned(queryId);
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        return catalog.listDatabases();
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName) {
        return schemaName
                .map(this::listTablesInSchema)
                .orElseGet(this::listAllTables);
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
        Identifier id = Identifier.create(tableName.schema(), tableName.table());
        try {
            Table table = catalog.getTable(id);
            return PaimonTableHandleFactory.fromTable(tableName.schema(), tableName.table(), table);
        } catch (Catalog.TableNotExistException e) {
            LOG.debug("Table not found: {}", tableName);
            return null;
        }
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
        PaimonTableHandle handle = (PaimonTableHandle) table;
        RowType rowType = PaimonTableHandleFactory.parseRowType(handle);
        var typedColumns = PaimonTypeMapper.toPangrovaColumns(rowType, handle.properties());
        List<String> columnNames = typedColumns.stream()
                .map(ConnectorTableMetadata.Column::name)
                .toList();
        SchemaTableName name = new SchemaTableName(handle.schemaName(), handle.tableName());
        return new ConnectorTableMetadata(name, columnNames, typedColumns, Map.of());
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle table) {
        PaimonTableHandle handle = (PaimonTableHandle) table;
        RowType rowType = PaimonTableHandleFactory.parseRowType(handle);
        return buildColumnHandles(rowType.getFields(), handle.properties());
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle table) {
        PaimonTableHandle handle = (PaimonTableHandle) table;
        try {
            Table freshTable = loadFreshTable(handle.schemaName(), handle.tableName());
            // Pin the snapshot for the current query to ensure consistent reads
            snapshotPins.pinLatestForQuery(session, freshTable);
            return PaimonTableStatisticsReader.build(freshTable);
        } catch (Exception e) {
            LOG.debug("event=paimon_stats_collection_failed schema={} table={} error={}",
                    handle.schemaName(), handle.tableName(), e.getMessage());
            return TableStatistics.UNKNOWN;
        }
    }

    @Override
    public ConnectorOutputTableHandle createTable(ConnectorSession session, ConnectorTableMetadata metadata) {
        Schema schema = buildPaimonSchema(metadata);
        Identifier id = Identifier.create(metadata.table().schema(), metadata.table().table());
        try {
            catalog.createTable(id, schema, false);
            Table table = catalog.getTable(id);
            return PaimonTableHandleFactory.outputFromTable(
                    metadata.table().schema(), metadata.table().table(), table);
        } catch (Catalog.TableAlreadyExistException e) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_TABLE_ALREADY_EXISTS,
                    "Table already exists: " + id, e);
        } catch (Catalog.DatabaseNotExistException e) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_SCHEMA_NOT_FOUND,
                    "Database not found: " + id.getDatabaseName(), e);
        } catch (Catalog.TableNotExistException e) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_TABLE_NOT_FOUND,
                    "Failed to load created table: " + id, e);
        }
    }

    @Override
    public ConnectorOutputTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle table) {
        PaimonTableHandle handle = (PaimonTableHandle) table;
        Table freshTable = loadFreshTable(handle.schemaName(), handle.tableName());
        PaimonDdlOperations.warnCrossPartitionUpdateIfNeeded(freshTable, handle);
        return PaimonTableHandleFactory.outputFromTable(
                handle.schemaName(), handle.tableName(), freshTable);
    }

    @Override
    public ConnectorOutputTableHandle beginOverwrite(ConnectorSession session, ConnectorTableHandle table) {
        PaimonTableHandle handle = (PaimonTableHandle) table;
        Table freshTable = loadFreshTable(handle.schemaName(), handle.tableName());
        return PaimonTableHandleFactory.outputFromTable(
                handle.schemaName(), handle.tableName(), freshTable);
    }

    @Override
    public ConnectorOutputTableHandle beginOverwrite(ConnectorSession session,
                                                     ConnectorTableHandle table,
                                                     java.util.Map<String, String> staticPartition) {
        PaimonTableHandle handle = (PaimonTableHandle) table;
        Table freshTable = loadFreshTable(handle.schemaName(), handle.tableName());
        return PaimonTableHandleFactory.outputFromTable(
                handle.schemaName(), handle.tableName(), freshTable, staticPartition);
    }

    @Override
    public ConnectorOutputTableHandle beginDelete(ConnectorSession session, ConnectorTableHandle table) {
        PaimonTableHandle handle = (PaimonTableHandle) table;
        Table freshTable = loadFreshTable(handle.schemaName(), handle.tableName());
        if (freshTable.primaryKeys().isEmpty()) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    "DELETE requires a primary key table: " + handle.schemaName() + "." + handle.tableName());
        }
        return PaimonTableHandleFactory.outputFromTable(
                handle.schemaName(), handle.tableName(), freshTable);
    }

    @Override
    public void deleteByFilter(ConnectorSession session,
                                ConnectorTableHandle table,
                                org.pangrova.spi.connector.metadata.DeleteFilter filter) {
        PaimonTableHandle handle = (PaimonTableHandle) table;
        Table freshTable = loadFreshTable(handle.schemaName(), handle.tableName());
        PaimonDeleteByFilterExecutor.execute(freshTable, filter,
                handle.schemaName(), handle.tableName());
    }

    @Override
    public void finishDistributedWrite(ConnectorSession session,
                                       ConnectorOutputTableHandle outputHandle,
                                       java.util.List<byte[]> fragments) {
        PaimonOutputTableHandle handle = (PaimonOutputTableHandle) outputHandle;
        Table freshTable = loadFreshTable(handle.schemaName(), handle.tableName());
        PaimonDistributedWriteFinisher.finish(freshTable, handle, fragments, this::notifyCommitListener);
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle table) {
        PaimonTableHandle handle = (PaimonTableHandle) table;
        Identifier id = Identifier.create(handle.schemaName(), handle.tableName());
        try {
            catalog.dropTable(id, false);
            LOG.info("event=paimon_table_dropped schema={} table={}", handle.schemaName(), handle.tableName());
        } catch (Catalog.TableNotExistException e) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_TABLE_NOT_FOUND,
                    "Table not found for drop: " + id, e);
        }
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle table,
                            String newSchemaName, String newTableName) {
        PaimonTableHandle handle = (PaimonTableHandle) table;
        Identifier from = Identifier.create(handle.schemaName(), handle.tableName());
        Identifier to = Identifier.create(newSchemaName, newTableName);
        try {
            // F-SPI-001: Paimon's catalog supports table rename — expose it instead of the SPI
            // default that throws UnsupportedOperationException (closing the capability-matrix gap).
            catalog.renameTable(from, to, false);
            LOG.info("event=paimon_table_renamed fromSchema={} fromTable={} toSchema={} toTable={}",
                    handle.schemaName(), handle.tableName(), newSchemaName, newTableName);
        } catch (Catalog.TableNotExistException e) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_TABLE_NOT_FOUND,
                    "Table not found for rename: " + from, e);
        } catch (Catalog.TableAlreadyExistException e) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_TABLE_ALREADY_EXISTS,
                    "Target table already exists for rename: " + to, e);
        }
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName) {
        PaimonDdlOperations.createSchema(catalog, schemaName);
    }

    public String getCatalogName() {
        return catalogName;
    }

    @Override
    public java.util.Optional<org.pangrova.spi.connector.metadata.ConnectorSystemTable> getSystemTable(
            ConnectorSession session, ConnectorTableHandle table, String systemTableName) {
        PaimonTableHandle handle = (PaimonTableHandle) table;
        Table paimonTable = loadFreshTable(handle.schemaName(), handle.tableName());
        return PaimonSystemTables.resolve(paimonTable, systemTableName);
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle table,
                          String columnName, PangrovaType columnType) {
        PaimonDdlOperations.addColumn(catalog, (PaimonTableHandle) table, columnName, columnType);
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle table,
                          String columnName, PangrovaType columnType, Object defaultValue) {
        PaimonDdlOperations.addColumn(
                catalog, (PaimonTableHandle) table, columnName, columnType, defaultValue);
    }

    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle table,
                           String columnName) {
        PaimonDdlOperations.dropColumn(catalog, (PaimonTableHandle) table, columnName);
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle table,
                             String oldName, String newName) {
        PaimonDdlOperations.renameColumn(catalog, (PaimonTableHandle) table, oldName, newName);
    }

    @Override
    public void changeColumnType(ConnectorSession session, ConnectorTableHandle table,
                                 String columnName, org.pangrova.spi.connector.data.type.PangrovaType newType) {
        PaimonDdlOperations.changeColumnType(catalog, (PaimonTableHandle) table, columnName, newType.name());
    }

    /**
     * Loads a fresh table from the catalog.
     * Used when SDK Table object is needed for operations.
     */
    private Table loadFreshTable(String schemaName, String tableName) {
        Identifier id = Identifier.create(schemaName, tableName);
        try {
            return catalog.getTable(id);
        } catch (Catalog.TableNotExistException e) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_TABLE_NOT_FOUND,
                    "Table not found: " + id, e);
        }
    }

    private Schema buildPaimonSchema(ConnectorTableMetadata metadata) {
        return org.pangrova.connector.paimon.schema.PaimonSchemaBuilder.buildPaimonSchema(metadata);
    }

    @Override
    public java.util.List<org.pangrova.spi.connector.data.TypedRow> executeProcedure(
            ConnectorSession session, String procedureName,
            java.util.Map<String, String> arguments) {
        return procedures.execute(procedureName, arguments);
    }

    private List<SchemaTableName> listTablesInSchema(String schema) {
        try {
            return catalog.listTables(schema).stream()
                    .map(name -> new SchemaTableName(schema, name))
                    .toList();
        } catch (Catalog.DatabaseNotExistException e) {
            LOG.debug("Database not found: {}", schema);
            return List.of();
        }
    }

    private List<SchemaTableName> listAllTables() {
        return catalog.listDatabases().stream()
                .flatMap(db -> listTablesInSchema(db).stream())
                .toList();
    }

    private Map<String, ColumnHandle> buildColumnHandles(List<DataField> fields,
                                                          Map<String, String> tableProperties) {
        Map<String, ColumnHandle> handles = new LinkedHashMap<>();
        for (DataField field : fields) {
            PangrovaType pangrovaType = PaimonTypeMapper.toPangrovaType(
                    field.type(), field.name(), tableProperties);
            handles.put(field.name(), new PaimonColumnHandle(field.id(), field.name(), pangrovaType));
        }
        return handles;
    }

    // --- Index SPI overrides ---

    @Override
    public java.util.List<org.pangrova.spi.connector.index.NativeIndexCapability> nativeIndexCapabilities() {
        return PaimonIndexAccessors.nativeIndexCapabilities();
    }

    @Override
    public java.util.List<String> listDataFiles(
            String targetCatalogName, String schemaName, String tableName) {
        return PaimonIndexAccessors.listDataFiles(catalog, schemaName, tableName);
    }

    @Override
    public java.util.Optional<org.pangrova.spi.connector.index.NativeIndexEvaluator> getNativeIndexEvaluator() {
        return java.util.Optional.of(nativeIndexEvaluator);
    }

    @Override
    public void registerCommitListener(org.pangrova.spi.connector.index.CommitListener listener) {
        commitNotifier.register(listener);
    }

    @Override
    public java.util.Optional<org.pangrova.spi.connector.index.SnapshotAwareMetadata> getSnapshotAwareMetadata(
            String schemaName, String tableName) {
        return PaimonIndexAccessors.getSnapshotAwareMetadata(catalog, schemaName, tableName);
    }

    @Override
    public java.util.Optional<org.pangrova.spi.connector.index.ConnectorIndexSupport> getIndexSupport() {
        return java.util.Optional.of(new PaimonConnectorIndexSupport(this));
    }

    @Override
    public java.util.List<org.pangrova.spi.index.statistics.DataFileStatistics> getFileStatistics(
            String targetCatalogName, String schemaName, String tableName,
            java.util.List<String> dataFilePaths) {
        return PaimonIndexAccessors.getFileStatistics(catalog, schemaName, tableName, dataFilePaths);
    }

    /**
     * Notifies the registered commit listener after a successful Paimon snapshot commit.
     * Never fails the commit due to listener errors.
     */
    protected void notifyCommitListener(
            String schemaName,
            String tableName,
            java.util.List<String> addedFiles,
            java.util.List<String> removedFiles,
            String snapshotId,
            org.pangrova.spi.connector.index.CommitEventType eventType) {
        commitNotifier.notify(schemaName, tableName, addedFiles, removedFiles, snapshotId, eventType);
    }

    /** Backward-compatible overload defaulting to APPEND. */
    protected void notifyCommitListener(
            String schemaName,
            String tableName,
            java.util.List<String> addedFiles,
            java.util.List<String> removedFiles,
            String snapshotId) {
        commitNotifier.notify(schemaName, tableName, addedFiles, removedFiles, snapshotId);
    }

    // --- Branch/Tag SPI overrides ---

    @Override
    public void createBranch(ConnectorSession session, ConnectorTableHandle table, String branchName) {
        PaimonTableHandle handle = (PaimonTableHandle) table;
        PaimonBranchTagOperations.createBranch(
                loadFreshTable(handle.schemaName(), handle.tableName()), handle, branchName);
    }

    @Override
    public void dropBranch(ConnectorSession session, ConnectorTableHandle table, String branchName) {
        PaimonTableHandle handle = (PaimonTableHandle) table;
        PaimonBranchTagOperations.dropBranch(
                loadFreshTable(handle.schemaName(), handle.tableName()), handle, branchName);
    }

    @Override
    public void createTag(ConnectorSession session, ConnectorTableHandle table, String tagName) {
        PaimonTableHandle handle = (PaimonTableHandle) table;
        PaimonBranchTagOperations.createTag(
                loadFreshTable(handle.schemaName(), handle.tableName()), handle, tagName);
    }

    @Override
    public void dropTag(ConnectorSession session, ConnectorTableHandle table, String tagName) {
        PaimonTableHandle handle = (PaimonTableHandle) table;
        PaimonBranchTagOperations.dropTag(
                loadFreshTable(handle.schemaName(), handle.tableName()), handle, tagName);
    }

    @Override
    public Optional<ConnectorTableHandle> applyFilter(
            ConnectorSession session, ConnectorTableHandle table,
            org.pangrova.spi.connector.metadata.Constraint constraint) {
        if (constraint.getDomains().isEmpty()) {
            return Optional.empty();
        }
        // Paimon supports partition pruning and key-range filtering.
        // The constraint is absorbed into the scan planning phase via the split manager.
        return Optional.of(table);
    }

    @Override
    public Optional<ConnectorTableHandle> applyLimit(
            ConnectorSession session, ConnectorTableHandle table, long limit) {
        PaimonTableHandle handle = (PaimonTableHandle) table;
        if (handle.hasLimit() && handle.pushedLimit() <= limit) {
            return Optional.empty();
        }
        return Optional.of(handle.withLimit(limit));
    }

    @Override
    public Optional<ConnectorTableHandle> applyTopN(
            ConnectorSession session, ConnectorTableHandle table,
            org.pangrova.spi.connector.metadata.TopNInfo topN) {
        // Paimon does not support sorted scans at the storage layer, so the engine
        // must still perform the sort. However, the limit portion of the top-N can be
        // absorbed to reduce the number of rows read from the source.
        return applyLimit(session, table, topN.topN());
    }

    @Override
    public void setPartitionSpec(ConnectorSession session, ConnectorTableHandle table,
                                 java.util.List<ConnectorMetadata.PartitionTransform> transforms) {
        PaimonTableHandle handle = (PaimonTableHandle) table;
        throw new UnsupportedOperationException(
                "Paimon does not support partition evolution on table "
                        + handle.schemaName() + "." + handle.tableName()
                        + ". Partition keys are fixed at creation time.");
    }
}
