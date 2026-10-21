package org.pangrova.connector.paimon;

import org.pangrova.connector.paimon.scan.PaimonReadSchema;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.spi.connector.base.ConnectorException;
import org.pangrova.connector.paimon.handle.PaimonOutputTableHandle;
import org.pangrova.connector.paimon.handle.PaimonTableHandle;
import org.pangrova.connector.paimon.handle.PaimonTableHandleFactory;
import org.pangrova.connector.paimon.handle.PaimonSystemTableHandle;
import org.pangrova.connector.paimon.handle.PaimonFilesTableHandle;
import org.pangrova.connector.paimon.handle.PaimonBranchTableHandle;
import org.pangrova.connector.paimon.handle.PaimonColumnHandle;
import org.pangrova.connector.paimon.type.PaimonTypeMapper;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.metadata.MetadataTableKind;
import org.pangrova.spi.connector.metadata.MetadataTableListing;
import org.pangrova.spi.connector.data.type.PangrovaType;
import org.pangrova.spi.connector.metadata.ConnectorOutputTableHandle;
import org.pangrova.spi.connector.metadata.DistributedWriteCommitContext;
import org.pangrova.spi.connector.metadata.table.ColumnHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.ConnectorTableReadMetadata;
import org.pangrova.spi.connector.metadata.table.ConnectorReadBinding;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.connector.metadata.table.TableStatistics;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.FileStoreTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * Paimon metadata implementation delegating to real Paimon Catalog API.
 * Acts as anti-corruption layer: Paimon SDK types stay within this module.
 */
public class PaimonMetadata implements ConnectorMetadata {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonMetadata.class);

    private final Catalog catalog;
    private final PaimonSplitRegistry splitRegistry;
    private final String catalogName;
    private final PaimonProcedureExecutor procedures;
    private final PaimonCommitNotifier commitNotifier;
    private final PaimonWriteLifecycle writes;
    private final org.pangrova.connector.paimon.index.PaimonNativeIndexEvaluator nativeIndexEvaluator =
            new org.pangrova.connector.paimon.index.PaimonNativeIndexEvaluator();

    /** Creates a metadata instance with a catalog borrowed from its owning connector. */
    public PaimonMetadata(Catalog catalog, String catalogName, PaimonSplitRegistry splitRegistry) {
        this.catalog = catalog;
        this.splitRegistry = java.util.Objects.requireNonNull(splitRegistry, "splitRegistry");
        this.catalogName = catalogName;
        this.procedures = new PaimonProcedureExecutor(catalog);
        this.commitNotifier = new PaimonCommitNotifier(catalogName);
        this.writes = new PaimonWriteLifecycle(catalog, catalogName, commitNotifier);
    }

    /** Opens the actual query scope; snapshots are selected separately for each accessed table. */
    @Override
    public synchronized void beginQuery(org.pangrova.spi.connector.ConnectorQueryAuthority authority) {
        splitRegistry.beginQuery(authority);
    }

    /**
     * Unpins the snapshot for the query and clears the boundsCache
     * to release memory held for file-level statistics.
     */
    @Override
    public synchronized void cleanupQuery(org.pangrova.spi.connector.ConnectorQueryAuthority authority) {
        splitRegistry.releaseQuery(authority);
        String queryId = authority.queryId();
        nativeIndexEvaluator.clearCache();
        LOG.debug("event=cleanup_query queryId={} catalog={}", queryId, catalogName);
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
        int separator = tableName.table().lastIndexOf('$');
        String suffix = separator > 0
                ? tableName.table().substring(separator + 1).toLowerCase(java.util.Locale.ROOT) : "";
        try {
            if (java.util.Set.of("files", "branches", "snapshots", "tags", "options", "schemas").contains(suffix)) {
                return nativeMetadataHandle(tableName.schema(), tableName.table().substring(0, separator), suffix);
            }
            Table table = catalog.getTable(Identifier.create(tableName.schema(), tableName.table()));
            return PaimonTableHandleFactory.fromTable(tableName.schema(), tableName.table(), table);
        } catch (Catalog.TableNotExistException missing) {
            LOG.debug("Table not found: {}", tableName);
            return null;
        }
    }

    private ConnectorTableHandle nativeMetadataHandle(String schema, String name, String suffix)
            throws Catalog.TableNotExistException {
        Table base = catalog.getTable(Identifier.create(schema, name));
        if (!(base instanceof FileStoreTable)) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_UNSUPPORTED_OPERATION,
                    "Paimon system tables require a physical base table");
        }
        var handle = PaimonTableHandleFactory.fromTable(schema, name, base);
        return switch (suffix) {
            case "files" -> PaimonFilesTableHandle.selector(handle);
            case "branches" -> PaimonBranchTableHandle.selector(handle);
            default -> PaimonSystemTableHandle.selector(handle,
                    PaimonSystemTableHandle.Kind.valueOf(suffix.toUpperCase(java.util.Locale.ROOT)));
        };
    }

    @Override
    public OptionalLong resolveRefSnapshotId(
            ConnectorSession session, SchemaTableName tableName, String refType, String refName) {
        if (!"TAG".equals(refType) || refName == null || refName.isBlank()) {
            return OptionalLong.empty();
        }
        try {
            Table table = catalog.getTable(Identifier.create(tableName.schema(), tableName.table()));
            if (!(table instanceof FileStoreTable fileStore)) {
                return OptionalLong.empty();
            }
            long[] matches = fileStore.tagManager().tags().entrySet().stream()
                    .filter(entry -> entry.getValue().contains(refName))
                    .mapToLong(entry -> entry.getKey().id())
                    .limit(2)
                    .toArray();
            if (matches.length > 1) {
                throw new IllegalStateException("Paimon tag resolves to multiple snapshots: " + refName);
            }
            return matches.length == 0 ? OptionalLong.empty() : OptionalLong.of(matches[0]);
        } catch (Catalog.TableNotExistException missing) {
            return OptionalLong.empty();
        }
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
        if (table instanceof PaimonBranchTableHandle branches) {
            var columns = branches.typedColumns();
            return new ConnectorTableMetadata(
                    new SchemaTableName(branches.baseTable().schemaName(), branches.tableName()),
                    columns.stream().map(ConnectorTableMetadata.Column::name).toList(),
                    columns, Map.of());
        }
        if (table instanceof PaimonFilesTableHandle files) {
            var columns = files.typedColumns();
            return new ConnectorTableMetadata(new SchemaTableName(files.baseTable().schemaName(), files.tableName()),
                    columns.stream().map(ConnectorTableMetadata.Column::name).toList(), columns, Map.of());
        }
        if (table instanceof PaimonSystemTableHandle system) {
            var columns = system.typedColumns();
            return new ConnectorTableMetadata(
                    new SchemaTableName(system.baseTable().schemaName(), system.tableName()),
                    columns.stream().map(ConnectorTableMetadata.Column::name).toList(),
                    columns, Map.of());
        }
        PaimonTableHandle handle = (PaimonTableHandle) table;
        var schema = readSchema(handle);
        var typedColumns = PaimonTypeMapper.toPangrovaColumns(schema.rowType(), schema.options());
        List<String> columnNames = typedColumns.stream()
                .map(ConnectorTableMetadata.Column::name)
                .toList();
        SchemaTableName name = new SchemaTableName(handle.schemaName(), handle.tableName());
        return new ConnectorTableMetadata(name, columnNames, typedColumns, schema.options());
    }

    @Override
    public ConnectorTableReadMetadata getTableReadMetadata(
            ConnectorSession session, ConnectorTableHandle table) {
        return new ConnectorTableReadMetadata(getTableMetadata(session, table),
                ConnectorTableReadMetadata.DeclarationEvidence.EXACT);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle table) {
        if (table instanceof PaimonBranchTableHandle branches) {
            Map<String, ColumnHandle> columns = new java.util.LinkedHashMap<>();
            int fieldId = 1;
            for (var column : branches.typedColumns()) {
                columns.put(column.name(), new PaimonColumnHandle(
                        fieldId++, column.name(), column.type()));
            }
            return Map.copyOf(columns);
        }
        if (table instanceof PaimonFilesTableHandle files) {
            Map<String, ColumnHandle> columns = new java.util.LinkedHashMap<>();
            int fieldId = 1;
            for (var column : files.typedColumns()) {
                columns.put(column.name(), new PaimonColumnHandle(fieldId++, column.name(), column.type()));
            }
            return Map.copyOf(columns);
        }
        if (table instanceof PaimonSystemTableHandle system) {
            Map<String, ColumnHandle> columns = new java.util.LinkedHashMap<>();
            int fieldId = 1;
            for (var column : system.typedColumns()) {
                columns.put(column.name(), new PaimonColumnHandle(
                        fieldId++, column.name(), column.type()));
            }
            return Map.copyOf(columns);
        }
        PaimonTableHandle handle = (PaimonTableHandle) table;
        return readSchema(handle).columnHandles();
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle table) {
        return bindRead(session, table).statistics();
    }

    @Override
    public ConnectorReadBinding bindRead(ConnectorSession session, ConnectorTableHandle table) {
        if (table instanceof PaimonFilesTableHandle files) return PaimonFilesReadBindings.bind(catalog, files);
        if (table instanceof PaimonBranchTableHandle branches) {
            return PaimonBranchReadBindings.bind(catalog, branches);
        }
        if (table instanceof PaimonSystemTableHandle system) {
            return PaimonSystemReadBindings.bind(catalog, system);
        }
        var selector = (PaimonTableHandle) table;
        return session.getQueryAuthority().map(authority -> splitRegistry.bindRead(authority, selector,
                () -> PaimonReadBindings.bind(catalog, selector)))
                .orElseGet(() -> PaimonReadBindings.bind(catalog, selector));
    }

    @Override
    public void validateReadBinding(ConnectorSession session,
            ConnectorReadBinding binding) {
        if (binding.handle() instanceof PaimonBranchTableHandle) {
            PaimonBranchReadBindings.validate(catalog, binding);
            return;
        }
        if (binding.handle() instanceof PaimonFilesTableHandle) {
            PaimonFilesReadBindings.validate(catalog, binding);
            return;
        }
        if (binding.handle() instanceof PaimonSystemTableHandle) {
            PaimonSystemReadBindings.validate(catalog, binding);
            return;
        }
        PaimonReadBindings.validate(catalog, binding);
    }

    @Override
    public ConnectorOutputTableHandle createTable(ConnectorSession session, ConnectorTableMetadata metadata) {
        return writes.create(metadata);
    }

    @Override
    public ConnectorOutputTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle table) {
        return writes.insert((PaimonTableHandle) table);
    }

    @Override
    public ConnectorOutputTableHandle beginOverwrite(ConnectorSession session, ConnectorTableHandle table) {
        return writes.overwrite((PaimonTableHandle) table);
    }

    @Override
    public ConnectorOutputTableHandle beginOverwrite(ConnectorSession session,
                                                     ConnectorTableHandle table,
                                                     java.util.Map<String, String> staticPartition) {
        return writes.overwrite((PaimonTableHandle) table, staticPartition);
    }

    @Override
    public ConnectorOutputTableHandle beginDelete(ConnectorSession session, ConnectorTableHandle table) {
        return writes.delete((PaimonTableHandle) table);
    }

    @Override
    public void deleteByFilter(ConnectorSession session,
                                ConnectorTableHandle table,
                                org.pangrova.spi.connector.metadata.DeleteFilter filter) {
        writes.deleteByFilter((PaimonTableHandle) table, filter);
    }

    @Override
    public void finishDistributedWrite(ConnectorSession session,
                                       ConnectorOutputTableHandle outputHandle,
                                       java.util.List<byte[]> fragments) {
        throw new UnsupportedOperationException(
                "Paimon distributed commit requires a durable commit context");
    }

    @Override
    public void finishDistributedWrite(ConnectorSession session,
                                       ConnectorOutputTableHandle outputHandle,
                                       java.util.List<byte[]> fragments,
                                       DistributedWriteCommitContext context) {
        writes.finish(session, (PaimonOutputTableHandle) outputHandle, fragments, context);
    }

    @Override
    public ConnectorOutputTableHandle reconstructOutputHandle(
            ConnectorSession session, SchemaTableName tableName,
            String writeMode, long baseSnapshotId) {
        return writes.reconstruct(tableName, writeMode, baseSnapshotId);
    }

    org.pangrova.spi.connector.metadata.NativePhysicalWriteProvider physicalWriteProvider() {
        return writes.physical();
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
    public Optional<MetadataTableListing> getMetadataListing(ConnectorSession session, MetadataTableKind requested) {
        return switch (requested) {
            case SNAPSHOTS, TAGS, BRANCHES, OPTIONS, SCHEMAS, FILES -> Optional.of(new MetadataTableListing(requested));
            default -> Optional.empty();
        };
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

    private PaimonReadSchema readSchema(PaimonTableHandle handle) {
        return PaimonReadSchema.resolve(handle, loadFreshTable(handle.schemaName(), handle.tableName()));
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

    // --- Index SPI overrides ---

    @Override
    public Optional<org.pangrova.spi.connector.index.FileSystemAccessor> getFileSystemAccessor() {
        Catalog underlying = catalog;
        while (underlying instanceof org.apache.paimon.catalog.DelegateCatalog delegate) {
            underlying = delegate.wrapped();
        }
        if (underlying instanceof org.apache.paimon.catalog.AbstractCatalog filesystemCatalog) {
            return Optional.of(new org.pangrova.connector.paimon.fs.PaimonFileSystemAccessor(
                    filesystemCatalog.fileIO()));
        }
        return Optional.empty();
    }

    @Override
    public Optional<org.pangrova.spi.connector.index.ColumnValueReader> getColumnValueReader() {
        return Optional.of(new PaimonCatalogColumnValueReader(catalog));
    }

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

    /** Registers the connector's internal lifecycle callback without a data session. */
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
        return java.util.Optional.of(new PaimonConnectorIndexSupport(this, catalog));
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
            String tenantId,
            String schemaName,
            String tableName,
            java.util.List<String> addedFiles,
            java.util.List<String> removedFiles,
            String snapshotId,
            org.pangrova.spi.connector.index.CommitEventType eventType) {
        commitNotifier.notify(
                tenantId, schemaName, tableName,
                addedFiles, removedFiles, snapshotId, eventType);
    }

    /** Backward-compatible overload defaulting to APPEND. */
    protected void notifyCommitListener(
            String tenantId,
            String schemaName,
            String tableName,
            java.util.List<String> addedFiles,
            java.util.List<String> removedFiles,
            String snapshotId) {
        commitNotifier.notify(
                tenantId, schemaName, tableName, addedFiles, removedFiles, snapshotId);
    }

    // --- Branch/Tag SPI overrides ---

    @Override
    public void createBranch(ConnectorSession session, ConnectorTableHandle table, String branchName,
            org.pangrova.spi.connector.metadata.BranchCreationOptions options) {
        PaimonTableHandle handle = (PaimonTableHandle) table;
        PaimonBranchTagOperations.createBranch(
                loadFreshTable(handle.schemaName(), handle.tableName()), handle, branchName, options);
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
        if (table instanceof PaimonSystemTableHandle || table instanceof PaimonBranchTableHandle
                || table instanceof PaimonFilesTableHandle) {
            return Optional.empty();
        }
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
