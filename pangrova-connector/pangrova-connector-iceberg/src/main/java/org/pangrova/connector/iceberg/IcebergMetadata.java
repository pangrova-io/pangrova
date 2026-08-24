package org.pangrova.connector.iceberg;

import org.pangrova.connector.iceberg.handle.IcebergOutputTableHandle;
import org.pangrova.connector.iceberg.handle.IcebergTableHandle;
import org.pangrova.connector.iceberg.handle.IcebergTableHandleFactory;
import org.pangrova.connector.iceberg.index.IcebergNativeIndexEvaluator;
import org.pangrova.connector.iceberg.schema.IcebergSchemaBuilder;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.data.type.PangrovaType;
import org.pangrova.spi.connector.index.ColumnValueReader;
import org.pangrova.spi.connector.index.CommitListener;
import org.pangrova.spi.connector.index.ConnectorIndexSupport;
import org.pangrova.spi.connector.index.FileSystemAccessor;
import org.pangrova.spi.connector.index.NativeIndexCapability;
import org.pangrova.spi.connector.index.NativeIndexEvaluator;
import org.pangrova.spi.connector.index.SnapshotAwareMetadata;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.ConnectorOutputTableHandle;
import org.pangrova.spi.connector.metadata.ConnectorSystemTable;
import org.pangrova.spi.connector.metadata.DeleteFilter;
import org.pangrova.spi.connector.metadata.DeletionMode;
import org.pangrova.spi.connector.metadata.table.ColumnHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.connector.metadata.table.TableStatistics;
import org.pangrova.spi.index.statistics.DataFileStatistics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;

/**
 * Iceberg metadata implementation delegating to real Iceberg Catalog API.
 * Acts as anti-corruption layer: Iceberg SDK types stay within this module.
 */
public class IcebergMetadata implements ConnectorMetadata, QuerySnapshotRegistry {

    static final long DEFAULT_PIN_TTL_MS = 1_800_000L; // 30 minutes
    static final long DEFAULT_COMMIT_TIMEOUT_MS = 30_000L; // 30 seconds

    private final Catalog catalog;
    private final String catalogName;
    private final IcebergMaintenanceProcedures maintenanceProcedures;
    private final CatalogExceptionClassifier exceptionClassifier = new CatalogExceptionClassifier();
    private final IcebergNativeIndexEvaluator nativeIndexEvaluator = new IcebergNativeIndexEvaluator();
    private final IcebergCommitNotifier notifier;
    private final IcebergWriteCommitter writeCommitter;
    private final IcebergSnapshotPinManager pinManager;
    private final IcebergIndexAccessors indexAccessors;
    private final IcebergColumnAccessGuard encryptionGuard;
    private final IcebergCatalogTables catalogTables;
    private final IcebergViewStore viewStore;

    public IcebergMetadata(Catalog catalog, String catalogName) {
        this(catalog, catalogName, DEFAULT_PIN_TTL_MS, DEFAULT_COMMIT_TIMEOUT_MS, Set.of(), null);
    }

    public IcebergMetadata(Catalog catalog, String catalogName, long pinTtlMs) {
        this(catalog, catalogName, pinTtlMs, DEFAULT_COMMIT_TIMEOUT_MS, Set.of(), null);
    }

    public IcebergMetadata(Catalog catalog, String catalogName, long pinTtlMs, long commitTimeoutMs) {
        this(catalog, catalogName, pinTtlMs, commitTimeoutMs, Set.of(), null);
    }

    public IcebergMetadata(Catalog catalog, String catalogName, long pinTtlMs,
                           long commitTimeoutMs, Set<String> accessibleEncryptionKeys) {
        this(catalog, catalogName, pinTtlMs, commitTimeoutMs, accessibleEncryptionKeys, null);
    }

    /**
     * Creates a metadata instance with explicit pin TTL, commit timeout,
     * encryption-key access set, and optional FileIO override.
     */
    public IcebergMetadata(Catalog catalog, String catalogName, long pinTtlMs,
                           long commitTimeoutMs, Set<String> accessibleEncryptionKeys,
                           org.apache.iceberg.io.FileIO fileIO) {
        this.catalog = catalog;
        this.catalogName = catalogName;
        this.maintenanceProcedures = new IcebergMaintenanceProcedures(catalog);
        Set<String> keys = accessibleEncryptionKeys != null
                ? Set.copyOf(accessibleEncryptionKeys) : Set.of();
        this.encryptionGuard = new IcebergColumnAccessGuard(keys);
        this.notifier = new IcebergCommitNotifier(catalogName);
        this.writeCommitter = new IcebergWriteCommitter(catalogName, commitTimeoutMs, notifier);
        this.pinManager = new IcebergSnapshotPinManager(
                catalogName, pinTtlMs, nativeIndexEvaluator::clearCache);
        this.indexAccessors = new IcebergIndexAccessors(
                catalog, fileIO, nativeIndexEvaluator, notifier);
        this.catalogTables = new IcebergCatalogTables(catalog);
        this.viewStore = new IcebergViewStore(catalogName);
    }

    /**
     * Starts periodic eviction of expired pinned snapshots.
     * Must be called once during connector initialization with a shared scheduler.
     */
    public void startPinEviction(ScheduledExecutorService scheduler) {
        pinManager.startPinEviction(scheduler);
    }

    /** Registers a query so that subsequent snapshot pinning applies to it. */
    @Override
    public void beginQuery(String queryId) {
        pinManager.beginQuery(queryId);
    }

    @Override
    public void pinSnapshotForQuery(String queryId, long snapshotId) {
        pinManager.pinSnapshotForQuery(queryId, snapshotId);
    }

    /** Pins a snapshot for a specific table within a query (F-CONNECTOR-08). */
    @Override
    public void pinSnapshotForQuery(String queryId, String schema, String table, long snapshotId) {
        pinManager.pinSnapshotForQuery(queryId, schema, table, snapshotId);
    }

    /** Unpins the snapshot for the query and releases index evaluator caches. */
    @Override
    public void cleanupQuery(String queryId) {
        pinManager.cleanupQuery(queryId);
    }

    /** Returns the pinned snapshot ID for the given query, or empty if none pinned. */
    @Override
    public Optional<Long> getPinnedSnapshot(String queryId) {
        return pinManager.getPinnedSnapshot(queryId);
    }

    /** Returns the pinned snapshot ID for a specific table within a query (F-CONNECTOR-08). */
    @Override
    public Optional<Long> getPinnedSnapshot(String queryId, String schema, String table) {
        return pinManager.getPinnedSnapshot(queryId, schema, table);
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        return catalogTables.listSchemaNames();
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName) {
        return catalogTables.listTables(schemaName);
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
        return catalogTables.getTableHandle(tableName);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
        return catalogTables.getTableMetadata((IcebergTableHandle) table);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle table) {
        return catalogTables.getColumnHandles((IcebergTableHandle) table);
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle table) {
        return IcebergTableStatisticsReader.read(
                catalogTables, (IcebergTableHandle) table, session, pinManager, exceptionClassifier);
    }

    @Override
    public ConnectorOutputTableHandle createTable(ConnectorSession session, ConnectorTableMetadata metadata) {
        Schema schema = IcebergSchemaBuilder.buildIcebergSchema(metadata.typedColumns(), metadata.properties());
        TableIdentifier id = TableIdentifier.of(metadata.table().schema(), metadata.table().table());
        PartitionSpec spec = IcebergSchemaBuilder.buildPartitionSpec(schema, metadata.properties());
        org.apache.iceberg.SortOrder sortOrder = IcebergSchemaBuilder.buildSortOrder(schema, metadata.properties());
        Map<String, String> icebergProps =
                IcebergSchemaBuilder.extractIcebergTableProperties(metadata.properties());
        Table table = icebergProps.isEmpty()
                ? catalog.createTable(id, schema, spec)
                : catalog.createTable(id, schema, spec, icebergProps);
        IcebergSchemaBuilder.applySortOrder(table, schema, sortOrder);
        return IcebergTableHandleFactory.outputFromTable(
                metadata.table().schema(), metadata.table().table(), table, schema);
    }

    @Override
    public ConnectorOutputTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle table) {
        IcebergTableHandle handle = (IcebergTableHandle) table;
        return IcebergOutputHandles.forInsert(handle, catalogTables.resolveTable(handle));
    }

    /**
     * A-COMMIT-01: rebuilds an output handle for crash recovery from the live table metadata,
     * mirroring {@code beginInsert/beginOverwrite/beginDelete} for the current table and the
     * recorded {@code writeMode}. Throws {@code NoSuchTableException} if the table is gone (the
     * recovering reconciler then aborts the intent).
     */
    @Override
    public ConnectorOutputTableHandle reconstructOutputHandle(ConnectorSession session,
            SchemaTableName tableName, String writeMode) {
        return IcebergOutputHandles.reconstruct(catalog, tableName, writeMode);
    }

    @Override
    public ConnectorOutputTableHandle beginOverwrite(ConnectorSession session, ConnectorTableHandle table) {
        IcebergTableHandle handle = (IcebergTableHandle) table;
        return IcebergOutputHandles.forOverwrite(handle, catalogTables.resolveTable(handle));
    }

    @Override
    public ConnectorOutputTableHandle beginDelete(ConnectorSession session, ConnectorTableHandle table) {
        IcebergTableHandle handle = (IcebergTableHandle) table;
        return IcebergOutputHandles.forDelete(handle, catalogTables.resolveTable(handle));
    }

    @Override
    public DeletionMode deletionMode(ConnectorSession session, ConnectorTableHandle table) {
        return IcebergFormatVersions.deletionMode(catalogTables.resolveTable((IcebergTableHandle) table));
    }

    @Override
    public void finishDistributedWrite(ConnectorSession session,
                                       ConnectorOutputTableHandle outputHandle,
                                       List<byte[]> fragments) {
        IcebergOutputTableHandle handle = (IcebergOutputTableHandle) outputHandle;
        TableIdentifier tableId = TableIdentifier.of(handle.schemaName(), handle.tableName());
        Table icebergTable = catalog.loadTable(tableId);
        icebergTable.refresh();
        writeCommitter.finishDistributedWrite(icebergTable, handle, fragments);
    }

    /**
     * A-EOS-02: discards worker-staged data files that were written but never published (the 2PC
     * abort/discard leg), so an aborted streaming prepare leaves no orphans.
     */
    @Override
    public void abortDistributedWrite(ConnectorSession session,
                                      ConnectorOutputTableHandle outputHandle,
                                      List<byte[]> fragments) {
        IcebergOutputTableHandle handle = (IcebergOutputTableHandle) outputHandle;
        TableIdentifier tableId = TableIdentifier.of(handle.schemaName(), handle.tableName());
        Table icebergTable = catalog.loadTable(tableId);
        writeCommitter.abortDistributedWrite(icebergTable, handle, fragments);
    }

    @Override
    public boolean supportsTargetedDelete() {
        return true;
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle table) {
        IcebergDdlOperations.dropTable(catalog, (IcebergTableHandle) table);
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName) {
        IcebergDdlOperations.createSchema(catalog, schemaName);
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle table,
                          String columnName, PangrovaType columnType) {
        IcebergDdlOperations.addColumn(catalog, (IcebergTableHandle) table, columnName, columnType);
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle table,
                          String columnName, PangrovaType columnType, Object defaultValue) {
        IcebergDdlOperations.addColumn(
                catalog, (IcebergTableHandle) table, columnName, columnType, defaultValue);
    }

    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle table,
                           String columnName) {
        IcebergDdlOperations.dropColumn(catalog, (IcebergTableHandle) table, columnName);
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle table,
                             String oldName, String newName) {
        IcebergDdlOperations.renameColumn(catalog, (IcebergTableHandle) table, oldName, newName);
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle table,
                            String newSchemaName, String newTableName) {
        IcebergDdlOperations.renameTable(catalog, (IcebergTableHandle) table, newSchemaName, newTableName);
    }

    @Override
    public void changeColumnType(ConnectorSession session, ConnectorTableHandle table,
                                 String columnName, PangrovaType newType) {
        IcebergDdlOperations.changeColumnType(catalog, (IcebergTableHandle) table, columnName, newType.name());
    }

    @Override
    public void setPartitionSpec(ConnectorSession session, ConnectorTableHandle table,
                                 List<PartitionTransform> transforms) {
        List<Map.Entry<String, List<String>>> entries = transforms.stream()
                .map(t -> Map.entry(
                        t.functionName() != null ? t.functionName() : "",
                        t.arguments()))
                .toList();
        IcebergDdlOperations.setPartitionSpec(catalog, (IcebergTableHandle) table, entries);
    }

    public String getCatalogName() {
        return catalogName;
    }

    @Override
    public Optional<ConnectorSystemTable> getSystemTable(
            ConnectorSession session, ConnectorTableHandle table, String systemTableName) {
        IcebergTableHandle handle = (IcebergTableHandle) table;
        return IcebergSystemTables.resolve(catalogTables.resolveTable(handle), systemTableName);
    }

    @Override
    public void validateColumnAccess(ConnectorSession session, ConnectorTableHandle table,
                                     List<String> projectedColumns) {
        encryptionGuard.validate(table, projectedColumns);
    }

    @Override
    public void deleteByFilter(ConnectorSession session,
                                ConnectorTableHandle table,
                                DeleteFilter filter) {
        IcebergTableHandle handle = (IcebergTableHandle) table;
        writeCommitter.deleteByFilter(catalogTables.resolveTable(handle), handle, filter);
    }

    /** Returns the underlying Iceberg catalog for direct metadata access. */
    public Catalog catalog() {
        return catalog;
    }

    @Override
    public Optional<java.time.Instant> resolveSnapshotTimestamp(
            ConnectorSession session, String schemaName, String tableName, long snapshotId) {
        return IcebergSnapshotResolver.resolveTimestamp(catalog, schemaName, tableName, snapshotId);
    }

    @Override
    public OptionalLong resolveRefSnapshotId(
            ConnectorSession session, SchemaTableName tableName,
            String refType, String refName) {
        return IcebergSnapshotResolver.resolveRefSnapshotId(catalog, tableName, refType, refName);
    }

    // --- Index SPI overrides ---

    @Override
    public Optional<FileSystemAccessor> getFileSystemAccessor() {
        return indexAccessors.getFileSystemAccessor();
    }

    @Override
    public Optional<ColumnValueReader> getColumnValueReader() {
        return indexAccessors.getColumnValueReader();
    }

    @Override
    public Optional<NativeIndexEvaluator> getNativeIndexEvaluator() {
        return indexAccessors.getNativeIndexEvaluator();
    }

    @Override
    public List<NativeIndexCapability> nativeIndexCapabilities() {
        return indexAccessors.nativeIndexCapabilities();
    }

    @Override
    public List<String> listDataFiles(String targetCatalogName, String schemaName, String tableName) {
        return indexAccessors.listDataFiles(targetCatalogName, schemaName, tableName);
    }

    @Override
    public void registerCommitListener(CommitListener listener) {
        notifier.registerCommitListener(listener);
    }

    /** Registers a callback to invalidate the split manager's manifest cache on commit. */
    void setSplitCacheInvalidator(Consumer<String> invalidator) {
        notifier.setSplitCacheInvalidator(invalidator);
    }

    @Override
    public Optional<SnapshotAwareMetadata> getSnapshotAwareMetadata(
            String schemaName, String tableName) {
        return indexAccessors.getSnapshotAwareMetadata(schemaName, tableName);
    }

    @Override
    public Optional<ConnectorIndexSupport> getIndexSupport() {
        return Optional.of(indexAccessors);
    }

    @Override
    public List<DataFileStatistics> getFileStatistics(
            String targetCatalogName, String schemaName, String tableName,
            List<String> dataFilePaths) {
        return indexAccessors.getFileStatistics(schemaName, tableName, dataFilePaths);
    }

    @Override
    public void createBranch(ConnectorSession session, ConnectorTableHandle table, String branchName) {
        IcebergTableHandle handle = (IcebergTableHandle) table;
        IcebergRefOperations.createBranch(catalogTables.resolveTable(handle), handle, branchName);
    }

    @Override
    public void dropBranch(ConnectorSession session, ConnectorTableHandle table, String branchName) {
        IcebergTableHandle handle = (IcebergTableHandle) table;
        IcebergRefOperations.dropBranch(catalogTables.resolveTable(handle), handle, branchName);
    }

    @Override
    public void createTag(ConnectorSession session, ConnectorTableHandle table, String tagName) {
        IcebergTableHandle handle = (IcebergTableHandle) table;
        IcebergRefOperations.createTag(catalogTables.resolveTable(handle), handle, tagName);
    }

    @Override
    public void dropTag(ConnectorSession session, ConnectorTableHandle table, String tagName) {
        IcebergTableHandle handle = (IcebergTableHandle) table;
        IcebergRefOperations.dropTag(catalogTables.resolveTable(handle), handle, tagName);
    }

    @Override
    public Optional<ConnectorTableHandle> applyFilter(
            ConnectorSession session, ConnectorTableHandle table,
            org.pangrova.spi.connector.metadata.Constraint constraint) {
        if (constraint.getDomains().isEmpty()) {
            return Optional.empty();
        }
        // Iceberg absorbs the filter into scan planning (partition pruning + file-level
        // min/max); returning the same handle signals the engine the filter was accepted.
        return Optional.of(table);
    }

    @Override
    public Optional<ConnectorTableHandle> applyLimit(
            ConnectorSession session, ConnectorTableHandle table, long limit) {
        IcebergTableHandle handle = (IcebergTableHandle) table;
        if (handle.hasLimit() && handle.pushedLimit() <= limit) {
            return Optional.empty();
        }
        return Optional.of(handle.withLimit(limit));
    }

    @Override
    public Optional<ConnectorTableHandle> applyTopN(
            ConnectorSession session, ConnectorTableHandle table,
            org.pangrova.spi.connector.metadata.TopNInfo topN) {
        // Iceberg does not support sorted scans at the storage layer, so the engine
        // must still perform the sort. However, the limit portion of the top-N can be
        // absorbed to reduce the number of rows read from the source.
        return applyLimit(session, table, topN.topN());
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName viewName, String sql) {
        viewStore.create(viewName, sql);
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName) {
        viewStore.drop(viewName);
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> schemaName) {
        return viewStore.list(schemaName);
    }

    @Override
    public Optional<String> getViewDefinition(ConnectorSession session, SchemaTableName viewName) {
        return viewStore.definition(viewName);
    }

    @Override
    public List<org.pangrova.spi.connector.data.TypedRow> executeProcedure(
            ConnectorSession session, String procedureName,
            Map<String, String> arguments) {
        return maintenanceProcedures.execute(procedureName, arguments);
    }
}
