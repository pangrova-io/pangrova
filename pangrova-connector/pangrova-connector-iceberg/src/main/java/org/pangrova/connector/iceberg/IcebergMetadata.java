package org.pangrova.connector.iceberg;

import org.pangrova.connector.iceberg.handle.IcebergOutputTableHandle;
import org.pangrova.connector.iceberg.handle.IcebergTableHandle;
import org.pangrova.connector.iceberg.handle.IcebergTableHandleFactory;
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
import org.pangrova.spi.connector.metadata.ConnectorOutputTableHandle;
import org.pangrova.spi.connector.metadata.DeleteFilter;
import org.pangrova.spi.connector.metadata.DeletionMode;
import org.pangrova.spi.connector.metadata.DistributedWriteCommitContext;
import org.pangrova.spi.connector.metadata.CopyFilePublicationMetadata;
import org.pangrova.spi.connector.metadata.ConditionalBranchDropResult;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.coordination.WriteCommitFingerprint;
import org.pangrova.spi.coordination.CopyFilePublication;
import org.pangrova.spi.index.statistics.DataFileStatistics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.TypeUtil;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Iceberg metadata implementation delegating to real Iceberg Catalog API.
 * Acts as anti-corruption layer: Iceberg SDK types stay within this module.
 */
public class IcebergMetadata extends IcebergReadMetadata implements CopyFilePublicationMetadata {

    static final long DEFAULT_PIN_TTL_MS = 1_800_000L; // 30 minutes
    static final long DEFAULT_COMMIT_TIMEOUT_MS = 30_000L; // 30 seconds

    private final Catalog catalog;
    private final String catalogName;
    private final IcebergMaintenanceProcedures maintenanceProcedures;
    private final IcebergCommitNotifier notifier;
    private final IcebergWriteCommitter writeCommitter;
    private final IcebergIndexAccessors indexAccessors;
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
        super(catalog, catalogName, pinTtlMs, accessibleEncryptionKeys);
        this.catalog = catalog;
        this.catalogName = catalogName;
        this.maintenanceProcedures = new IcebergMaintenanceProcedures(catalog);
        this.notifier = new IcebergCommitNotifier(catalogName);
        this.writeCommitter = new IcebergWriteCommitter(catalog, catalogName, commitTimeoutMs, notifier);
        this.indexAccessors = new IcebergIndexAccessors(
                catalog, fileIO, nativeIndexEvaluator, notifier);
        this.viewStore = new IcebergViewStore(catalogName);
    }

    @Override
    public ConnectorOutputTableHandle createTable(ConnectorSession session, ConnectorTableMetadata metadata) {
        Map<String, String> declaredProperties = IcebergSchemaBuilder.declaredTableProperties(
                metadata.typedColumns(), metadata.properties());
        Schema schema = TypeUtil.assignIncreasingFreshIds(
                IcebergSchemaBuilder.buildIcebergSchema(metadata.typedColumns(), declaredProperties));
        TableIdentifier id = TableIdentifier.of(metadata.table().schema(), metadata.table().table());
        PartitionSpec spec = IcebergSchemaBuilder.buildPartitionSpec(schema, declaredProperties);
        org.apache.iceberg.SortOrder sortOrder = IcebergSchemaBuilder.buildSortOrder(schema, declaredProperties);
        Map<String, String> icebergProps =
                IcebergFormatVersions.creationProperties(schema, declaredProperties);
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
        return IcebergOutputHandles.forInsert(handle, writeTable(handle));
    }

    /** Rebuilds a non-recovery handle from current table metadata. */
    @Override
    public ConnectorOutputTableHandle reconstructOutputHandle(ConnectorSession session,
            SchemaTableName tableName, String writeMode) {
        return IcebergOutputHandles.reconstruct(catalog, tableName, writeMode);
    }

    @Override
    public ConnectorOutputTableHandle reconstructOutputHandle(
            ConnectorSession session,
            SchemaTableName tableName,
            String writeMode,
            long baseSnapshotId) {
        return IcebergOutputHandles.reconstruct(
                catalog, tableName, writeMode, baseSnapshotId);
    }

    @Override
    public ConnectorOutputTableHandle beginOverwrite(ConnectorSession session, ConnectorTableHandle table) {
        IcebergTableHandle handle = (IcebergTableHandle) table;
        return IcebergOutputHandles.forOverwrite(handle, writeTable(handle));
    }

    @Override
    public ConnectorOutputTableHandle beginOverwrite(
            ConnectorSession session, ConnectorTableHandle table, Map<String, String> staticPartitions) {
        IcebergTableHandle handle = (IcebergTableHandle) table;
        return IcebergOutputHandles.forOverwrite(handle, writeTable(handle), staticPartitions);
    }

    @Override
    public ConnectorOutputTableHandle beginDelete(ConnectorSession session, ConnectorTableHandle table) {
        IcebergTableHandle handle = (IcebergTableHandle) table;
        return IcebergOutputHandles.forDelete(handle, writeTable(handle));
    }

    private Table writeTable(IcebergTableHandle handle) {
        Table table = catalogTables.resolveTable(handle);
        IcebergDeclarationCommit.requireSupported(catalog, table);
        return table;
    }

    @Override
    public DeletionMode deletionMode(ConnectorSession session, ConnectorTableHandle table) {
        return IcebergFormatVersions.deletionMode(catalogTables.resolveTable((IcebergTableHandle) table));
    }

    @Override
    public void finishDistributedWrite(ConnectorSession session,
                                       ConnectorOutputTableHandle outputHandle,
                                       List<byte[]> fragments) {
        finishIcebergDistributedWrite(session, outputHandle, fragments, null);
    }

    @Override
    public void finishDistributedWrite(
            ConnectorSession session,
            ConnectorOutputTableHandle outputHandle,
            List<byte[]> fragments,
            DistributedWriteCommitContext context) {
        finishIcebergDistributedWrite(session, outputHandle, fragments, context);
    }

    private void finishIcebergDistributedWrite(
            ConnectorSession session,
            ConnectorOutputTableHandle outputHandle,
            List<byte[]> fragments,
            DistributedWriteCommitContext context) {
        IcebergOutputTableHandle handle = (IcebergOutputTableHandle) outputHandle;
        String tenantId = tenantId(session);
        if (context != null) {
            IcebergWriteBinding.requireMatches(handle, context);
            if (!tenantId.equals(context.tenantId())) {
                throw new IllegalArgumentException(
                        "Distributed write tenant does not match durable commit context");
            }
            if (handle.baseSnapshotId() != context.baseSnapshotId()) {
                throw new IllegalArgumentException(
                        "Distributed write handle does not match durable base snapshot");
            }
            String expectedFingerprint = WriteCommitFingerprint.compute(
                new org.pangrova.spi.coordination.WriteCommitFingerprint.Intent(
                        context.executionId(),
                        tenantId,
                        catalogName,
                        new SchemaTableName(handle.schemaName(), handle.tableName()).toString(),
                        handle.writeMode(),
                        handle.baseSnapshotId(),
                        context.writeOutputBinding(),
                        context.principal(),
                        context.authorizationReceipt(), context.copyFile()),
                fragments);
            if (!expectedFingerprint.equals(context.commitFingerprint())) {
                throw new IllegalArgumentException(
                        "Distributed write payload does not match durable commit fingerprint");
            }
        }
        TableIdentifier tableId = TableIdentifier.of(handle.schemaName(), handle.tableName());
        Table icebergTable = catalog.loadTable(tableId);
        icebergTable.refresh();
        if (!handle.tableUuid().equals(IcebergOutputHandles.tableUuid(icebergTable))) {
            throw new IllegalStateException(
                    "Iceberg table UUID changed since the distributed write was planned");
        }
        if (context != null && context.copyFile() != null
                && (!IcebergOutputTableHandle.MODE_INSERT.equals(handle.writeMode())
                || !handle.tableUuid().equals(context.copyFile().tableUuid()))) {
            throw new IllegalArgumentException("COPY claim does not match the Iceberg INSERT target");
        }
        writeCommitter.finishDistributedWrite(
                icebergTable, handle, fragments, tenantId, context);
    }

    @Override
    public CopyFilePublication confirmCopyFilePublication(
            ConnectorSession session,
            ConnectorOutputTableHandle outputHandle,
            DistributedWriteCommitContext context) {
        if (context == null || context.copyFile() == null) {
            throw new IllegalArgumentException("COPY publication requires an exact claim binding");
        }
        IcebergOutputTableHandle handle = (IcebergOutputTableHandle) outputHandle;
        if (!handle.tableUuid().equals(context.copyFile().tableUuid())
                || !tenantId(session).equals(context.tenantId())
                || !IcebergOutputTableHandle.MODE_INSERT.equals(handle.writeMode())) {
            throw new IllegalArgumentException("COPY publication target differs from its write intent");
        }
        Table table = catalog.loadTable(TableIdentifier.of(handle.schemaName(), handle.tableName()));
        table.refresh();
        if (!handle.tableUuid().equals(IcebergOutputHandles.tableUuid(table))) {
            throw new IllegalStateException("Iceberg table UUID changed before COPY publication proof");
        }
        org.apache.iceberg.Snapshot snapshot = IcebergDistributedCommitMarker.publishedSnapshot(table, context);
        if (snapshot == null) {
            throw new IllegalStateException("COPY Iceberg snapshot marker is absent or outcome is unknown");
        }
        return new CopyFilePublication(context.copyFile(), context.executionId(),
                context.commitFingerprint(), snapshot.snapshotId());
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
        icebergTable.refresh();
        if (!handle.tableUuid().equals(IcebergOutputHandles.tableUuid(icebergTable))) {
            throw new IllegalStateException(
                    "Iceberg table UUID changed since the distributed write was planned");
        }
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
    public void deleteByFilter(ConnectorSession session,
                                ConnectorTableHandle table,
                                DeleteFilter filter) {
        IcebergTableHandle handle = (IcebergTableHandle) table;
        writeCommitter.deleteByFilter(
                catalogTables.resolveTable(handle), handle, filter, tenantId(session));
    }

    private static String tenantId(ConnectorSession session) {
        java.util.Objects.requireNonNull(session, "session must not be null");
        String tenantId = session.getProperties().get("tenant_id");
        if (tenantId == null || tenantId.isBlank()) {
            throw new IllegalArgumentException("tenant_id connector session property is required");
        }
        return tenantId;
    }

    /** Returns the underlying Iceberg catalog for direct metadata access. */
    public Catalog catalog() {
        return catalog;
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

    /** Registers the connector's internal lifecycle callback without a data session. */
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
    public void createBranch(ConnectorSession session, ConnectorTableHandle table, String branchName,
            org.pangrova.spi.connector.metadata.BranchCreationOptions options) {
        IcebergTableHandle handle = (IcebergTableHandle) table;
        IcebergRefOperations.createBranch(catalogTables.resolveTable(handle), handle, branchName, options);
    }

    @Override
    public void dropBranch(ConnectorSession session, ConnectorTableHandle table, String branchName) {
        IcebergTableHandle handle = (IcebergTableHandle) table;
        IcebergRefOperations.dropBranch(catalogTables.resolveTable(handle), handle, branchName);
    }

    @Override
    public ConditionalBranchDropResult dropBranchIfSnapshotMatches(
            ConnectorSession session, ConnectorTableHandle table,
            String branchName, long expectedSnapshotId) {
        IcebergTableHandle handle = (IcebergTableHandle) table;
        return IcebergRefOperations.dropBranchIfSnapshotMatches(
                catalog, catalogTables.resolveTable(handle), handle, branchName, expectedSnapshotId);
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
