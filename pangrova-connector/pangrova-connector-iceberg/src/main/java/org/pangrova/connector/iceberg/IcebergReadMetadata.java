package org.pangrova.connector.iceberg;

import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.pangrova.connector.iceberg.handle.IcebergColumnHandle;
import org.pangrova.connector.iceberg.handle.IcebergSystemTableHandle;
import org.pangrova.connector.iceberg.handle.IcebergTableHandle;
import org.pangrova.connector.iceberg.index.IcebergNativeIndexEvaluator;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.ConnectorSystemTable;
import org.pangrova.spi.connector.metadata.SnapshotReference;
import org.pangrova.spi.connector.metadata.table.ColumnHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.ConnectorTableReadMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.connector.metadata.table.TableStatistics;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

/** Sole connector read SPI implementation; write and index lifecycles share these actual owners. */
abstract class IcebergReadMetadata implements ConnectorMetadata, QuerySnapshotRegistry {
    protected final IcebergCatalogTables catalogTables;
    protected final IcebergSnapshotPinManager pinManager;
    protected final IcebergNativeIndexEvaluator nativeIndexEvaluator = new IcebergNativeIndexEvaluator();
    protected final IcebergColumnAccessGuard encryptionGuard;
    private final Catalog catalog;
    private final CatalogExceptionClassifier exceptionClassifier = new CatalogExceptionClassifier();

    IcebergReadMetadata(Catalog catalog, String catalogName, long pinTtlMs, Set<String> accessibleKeys) {
        this.catalog = catalog;
        this.catalogTables = new IcebergCatalogTables(catalog);
        this.pinManager = new IcebergSnapshotPinManager(catalogName, pinTtlMs, nativeIndexEvaluator::clearCache);
        this.encryptionGuard = new IcebergColumnAccessGuard(accessibleKeys == null ? Set.of() : Set.copyOf(accessibleKeys));
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
        int separator = tableName.table().lastIndexOf('$');
        if (separator > 0 && separator < tableName.table().length() - 1) {
            var kind = IcebergSystemTableHandle.Kind.fromSuffix(
                    tableName.table().substring(separator + 1));
            if (kind.isPresent()) {
                String baseName = tableName.table().substring(0, separator);
                IcebergTableHandle base = (IcebergTableHandle) catalogTables.getTableHandle(
                        new SchemaTableName(tableName.schema(), baseName));
                if (base == null) {
                    return null;
                }
                Table icebergTable = catalogTables.resolveTable(base);
                if (!(icebergTable instanceof HasTableOperations operations)) {
                    throw new IllegalStateException(
                            "Iceberg catalog does not expose pinned metadata object for system table");
                }
                String metadataLocation = operations.operations().current().metadataFileLocation();
                long metadataLength = operations.operations().io()
                        .newInputFile(metadataLocation).getLength();
                return new IcebergSystemTableHandle(base, kind.get(),
                        metadataLocation, metadataLength);
            }
        }
        return catalogTables.getTableHandle(tableName);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
        if (table instanceof IcebergSystemTableHandle system) {
            List<ConnectorTableMetadata.Column> columns = system.typedColumns();
            return new ConnectorTableMetadata(
                    new SchemaTableName(system.baseTable().schemaName(), system.tableName()),
                    columns.stream().map(ConnectorTableMetadata.Column::name).toList(),
                    columns, Map.of());
        }
        return catalogTables.getTableMetadata((IcebergTableHandle) table);
    }

    @Override
    public ConnectorTableReadMetadata getTableReadMetadata(ConnectorSession session, ConnectorTableHandle table) {
        if (table instanceof IcebergSystemTableHandle) {
            return new ConnectorTableReadMetadata(getTableMetadata(session, table),
                    ConnectorTableReadMetadata.DeclarationEvidence.EXACT);
        }
        return catalogTables.getTableReadMetadata((IcebergTableHandle) table);
    }

    @Override
    public org.pangrova.spi.connector.metadata.table.ConnectorReadBinding bindRead(
            ConnectorSession session, ConnectorTableHandle table) {
        return IcebergReadBindings.bind(catalogTables, table);
    }

    @Override
    public void validateReadBinding(ConnectorSession session,
            org.pangrova.spi.connector.metadata.table.ConnectorReadBinding binding) {
        IcebergReadBindings.validate(catalogTables, binding);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle table) {
        if (table instanceof IcebergSystemTableHandle system) {
            Map<String, ColumnHandle> columns = new java.util.LinkedHashMap<>();
            int fieldId = 1;
            for (ConnectorTableMetadata.Column column : system.typedColumns()) {
                columns.put(column.name(),
                        new IcebergColumnHandle(fieldId++, column.name(), column.type()));
            }
            return Map.copyOf(columns);
        }
        return catalogTables.getColumnHandles((IcebergTableHandle) table);
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle table) {
        if (table instanceof IcebergSystemTableHandle) {
            return TableStatistics.UNKNOWN;
        }
        return IcebergTableStatisticsReader.read(
                catalogTables, (IcebergTableHandle) table, session, pinManager, exceptionClassifier);
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
        if (table instanceof IcebergSystemTableHandle) {
            // Metadata columns have no base-table encryption key. The query
            // authorization layer still requires SELECT on the base table.
            return;
        }
        encryptionGuard.validate(table, projectedColumns);
    }

    @Override
    public Optional<java.time.Instant> resolveSnapshotTimestamp(
            ConnectorSession session, String schemaName, String tableName, long snapshotId) {
        return IcebergSnapshotResolver.resolveTimestamp(catalog, schemaName, tableName, snapshotId);
    }

    @Override
    public List<SnapshotReference> listSnapshotReferences(
            ConnectorSession session, ConnectorTableHandle table) {
        IcebergTableHandle handle = (IcebergTableHandle) table;
        List<SnapshotReference> result = new ArrayList<>();
        for (Map.Entry<String, SnapshotRef> entry : catalogTables.resolveTable(handle).refs().entrySet()) {
            SnapshotRef ref = entry.getValue();
            result.add(new SnapshotReference(entry.getKey(),
                    ref.isBranch() ? SnapshotReference.Type.BRANCH : SnapshotReference.Type.TAG,
                    ref.snapshotId()));
        }
        return List.copyOf(result);
    }

    @Override
    public OptionalLong resolveRefSnapshotId(
            ConnectorSession session, SchemaTableName tableName,
            String refType, String refName) {
        return IcebergSnapshotResolver.resolveRefSnapshotId(catalog, tableName, refType, refName);
    }

}
