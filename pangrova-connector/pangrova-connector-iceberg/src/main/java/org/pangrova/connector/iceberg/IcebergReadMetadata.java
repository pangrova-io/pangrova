package org.pangrova.connector.iceberg;

import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.pangrova.connector.api.enums.TableDeclarationEvidence;
import org.pangrova.connector.api.enums.SnapshotReferenceType;
import org.pangrova.connector.iceberg.enums.IcebergSystemTableKind;
import org.pangrova.connector.iceberg.handle.IcebergColumnHandle;
import org.pangrova.connector.iceberg.handle.IcebergManifestTableHandle;
import org.pangrova.connector.iceberg.handle.IcebergFileTableHandle;
import org.pangrova.connector.iceberg.handle.IcebergPartitionTableHandle;
import org.pangrova.connector.iceberg.handle.IcebergSystemTableHandle;
import org.pangrova.connector.iceberg.handle.IcebergTableHandle;
import org.pangrova.connector.iceberg.handle.IcebergTableHandleFactory;
import org.pangrova.connector.iceberg.index.IcebergNativeIndexEvaluator;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.enums.MetadataTableKind;
import org.pangrova.connector.api.metadata.MetadataTableListing;
import org.pangrova.connector.api.metadata.ConnectorMetadata;
import org.pangrova.connector.api.metadata.ConnectorReadDiscovery;
import org.pangrova.connector.api.metadata.ConnectorSystemTable;
import org.pangrova.connector.api.metadata.SnapshotReference;
import org.pangrova.connector.api.metadata.table.ColumnHandle;
import org.pangrova.connector.api.metadata.table.ConnectorReadBinding;
import org.pangrova.connector.api.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;
import org.pangrova.connector.api.metadata.table.ConnectorTableReadMetadata;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.connector.api.metadata.table.TableIdentity;
import org.pangrova.connector.api.metadata.table.TableStatistics;
import org.pangrova.connector.api.metadata.table.TableVersionToken;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;

/** Sole connector read SPI implementation; write and index lifecycles share these actual owners. */
abstract class IcebergReadMetadata implements ConnectorMetadata, QuerySnapshotRegistry {
    protected final IcebergCatalogTables catalogTables;
    protected final IcebergSnapshotPinManager pinManager;
    protected final IcebergNativeIndexEvaluator nativeIndexEvaluator = new IcebergNativeIndexEvaluator();
    protected final IcebergColumnAccessGuard encryptionGuard;
    private final Catalog catalog;
    private final boolean exactTableNameLookup;
    private final CatalogExceptionClassifier exceptionClassifier = new CatalogExceptionClassifier();

    IcebergReadMetadata(Catalog catalog, String catalogName, long pinTtlMs, Set<String> accessibleKeys,
            boolean exactTableNameLookup) {
        this.catalog = catalog;
        this.exactTableNameLookup = exactTableNameLookup;
        this.catalogTables = new IcebergCatalogTables(catalog);
        this.pinManager = new IcebergSnapshotPinManager(catalogName, pinTtlMs, nativeIndexEvaluator::clearCache);
        this.encryptionGuard = new IcebergColumnAccessGuard(
                accessibleKeys == null ? Set.of() : Set.copyOf(accessibleKeys));
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
    public void beginQuery(org.pangrova.connector.api.query.ConnectorQueryAuthority authority) {
        try (var operation = authority.enterOperation()) {
            pinManager.beginQuery(authority.queryId());
        }
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
    public void cleanupQuery(org.pangrova.connector.api.query.ConnectorQueryAuthority authority) {
        if (!authority.isClosed()) throw new IllegalStateException("Iceberg cleanup requires closed query admission");
        authority.requireDrained();
        pinManager.cleanupQuery(authority.queryId());
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
    public boolean hasExactSchemaName(ConnectorSession session, String schemaName) {
        // This catalog projects first-level Iceberg namespaces as SQL schemas.
        return catalog.getClass() == JdbcCatalog.class && !schemaName.isEmpty()
                && schemaName.indexOf('.') < 0 && ((JdbcCatalog) catalog).namespaceExists(Namespace.of(schemaName));
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName) {
        return catalogTables.listTables(schemaName);
    }

    @Override
    public boolean supportsExactTableNameLookup() {
        return exactTableNameLookup;
    }

    @Override
    public ConnectorReadDiscovery discoverRead(ConnectorSession session, SchemaTableName name) {
        if (catalog.getClass() != JdbcCatalog.class || name.table().indexOf('$') >= 0) {
            return ConnectorMetadata.super.discoverRead(session, name);
        }
        // A direct JDBC load gives this compilation its own table operations.
        final Table table;
        try {
            table = catalog.loadTable(TableIdentifier.of(name.schema(), name.table()));
        } catch (NoSuchTableException absent) {
            return null;
        }
        var handle = IcebergTableHandleFactory.fromTable(name.schema(), name.table(), table);
        return new ConnectorReadDiscovery(this, handle,
                (bindingSession, selector, check) -> {
                    var selected = (IcebergTableHandle) selector;
                    requireSameTable(handle, selected);
                    return IcebergReadBindings.bindDiscoveredData(table, handle, selected, check);
                }, (type, refName) -> IcebergSnapshotResolver.resolveRefSnapshotId(table, type, refName));
    }

    @Override
    public Optional<TableIdentity> getTableIdentity(
            ConnectorSession session, ConnectorTableHandle table) {
        if (!(table instanceof IcebergTableHandle handle)) {
            return Optional.empty();
        }
        var metadata = currentMetadata(catalogTables.resolveTable(handle));
        return Optional.of(IcebergReadBindings.liveIdentity(metadata));
    }

    @Override
    public Optional<TableVersionToken> observeVersion(
            ConnectorSession session, ConnectorTableHandle table) {
        if (!(table instanceof IcebergTableHandle handle)) {
            return Optional.empty();
        }
        return Optional.of(IcebergReadBindings.currentVersion(currentMetadata(catalogTables.resolveTable(handle))));
    }

    @Override
    public org.pangrova.connector.api.metadata.table.TableCapabilities getTableCapabilities(
            ConnectorSession session, ConnectorTableHandle table) {
        return table instanceof IcebergTableHandle
                ? IcebergTableHandle.CAPABILITIES
                : org.pangrova.connector.api.metadata.table.TableCapabilities.UNSUPPORTED;
    }

    private static org.apache.iceberg.TableMetadata currentMetadata(Table table) {
        return ((HasTableOperations) table).operations().current();
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
        int separator = tableName.table().lastIndexOf('$');
        if (separator > 0 && separator < tableName.table().length() - 1) {
            if ("partitions".equalsIgnoreCase(tableName.table().substring(separator + 1))) {
                String baseName = tableName.table().substring(0, separator);
                var base = (IcebergTableHandle) catalogTables.getTableHandle(
                        new SchemaTableName(tableName.schema(), baseName));
                return base == null ? null : IcebergPartitionReadBindings.select(
                        catalogTables, base, catalogTables.resolveTable(base));
            }
            if ("entries".equalsIgnoreCase(tableName.table().substring(separator + 1))) {
                String baseName = tableName.table().substring(0, separator);
                IcebergTableHandle base = (IcebergTableHandle) catalogTables.getTableHandle(
                        new SchemaTableName(tableName.schema(), baseName));
                return base == null ? null : IcebergFileReadBindings.selectEntries(
                        base, catalogTables.resolveTable(base));
            }
            if ("files".equalsIgnoreCase(tableName.table().substring(separator + 1))) {
                String baseName = tableName.table().substring(0, separator);
                IcebergTableHandle base = (IcebergTableHandle) catalogTables.getTableHandle(
                        new SchemaTableName(tableName.schema(), baseName));
                return base == null ? null : IcebergFileReadBindings.select(
                        base, catalogTables.resolveTable(base));
            }
            if ("manifests".equalsIgnoreCase(tableName.table().substring(separator + 1))) {
                String baseName = tableName.table().substring(0, separator);
                IcebergTableHandle base = (IcebergTableHandle) catalogTables.getTableHandle(
                        new SchemaTableName(tableName.schema(), baseName));
                return base == null ? null : IcebergManifestReadBindings.select(
                        base, catalogTables.resolveTable(base));
            }
            var kind = IcebergSystemTableKind.fromSuffix(
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
    public Optional<ConnectorTableMetadata> describeNamedTable(ConnectorSession session, SchemaTableName table) {
        if (table.table().indexOf('$') >= 0) return ConnectorMetadata.super.describeNamedTable(session, table);
        return catalogTables.describeNamedTable(table);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
        if (table instanceof IcebergPartitionTableHandle partitions) {
            var columns = partitions.typedColumns();
            return new ConnectorTableMetadata(
                    new SchemaTableName(partitions.baseTable().schemaName(), partitions.tableName()),
                    columns.stream().map(ConnectorTableMetadata.Column::name).toList(), columns, Map.of());
        }
        if (table instanceof IcebergFileTableHandle files) {
            List<ConnectorTableMetadata.Column> columns = files.typedColumns();
            return new ConnectorTableMetadata(
                    new SchemaTableName(files.baseTable().schemaName(), files.tableName()),
                    columns.stream().map(ConnectorTableMetadata.Column::name).toList(),
                    columns, Map.of());
        }
        if (table instanceof IcebergManifestTableHandle manifests) {
            List<ConnectorTableMetadata.Column> columns = manifests.typedColumns();
            return new ConnectorTableMetadata(
                    new SchemaTableName(manifests.baseTable().schemaName(), manifests.tableName()),
                    columns.stream().map(ConnectorTableMetadata.Column::name).toList(),
                    columns, Map.of());
        }
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
        if (table instanceof IcebergSystemTableHandle || table instanceof IcebergManifestTableHandle
                || table instanceof IcebergFileTableHandle || table instanceof IcebergPartitionTableHandle) {
            return new ConnectorTableReadMetadata(getTableMetadata(session, table),
                    TableDeclarationEvidence.EXACT);
        }
        return catalogTables.getTableReadMetadata((IcebergTableHandle) table);
    }

    @Override
    public ConnectorReadBinding bindRead(ConnectorSession session, ConnectorTableHandle table) {
        return IcebergReadBindings.bind(catalogTables, table);
    }

    @Override
    public ConnectorReadBinding bindReadWithIdentityCheck(ConnectorSession session, ConnectorTableHandle liveTable,
            ConnectorTableHandle selector, Consumer<Optional<TableIdentity>> beforeSnapshotBinding) {
        if (liveTable instanceof IcebergTableHandle live && selector instanceof IcebergTableHandle selected) {
            requireSameTable(live, selected);
            return IcebergReadBindings.bindData(catalogTables.resolveTable(selected), selected, beforeSnapshotBinding);
        }
        return ConnectorMetadata.super.bindReadWithIdentityCheck(session, liveTable, selector, beforeSnapshotBinding);
    }

    private static void requireSameTable(IcebergTableHandle live, IcebergTableHandle selected) {
        if (!live.schemaName().equals(selected.schemaName()) || !live.tableName().equals(selected.tableName())) {
            throw new IllegalArgumentException("Read selector refers to another Iceberg table");
        }
    }

    @Override
    public void validateReadBinding(ConnectorSession session,
            org.pangrova.connector.api.metadata.table.ConnectorReadBinding binding) {
        IcebergReadBindings.validate(catalogTables, binding);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle table) {
        if (table instanceof IcebergSystemTableHandle || table instanceof IcebergManifestTableHandle
                || table instanceof IcebergFileTableHandle || table instanceof IcebergPartitionTableHandle) {
            var typedColumns = getTableMetadata(session, table).typedColumns();
            Map<String, ColumnHandle> columns = new java.util.LinkedHashMap<>();
            int fieldId = 1;
            for (ConnectorTableMetadata.Column column : typedColumns) {
                columns.put(column.name(),
                        new IcebergColumnHandle(fieldId++, column.name(), column.type()));
            }
            return Map.copyOf(columns);
        }
        return catalogTables.getColumnHandles((IcebergTableHandle) table);
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle table) {
        if (table instanceof IcebergSystemTableHandle || table instanceof IcebergManifestTableHandle
                || table instanceof IcebergFileTableHandle || table instanceof IcebergPartitionTableHandle) {
            return TableStatistics.UNKNOWN;
        }
        return IcebergTableStatisticsReader.read(
                catalogTables, (IcebergTableHandle) table, session, pinManager, exceptionClassifier);
    }

    @Override
    public Optional<MetadataTableListing> getMetadataListing(ConnectorSession session, MetadataTableKind requested) {
        return switch (requested) {
            case TAGS -> Optional.of(new MetadataTableListing(MetadataTableKind.REFS, Map.of("type", "TAG")));
            case BRANCHES -> Optional.of(new MetadataTableListing(MetadataTableKind.REFS, Map.of("type", "BRANCH")));
            case SNAPSHOTS, HISTORY, REFS, MANIFESTS, FILES, ENTRIES, PARTITIONS ->
                    Optional.of(new MetadataTableListing(requested));
            default -> Optional.empty();
        };
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
        if (table instanceof IcebergFileTableHandle) {
            IcebergFileTableHandle files = (IcebergFileTableHandle) table;
            for (String column : projectedColumns) {
                boolean supported = files.typedColumns().stream()
                        .anyMatch(typed -> typed.name().equalsIgnoreCase(column));
                if (!supported) {
                    throw new UnsupportedOperationException(
                            "Iceberg " + (files.entries() ? "$entries" : "$files")
                                    + " Native reader does not support projected column: " + column);
                }
            }
            return;
        }
        if (table instanceof IcebergSystemTableHandle || table instanceof IcebergManifestTableHandle
                || table instanceof IcebergPartitionTableHandle) {
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
                    ref.isBranch() ? SnapshotReferenceType.BRANCH : SnapshotReferenceType.TAG,
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
