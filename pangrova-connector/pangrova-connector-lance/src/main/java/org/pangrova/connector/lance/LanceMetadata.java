package org.pangrova.connector.lance;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.base.ConnectorException;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.ConnectorOutputTableHandle;
import org.pangrova.spi.connector.metadata.DistributedWriteCommitContext;
import org.pangrova.spi.connector.metadata.NativeDatasetControlPlane;
import org.pangrova.spi.connector.metadata.NativeWriteAllocation;
import org.pangrova.spi.connector.metadata.NativeWriteFinalization;
import org.pangrova.spi.connector.metadata.table.ColumnHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.ConnectorTableReadMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.connector.metadata.table.TableStatistics;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;

/** Tenant-scoped SQL metadata and catalog CAS after one journaled Native publication. */
public final class LanceMetadata implements ConnectorMetadata {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(LanceMetadata.class);
    private final String catalog;
    private final String warehouse;
    private final Supplier<LanceCatalogStore> store;
    private final Optional<NativeDatasetControlPlane> controlPlane;
    private final LanceSnapshotResolver snapshots;
    private volatile org.pangrova.spi.connector.index.CommitListener commitListener;

    /** Registers only the durable event append callback, not a source-row processor. */
    void registerCommitListener(org.pangrova.spi.connector.index.CommitListener listener) {
        commitListener = java.util.Objects.requireNonNull(listener, "listener");
    }

    LanceMetadata(String catalog, String warehouse, Supplier<LanceCatalogStore> store,
            Optional<NativeDatasetControlPlane> controlPlane) {
        this.catalog = catalog;
        this.warehouse = warehouse;
        this.store = store;
        this.controlPlane = controlPlane;
        this.snapshots = new LanceSnapshotResolver(store, controlPlane, catalog, warehouse);
    }
    @Override public List<String> listSchemaNames(ConnectorSession session) {
        String tenant = LanceSessionIdentity.tenant(session);
        return store.get().listSchemas(catalog, tenant);
    }
    @Override public void createSchema(ConnectorSession session, String schemaName) {
        String tenant = LanceSessionIdentity.tenant(session);
        LanceCatalogStore.requireSchema(schemaName);
        store.get().createSchema(catalog, tenant, schemaName);
    }
    @Override public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName) {
        String tenant = LanceSessionIdentity.tenant(session);
        return store.get().list(catalog, tenant).stream()
                .filter(table -> schemaName.isEmpty() || schemaName.orElseThrow().equals(table.schema())).toList();
    }
    @Override public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
        String tenant = LanceSessionIdentity.tenant(session);
        return store.get().load(catalog, tenant, tableName);
    }

    @Override
    public java.util.Optional<org.pangrova.spi.connector.metadata.table.TableIdentity> getTableIdentity(
            ConnectorSession session, ConnectorTableHandle table) {
        var live = requireTable(session, table);
        return java.util.Optional.of(new org.pangrova.spi.connector.metadata.table.TableIdentity(
                "LANCE", live.tableId(), live.datasetUri(), LanceTableHandle.hex(live.schemaFence())));
    }

    @Override
    public java.util.Optional<org.pangrova.spi.connector.metadata.table.TableVersionToken> observeVersion(
            ConnectorSession session, ConnectorTableHandle table) {
        var live = requireTable(session, table);
        return java.util.Optional.of(org.pangrova.spi.connector.metadata.table.TableVersionToken.of(
                "LANCE", live.tableId(), Long.toString(live.version()), Long.toString(live.revision())));
    }

    @Override
    public org.pangrova.spi.connector.metadata.table.TableCapabilities getTableCapabilities(
            ConnectorSession session, ConnectorTableHandle table) {
        return table instanceof LanceTableHandle
                ? LanceTableHandle.CAPABILITIES
                : org.pangrova.spi.connector.metadata.table.TableCapabilities.UNSUPPORTED;
    }
    @Override public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
        var pinned = snapshots.resolve(session, requireTable(session, table));
        return new ConnectorTableMetadata(pinned.table(), pinned.columns().stream().map(LanceColumn::name).toList(),
                pinned.columns().stream().map(LanceColumn::metadata).toList(), pinned.properties());
    }
    @Override public ConnectorTableReadMetadata getTableReadMetadata(ConnectorSession session, ConnectorTableHandle table) {
        return new ConnectorTableReadMetadata(getTableMetadata(session, table),
                ConnectorTableReadMetadata.DeclarationEvidence.EXACT);
    }
    @Override public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle table) {
        Map<String, ColumnHandle> columns = new LinkedHashMap<>();
        snapshots.resolve(session, requireTable(session, table)).columns().forEach(column ->
                columns.put(column.name(), new LanceColumnHandle(column.name())));
        return java.util.Collections.unmodifiableMap(columns);
    }
    @Override public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle table) {
        return new TableStatistics(snapshots.resolve(session, requireTable(session, table)).rowCount(),
                -1, -1, java.util.Map.of());
    }
    @Override public org.pangrova.spi.connector.metadata.table.ConnectorReadBinding bindRead(
            ConnectorSession session, ConnectorTableHandle table) {
        var selected = snapshots.resolve(session, requireTable(session, table));
        return new org.pangrova.spi.connector.metadata.table.ConnectorReadBinding(selected,
                getTableReadMetadata(session, selected),
                new TableStatistics(selected.rowCount(), -1, -1, java.util.Map.of()),
                selected.version() == 0
                        ? org.pangrova.spi.connector.metadata.table.ConnectorReadBinding.SnapshotState.EMPTY
                        : org.pangrova.spi.connector.metadata.table.ConnectorReadBinding.SnapshotState.SNAPSHOT,
                selected.version() == 0 ? java.util.OptionalLong.empty()
                        : java.util.OptionalLong.of(selected.version()),
                selected.tableId() + ":" + selected.version());
    }
    @Override public void validateReadBinding(ConnectorSession session,
            org.pangrova.spi.connector.metadata.table.ConnectorReadBinding binding) {
        var selected = requireTable(session, binding.handle());
        LanceSessionIdentity.requireTenant(session, selected.tenantId());
        if (!binding.sourceIdentity().equals(selected.tableId() + ":" + selected.version())) {
            throw new IllegalStateException("Selected dataset identity differs");
        }
        var current = store.get().load(catalog, selected.tenantId(), selected.table());
        if (current == null || !current.tableId().equals(selected.tableId())
                || !current.datasetUri().equals(selected.datasetUri())) {
            throw new IllegalStateException("Selected dataset was dropped or replaced");
        }
        if (binding.state() != org.pangrova.spi.connector.metadata.table.ConnectorReadBinding.SnapshotState.EMPTY) {
            snapshots.resolve(session, selected);
        }
    }
    @Override public ConnectorOutputTableHandle createTable(ConnectorSession session, ConnectorTableMetadata metadata) {
        String tenant = LanceSessionIdentity.tenant(session);
        if (metadata.typedColumns().isEmpty() || metadata.columns().size() != metadata.typedColumns().size()) {
            throw new IllegalArgumentException("Lance CREATE requires exact ordered typed columns");
        }
        List<LanceColumn> columns = metadata.typedColumns().stream().map(LanceColumn::from).toList();
        if (!columns.stream().map(LanceColumn::name).toList().equals(metadata.columns())) {
            throw new IllegalArgumentException("Lance CREATE column order differs from typed schema");
        }
        byte[] fence = nativeControl().schemaFingerprint(session, metadata.typedColumns());
        String tableId = UUID.randomUUID().toString();
        var table = new LanceTableHandle(catalog, tenant, metadata.table(), tableId,
                warehouse + "/" + tableId + ".lance", columns, metadata.properties(), fence, 0, 1, 0);
        store.get().create(table);
        return allocate(table, NativeWriteAllocation.Operation.CREATE, "INSERT");
    }
    @Override public ConnectorOutputTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle table) {
        var pinned = requireTable(session, table);
        return allocate(pinned, pinned.version() == 0
                ? NativeWriteAllocation.Operation.CREATE : NativeWriteAllocation.Operation.APPEND, "INSERT");
    }
    @Override public ConnectorOutputTableHandle beginOverwrite(ConnectorSession session, ConnectorTableHandle table) {
        var pinned = requireTable(session, table);
        return allocate(pinned, pinned.version() == 0
                ? NativeWriteAllocation.Operation.CREATE : NativeWriteAllocation.Operation.OVERWRITE, "OVERWRITE");
    }
    @Override public void dropTable(ConnectorSession session, ConnectorTableHandle table) {
        store.get().drop(requireTable(session, table));
    }

    @Override public void finishDistributedWrite(ConnectorSession session, ConnectorOutputTableHandle output,
            List<byte[]> fragments, DistributedWriteCommitContext context) {
        if (!(output instanceof LanceOutputHandle lance)) {
            throw new IllegalArgumentException("Expected Lance output handle");
        }
        requireTable(session, lance.table());
        if (!context.tenantId().equals(lance.table().tenantId())
                || context.baseSnapshotId() != lance.table().version()) {
            throw new SecurityException("Lance write intent tenant or base snapshot mismatch");
        }
        long rows = LanceCommitFragments.validate(lance, fragments, context);
        var accepted = store.get().accepted(lance, fragments, context);
        if (accepted.isEmpty()) {
            var reservation = store.get().reserve(lance, fragments, context);
            var preparation = nativeControl().prepareWrite(session, output, fragments, context, reservation);
            if (preparation.outcome() != org.pangrova.spi.connector.metadata.NativeWritePreparation.Outcome.PREPARED) {
                throw new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                        "Native dataset preparation is UNKNOWN; reserved objects remain held for replay");
            }
            accepted = Optional.of(store.get().accept(
                    lance, reservation, preparation.publication().orElseThrow(), rows));
        }
        var reference = accepted.orElseThrow().reference();
        NativeWriteFinalization publication = new NativeWriteFinalization(NativeWriteFinalization.Outcome.COMMITTED,
                reference.version(), reference.manifestSha256());
        cleanupAfterBarrier(session, lance, context);
        notifyCommitted(lance, context, publication);
    }

    @Override public org.pangrova.spi.connector.metadata.DistributedWriteAbortReceipt abortDistributedWrite(
            ConnectorSession session, ConnectorOutputTableHandle output,
            List<byte[]> fragments, DistributedWriteCommitContext context) {
        if (!(output instanceof LanceOutputHandle lance)) {
            throw new IllegalArgumentException("Expected Lance output handle");
        }
        requireTable(session, lance.table());
        if (!context.tenantId().equals(lance.table().tenantId())
                || context.baseSnapshotId() != lance.table().version()) {
            throw new SecurityException("Lance abort intent tenant or base snapshot mismatch");
        }
        LanceCommitFragments.validate(lance, fragments, context);
        var receipt = store.get().abort(lance, context);
        cleanupAfterBarrier(session, lance, context);
        return receipt;
    }

    @Override public void abortDistributedWrite(ConnectorSession session, ConnectorOutputTableHandle output,
            List<byte[]> fragments) {
        throw new UnsupportedOperationException("Lance abort requires its exact durable journal context");
    }

    private void cleanupAfterBarrier(
            ConnectorSession session, LanceOutputHandle output,
            DistributedWriteCommitContext context) {
        try {
            nativeControl().cleanupSourceObjects(session, store.get().reservation(output, context));
        } catch (RuntimeException held) {
            // Source visibility/abort is already committed. Keep its truthful receipt while
            // the durable DELETING/ISSUED facts retain objects for exact reconciliation.
            LOG.warn("event=lance_source_cleanup_held executionId={} fingerprint={}",
                    context.executionId(), context.commitFingerprint(), held);
        }
    }

    private void notifyCommitted(LanceOutputHandle output, DistributedWriteCommitContext context,
                                 NativeWriteFinalization publication) {
        var listener = commitListener;
        if (listener == null) {
            return;
        }
        var type = output.allocation().operation() == NativeWriteAllocation.Operation.OVERWRITE
                ? org.pangrova.spi.connector.index.CommitEventType.OVERWRITE
                : org.pangrova.spi.connector.index.CommitEventType.APPEND;
        // Dataset partitions are not file paths. Preserve their exact version without inventing FileSplits.
        listener.onCommit(new org.pangrova.spi.connector.index.CommitEvent(context.tenantId(), catalog,
                output.table().table().schema(), output.table().table().table(), List.of(), List.of(),
                java.time.Instant.now(), Long.toString(publication.publishedVersion()), type,
                context.commitFingerprint()));
    }

    private NativeDatasetControlPlane nativeControl() {
        return controlPlane.orElseThrow(() -> new ConnectorException(PangrovaErrorCode.CONNECTOR_CONFIGURATION_ERROR,
                "Lance catalog mutation requires the Coordinator Native dataset control port"));
    }
    private LanceTableHandle requireTable(ConnectorSession session, ConnectorTableHandle table) {
        if (!(table instanceof LanceTableHandle lance) || !catalog.equals(lance.catalog())
                || !lance.datasetUri().startsWith(warehouse + "/")) {
            throw new IllegalArgumentException("Lance table belongs to another catalog or warehouse");
        }
        LanceSessionIdentity.requireTenant(session, lance.tenantId());
        return lance;
    }
    private LanceOutputHandle allocate(LanceTableHandle table,
            NativeWriteAllocation.Operation operation, String mode) {
        String writeId = UUID.randomUUID().toString();
        return new LanceOutputHandle(table, new NativeWriteAllocation(table.datasetUri(), operation, table.version(),
                table.schemaFence(), writeId, "_pangrova/write-staging/" + writeId, UUID.randomUUID().toString(),
                table.columns().stream().map(LanceColumn::name).toList()), mode,
                table.version() == 0 ? null : store.get().loadSnapshot(table).reference());
    }
    private record LanceColumnHandle(String columnName) implements ColumnHandle {}
}
