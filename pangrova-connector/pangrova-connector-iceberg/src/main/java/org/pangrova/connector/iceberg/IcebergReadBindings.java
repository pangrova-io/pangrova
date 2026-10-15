package org.pangrova.connector.iceberg;

import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.pangrova.common.util.DigestUtils;
import org.pangrova.connector.iceberg.handle.IcebergSystemTableHandle;
import org.pangrova.connector.iceberg.handle.IcebergManifestTableHandle;
import org.pangrova.connector.iceberg.handle.IcebergTableHandle;
import org.pangrova.connector.iceberg.handle.IcebergTableHandleFactory;
import org.pangrova.spi.connector.metadata.table.ConnectorReadBinding;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.ConnectorTableReadMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.connector.metadata.table.TableStatistics;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

/** Binds immutable data snapshot evidence without changing ordinary current-schema semantics. */
final class IcebergReadBindings {
    private IcebergReadBindings() {}

    static ConnectorReadBinding bind(IcebergCatalogTables tables, ConnectorTableHandle selector) {
        if (selector instanceof IcebergManifestTableHandle manifests) {
            IcebergManifestReadBindings.validate(manifests, tables.resolveTable(manifests.baseTable()));
            var columns = manifests.typedColumns();
            var metadata = new ConnectorTableMetadata(
                    new SchemaTableName(manifests.baseTable().schemaName(), manifests.tableName()),
                    columns.stream().map(ConnectorTableMetadata.Column::name).toList(), columns, Map.of());
            return new ConnectorReadBinding(manifests, new ConnectorTableReadMetadata(metadata,
                    ConnectorTableReadMetadata.DeclarationEvidence.EXACT), TableStatistics.UNKNOWN,
                    ConnectorReadBinding.SnapshotState.METADATA, OptionalLong.empty(),
                    manifestSourceIdentity(manifests));
        }
        if (selector instanceof IcebergSystemTableHandle system) {
            var columns = system.typedColumns();
            var metadata = new ConnectorTableMetadata(
                    new SchemaTableName(system.baseTable().schemaName(), system.tableName()),
                    columns.stream().map(ConnectorTableMetadata.Column::name).toList(), columns, Map.of());
            return new ConnectorReadBinding(system, new ConnectorTableReadMetadata(metadata,
                    ConnectorTableReadMetadata.DeclarationEvidence.EXACT), TableStatistics.UNKNOWN,
                    ConnectorReadBinding.SnapshotState.METADATA, OptionalLong.empty(), system.metadataLocation());
        }
        var handle = (IcebergTableHandle) selector;
        Table table = tables.resolveTable(handle);
        Snapshot snapshot = selectedSnapshot(table, handle);
        String identity = identity(table, snapshot);
        var properties = new LinkedHashMap<>(table.properties());
        properties.put(IcebergTableHandle.SOURCE_IDENTITY_PROPERTY, identity);
        var declaration = tables.getTableReadMetadata(handle, table);
        var schema = handle.snapshotId().isPresent()
                ? IcebergReadSchema.resolve(handle, table).schema() : table.schema();
        var fresh = IcebergTableHandleFactory.fromTable(handle.schemaName(), handle.tableName(), table);
        var bound = new IcebergTableHandle(fresh.schemaName(), fresh.tableName(), fresh.tableLocation(),
                snapshot == null ? Optional.empty() : Optional.of(snapshot.snapshotId()),
                org.apache.iceberg.SchemaParser.toJson(schema), fresh.partitionSpecJson(),
                fresh.formatVersion(), properties, handle.readMode(), handle.fromSnapshotId(),
                handle.toSnapshotId(), handle.pushedLimit());
        TableStatistics statistics = snapshot == null ? new TableStatistics(0, 0)
                : IcebergStatisticsCollector.buildStatistics(snapshot, table);
        return new ConnectorReadBinding(bound, declaration, statistics,
                snapshot == null ? ConnectorReadBinding.SnapshotState.EMPTY
                        : ConnectorReadBinding.SnapshotState.SNAPSHOT,
                snapshot == null ? OptionalLong.empty() : OptionalLong.of(snapshot.snapshotId()), identity);
    }

    static void validate(IcebergCatalogTables tables, ConnectorReadBinding binding) {
        if (binding.handle() instanceof IcebergManifestTableHandle manifests) {
            String identity = manifestSourceIdentity(manifests);
            if (!identity.equals(binding.sourceIdentity())) {
                throw new IllegalStateException("Selected Iceberg manifest-list binding identity differs");
            }
            IcebergManifestReadBindings.validate(manifests, tables.resolveTable(manifests.baseTable()));
            return;
        }
        if (binding.handle() instanceof IcebergSystemTableHandle system) {
            if (!system.metadataLocation().equals(binding.sourceIdentity())) {
                throw new IllegalStateException("Selected metadata object identity differs");
            }
            return;
        }
        var handle = (IcebergTableHandle) binding.handle();
        Table table = tables.resolveTable(handle);
        // An empty view remains empty after a first write; only its table identity is validated.
        Snapshot snapshot = binding.state() == ConnectorReadBinding.SnapshotState.EMPTY
                ? null : selectedSnapshot(table, handle);
        if (!binding.sourceIdentity().equals(identity(table, snapshot))) {
            throw new IllegalStateException("Selected table or snapshot identity changed");
        }
    }

    static void validateSelected(Table table, IcebergTableHandle handle) {
        String expected = handle.properties().get(IcebergTableHandle.SOURCE_IDENTITY_PROPERTY);
        if (expected != null && !expected.equals(identity(table, selectedSnapshot(table, handle)))) {
            throw new IllegalStateException("Selected table or snapshot identity changed before split planning");
        }
    }

    static Snapshot selectedSnapshot(Table table, IcebergTableHandle handle) {
        if (!table.location().equals(handle.tableLocation())) {
            throw new IllegalStateException("Selected table location changed");
        }
        if (handle.snapshotId().isEmpty()) return table.currentSnapshot();
        Snapshot snapshot = table.snapshot(handle.snapshotId().orElseThrow());
        if (snapshot == null) {
            throw new IllegalStateException("Selected snapshot is unavailable: " + handle.snapshotId().orElseThrow());
        }
        return snapshot;
    }

    private static String identity(Table table, Snapshot snapshot) {
        if (!(table instanceof HasTableOperations operations)) {
            throw new IllegalStateException("Selected read requires actual table identity metadata");
        }
        String uuid = operations.operations().current().uuid();
        if (uuid == null || uuid.isBlank()) throw new IllegalStateException("Table UUID is unavailable");
        String selected = snapshot == null ? "EMPTY" : snapshot.snapshotId() + "\0"
                + snapshot.schemaId() + "\0" + snapshot.sequenceNumber() + "\0"
                + snapshot.timestampMillis() + "\0" + snapshot.manifestListLocation();
        return DigestUtils.sha256Hex(uuid + "\0" + table.location() + "\0" + selected);
    }

    private static String manifestSourceIdentity(IcebergManifestTableHandle manifests) {
        return manifests.empty() ? manifests.tableUuid()
                : manifests.manifestListLocation() + "#" + manifests.manifestListSha256();
    }
}
