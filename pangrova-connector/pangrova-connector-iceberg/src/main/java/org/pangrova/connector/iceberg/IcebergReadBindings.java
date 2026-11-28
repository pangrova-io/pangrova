package org.pangrova.connector.iceberg;

import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.pangrova.connector.api.enums.ReadSnapshotState;
import org.pangrova.connector.api.enums.TableDeclarationEvidence;
import org.pangrova.common.crypto.Sha256Digests;
import org.pangrova.connector.iceberg.handle.IcebergSystemTableHandle;
import org.pangrova.connector.iceberg.handle.IcebergFileTableHandle;
import org.pangrova.connector.iceberg.handle.IcebergPartitionTableHandle;
import org.pangrova.connector.iceberg.handle.IcebergManifestTableHandle;
import org.pangrova.connector.iceberg.handle.IcebergTableHandle;
import org.pangrova.connector.api.metadata.table.ConnectorReadBinding;
import org.pangrova.connector.api.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;
import org.pangrova.connector.api.metadata.table.ConnectorTableReadMetadata;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.connector.api.metadata.table.TableIdentity;
import org.pangrova.connector.api.metadata.table.TableStatistics;
import org.pangrova.connector.api.metadata.table.TableVersionToken;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Consumer;

/** Binds immutable data snapshot evidence without changing ordinary current-schema semantics. */
final class IcebergReadBindings {
    private IcebergReadBindings() {}

    static ConnectorReadBinding bind(IcebergCatalogTables tables, ConnectorTableHandle selector) {
        if (selector instanceof IcebergPartitionTableHandle partitions) {
            for (var input : partitions.derivedQuery().inputs()) validate(tables, input.binding());
            var columns = partitions.typedColumns();
            var metadata = new ConnectorTableMetadata(
                    new SchemaTableName(partitions.baseTable().schemaName(), partitions.tableName()),
                    columns.stream().map(ConnectorTableMetadata.Column::name).toList(), columns, Map.of());
            return new ConnectorReadBinding(partitions, new ConnectorTableReadMetadata(metadata,
                    TableDeclarationEvidence.EXACT), TableStatistics.UNKNOWN,
                    ReadSnapshotState.METADATA, OptionalLong.empty(),
                    partitions.sourceIdentity(), Optional.empty(), Optional.empty());
        }
        if (selector instanceof IcebergFileTableHandle files) {
            IcebergFileReadBindings.validate(files, tables.resolveTable(files.baseTable()));
            var columns = files.typedColumns();
            var metadata = new ConnectorTableMetadata(
                    new SchemaTableName(files.baseTable().schemaName(), files.tableName()),
                    columns.stream().map(ConnectorTableMetadata.Column::name).toList(), columns, Map.of());
            return new ConnectorReadBinding(files, new ConnectorTableReadMetadata(metadata,
                    TableDeclarationEvidence.EXACT), TableStatistics.UNKNOWN,
                    ReadSnapshotState.METADATA, OptionalLong.empty(),
                    fileSourceIdentity(files), java.util.Optional.empty(), Optional.empty());
        }
        if (selector instanceof IcebergManifestTableHandle manifests) {
            IcebergManifestReadBindings.validate(manifests, tables.resolveTable(manifests.baseTable()));
            var columns = manifests.typedColumns();
            var metadata = new ConnectorTableMetadata(
                    new SchemaTableName(manifests.baseTable().schemaName(), manifests.tableName()),
                    columns.stream().map(ConnectorTableMetadata.Column::name).toList(), columns, Map.of());
            return new ConnectorReadBinding(manifests, new ConnectorTableReadMetadata(metadata,
                    TableDeclarationEvidence.EXACT), TableStatistics.UNKNOWN,
                    ReadSnapshotState.METADATA, OptionalLong.empty(),
                    manifestSourceIdentity(manifests), java.util.Optional.empty(), Optional.empty());
        }
        if (selector instanceof IcebergSystemTableHandle system) {
            var columns = system.typedColumns();
            var metadata = new ConnectorTableMetadata(
                    new SchemaTableName(system.baseTable().schemaName(), system.tableName()),
                    columns.stream().map(ConnectorTableMetadata.Column::name).toList(), columns, Map.of());
            return new ConnectorReadBinding(system, new ConnectorTableReadMetadata(metadata,
                    TableDeclarationEvidence.EXACT), TableStatistics.UNKNOWN,
                    ReadSnapshotState.METADATA, OptionalLong.empty(),
                    system.metadataLocation(), Optional.empty(), Optional.empty());
        }
        var handle = (IcebergTableHandle) selector;
        return bindData(tables.resolveTable(handle), handle, null);
    }

    static ConnectorReadBinding bindData(Table table, IcebergTableHandle handle,
            Consumer<Optional<TableIdentity>> beforeSnapshotBinding) {
        return bindData(table, handle, beforeSnapshotBinding, null);
    }

    /** Reuses the encodings produced from the same query-owned JDBC table during discovery. */
    static ConnectorReadBinding bindDiscoveredData(Table table, IcebergTableHandle discovered,
            IcebergTableHandle selected, Consumer<Optional<TableIdentity>> beforeSnapshotBinding) {
        return bindData(table, selected, beforeSnapshotBinding, discovered);
    }

    private static ConnectorReadBinding bindData(Table table, IcebergTableHandle handle,
            Consumer<Optional<TableIdentity>> beforeSnapshotBinding, IcebergTableHandle discovered) {
        var capturedMetadata = metadata(table);
        String currentSchemaJson = discovered == null ? null : discovered.schemaJson();
        if (beforeSnapshotBinding != null) {
            if (currentSchemaJson == null) currentSchemaJson = SchemaParser.toJson(capturedMetadata.schema());
            beforeSnapshotBinding.accept(Optional.of(IcebergReadSchema.identity(capturedMetadata, currentSchemaJson)));
        }
        Snapshot snapshot = selectedSnapshot(table, handle);
        String identity = identity(table, snapshot);
        var properties = new LinkedHashMap<>(table.properties());
        properties.put(IcebergTableHandle.SOURCE_IDENTITY_PROPERTY, identity);
        var readSchema = IcebergReadSchema.resolve(handle, table, capturedMetadata, currentSchemaJson);
        var declaration = IcebergCatalogTables.readDeclaration(handle, readSchema);
        var schema = readSchema.schema();
        var schemaIdentity = declaration.declarationEvidence() == TableDeclarationEvidence.EXACT
                ? Optional.of(new org.pangrova.connector.api.metadata.table.ConnectorReadSchemaIdentity(
                        readSchema.tableIdentity(), "iceberg.table-fields.v1", declaration.metadata().typedColumns(),
                        org.pangrova.connector.api.enums.FieldIdStability
                                .NON_REUSABLE_NAMESPACE))
                : Optional.<org.pangrova.connector.api.metadata.table.ConnectorReadSchemaIdentity>empty();
        String partitionSpecJson = discovered == null
                ? PartitionSpecParser.toJson(capturedMetadata.spec()) : discovered.partitionSpecJson();
        var bound = new IcebergTableHandle(handle.schemaName(), handle.tableName(), table.location(),
                snapshot == null ? Optional.empty() : Optional.of(snapshot.snapshotId()),
                readSchema.schemaJson(), partitionSpecJson,
                capturedMetadata.formatVersion(), properties, handle.readMode(), handle.fromSnapshotId(),
                handle.toSnapshotId(), handle.pushedLimit());
        TableStatistics statistics = snapshot == null ? new TableStatistics(0, 0, 0, Map.of())
                : IcebergStatisticsCollector.planningStatistics(snapshot, table, bound, schema);
        return new ConnectorReadBinding(bound, declaration, statistics,
                snapshot == null ? ReadSnapshotState.EMPTY
                        : ReadSnapshotState.SNAPSHOT,
                snapshot == null ? OptionalLong.empty() : OptionalLong.of(snapshot.snapshotId()), identity,
                schemaIdentity, IcebergTableHandle.READ_MODE_FULL.equals(handle.readMode())
                        && handle.snapshotId().isEmpty() && capturedMetadata.currentSnapshot() == snapshot
                        && capturedMetadata.schema().schemaId() == schema.schemaId()
                        ? Optional.of(currentVersion(capturedMetadata)) : Optional.empty());
    }

    static TableIdentity liveIdentity(TableMetadata metadata) {
        return IcebergReadSchema.identity(metadata, SchemaParser.toJson(metadata.schema()));
    }

    static TableVersionToken currentVersion(TableMetadata metadata) {
        Snapshot current = metadata.currentSnapshot();
        return TableVersionToken.of("ICEBERG", metadata.uuid(),
                current == null ? "EMPTY" : Long.toString(current.snapshotId()),
                current == null ? "0" : Long.toString(current.sequenceNumber()),
                Integer.toString(metadata.schema().schemaId()), Integer.toString(metadata.spec().specId()));
    }

    static void validate(IcebergCatalogTables tables, ConnectorReadBinding binding) {
        if (binding.handle() instanceof IcebergPartitionTableHandle partitions) {
            if (!partitions.sourceIdentity().equals(binding.sourceIdentity())) {
                throw new IllegalStateException("Selected Iceberg partitions binding identity differs");
            }
            for (var input : partitions.derivedQuery().inputs()) validate(tables, input.binding());
            return;
        }
        if (binding.handle() instanceof IcebergFileTableHandle files) {
            if (!fileSourceIdentity(files).equals(binding.sourceIdentity())) {
                throw new IllegalStateException("Selected Iceberg files binding identity differs");
            }
            IcebergFileReadBindings.validate(files, tables.resolveTable(files.baseTable()));
            return;
        }
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
        validate(tables.resolveTable(handle), handle, binding);
    }

    static void validate(Table table, IcebergTableHandle handle, ConnectorReadBinding binding) {
        // An empty view remains empty after a first write; only its table identity is validated.
        Snapshot snapshot = binding.state() == ReadSnapshotState.EMPTY
                ? null : selectedSnapshot(table, handle);
        if (binding.snapshotId().isPresent()
                && (snapshot == null || snapshot.snapshotId() != binding.snapshotId().orElseThrow())) {
            throw new IllegalStateException("Selected table or snapshot identity changed");
        }
        String observed = identity(table, snapshot);
        String selected = handle.properties().get(IcebergTableHandle.SOURCE_IDENTITY_PROPERTY);
        if (!binding.sourceIdentity().equals(observed) || (selected != null && !selected.equals(observed))) {
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
        String uuid = metadata(table).uuid();
        if (uuid == null || uuid.isBlank()) throw new IllegalStateException("Table UUID is unavailable");
        String selected = snapshot == null ? "EMPTY" : snapshot.snapshotId() + "\0"
                + snapshot.schemaId() + "\0" + snapshot.sequenceNumber() + "\0"
                + snapshot.timestampMillis() + "\0" + snapshot.manifestListLocation();
        return Sha256Digests.sha256Hex(uuid + "\0" + table.location() + "\0" + selected);
    }

    private static TableMetadata metadata(Table table) {
        if (!(table instanceof HasTableOperations operations)) {
            throw new IllegalStateException("Selected read requires actual table identity metadata");
        }
        return operations.operations().current();
    }

    private static String manifestSourceIdentity(IcebergManifestTableHandle manifests) {
        return manifests.empty() ? manifests.tableUuid()
                : manifests.manifestListLocation() + "#" + manifests.manifestListSha256();
    }

    private static String fileSourceIdentity(IcebergFileTableHandle files) {
        StringBuilder identity = new StringBuilder(manifestSourceIdentity(files.manifestSelection()));
        identity.append('\0').append(files.manifestSelection().metadataLocation());
        for (var spec : files.manifestSelection().protocolPartitionSpecs()) {
            identity.append('\0').append(java.util.HexFormat.of().formatHex(spec.toByteArray()));
        }
        if (files.entries()) {
            identity.append("\0entries");
        }
        for (var manifest : files.manifests()) {
            identity.append('\0').append(manifest.uri()).append('\0')
                    .append(manifest.length()).append('\0').append(manifest.sha256())
                    .append('\0').append(manifest.specId()).append('\0').append(manifest.content());
        }
        return Sha256Digests.sha256Hex(identity.toString());
    }
}
