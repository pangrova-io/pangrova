package org.pangrova.connector.paimon;

import org.pangrova.connector.paimon.handle.PaimonTableIdentity;

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.FileStoreTable;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.util.DigestUtils;
import org.pangrova.connector.paimon.handle.PaimonTableHandle;
import org.pangrova.connector.paimon.handle.PaimonTableHandleFactory;
import org.pangrova.connector.paimon.scan.PaimonReadSchema;
import org.pangrova.connector.paimon.type.PaimonTypeMapper;
import org.pangrova.connector.api.exception.ConnectorException;
import org.pangrova.connector.api.metadata.table.ConnectorReadBinding;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;
import org.pangrova.connector.api.metadata.table.ConnectorTableReadMetadata;
import org.pangrova.connector.api.metadata.table.SchemaTableName;

import java.util.LinkedHashMap;
import java.util.Optional;
import java.util.OptionalLong;

/** Selects one actual SDK snapshot and preserves its ordinary or historical schema mode. */
final class PaimonReadBindings {
    private PaimonReadBindings() {}

    static ConnectorReadBinding bind(Catalog catalog, PaimonTableHandle selector) {
        FileStoreTable table = load(catalog, selector);
        Snapshot snapshot = selectedSnapshot(table, selector);
        String identity = identity(table, snapshot);
        var schema = PaimonReadSchema.resolve(selector, table);
        var columns = PaimonTypeMapper.toPangrovaColumns(schema.rowType(), schema.options());
        var declaration = new ConnectorTableReadMetadata(new ConnectorTableMetadata(
                new SchemaTableName(selector.schemaName(), selector.tableName()),
                columns.stream().map(ConnectorTableMetadata.Column::name).toList(), columns, schema.options()),
                ConnectorTableReadMetadata.DeclarationEvidence.EXACT);
        var properties = new LinkedHashMap<>(schema.options());
        properties.put(PaimonTableHandle.SOURCE_IDENTITY_PROPERTY, identity);
        if (selector.snapshotId().isEmpty()) properties.put(PaimonTableHandle.CURRENT_SCHEMA_PROPERTY, "true");
        var bound = new PaimonTableHandle(selector.schemaName(), selector.tableName(),
                table.location().toString(), table.uuid(),
                org.pangrova.connector.paimon.handle.PaimonTableIdentity
                        .schemaFingerprint(table.schema()),
                snapshot == null ? Optional.empty() : Optional.of(snapshot.id()),
                org.apache.paimon.utils.JsonSerdeUtil.toFlatJson(schema.rowType()),
                table.primaryKeys(), table.partitionKeys(), properties, selector.pushedLimit(),
                selector.incrementalRange());
        var statistics = PaimonTableStatisticsReader.build(table, snapshot, schema.rowType());
        validateSelected(table, bound);
        return new ConnectorReadBinding(bound, declaration, statistics,
                snapshot == null ? ConnectorReadBinding.SnapshotState.EMPTY
                        : ConnectorReadBinding.SnapshotState.SNAPSHOT,
                snapshot == null ? OptionalLong.empty() : OptionalLong.of(snapshot.id()), identity,
                Optional.of(new org.pangrova.connector.api.metadata.table.ConnectorReadSchemaIdentity(
                        new org.pangrova.connector.api.metadata.table.TableIdentity("PAIMON", table.uuid(),
                                table.location().toString(), schema.schemaFingerprint()),
                        "paimon.branch-fields.v1:" + table.snapshotManager().branch(), columns,
                        org.pangrova.connector.api.metadata.table.ConnectorReadSchemaIdentity.FieldIdStability
                                .SELECTED_SOURCE)));
    }

    static void validate(Catalog catalog, ConnectorReadBinding binding) {
        var handle = (PaimonTableHandle) binding.handle();
        var table = load(catalog, handle);
        var snapshot = binding.state() == ConnectorReadBinding.SnapshotState.EMPTY
                ? null : selectedSnapshot(table, handle);
        if (!binding.sourceIdentity().equals(identity(table, snapshot))) {
            throw new IllegalStateException("Selected source identity changed");
        }
    }

    static void validateSelected(FileStoreTable table, PaimonTableHandle handle) {
        String expected = handle.properties().get(PaimonTableHandle.SOURCE_IDENTITY_PROPERTY);
        if (expected != null && !expected.equals(identity(table, selectedSnapshot(table, handle)))) {
            throw new IllegalStateException("Selected snapshot changed during split planning");
        }
    }

    static FileStoreTable requireSelectedTable(org.apache.paimon.table.Table table, PaimonTableHandle handle) {
        if (!(table instanceof FileStoreTable fileStore)
                || !fileStore.location().toString().equals(handle.tableLocation())
                || !fileStore.uuid().equals(handle.tableUuid())) {
            throw new IllegalStateException("Selected native table identity changed");
        }
        if ("true".equals(handle.properties().get(PaimonTableHandle.CURRENT_SCHEMA_PROPERTY))) {
            var options = new LinkedHashMap<>(handle.properties());
            options.remove(PaimonTableHandle.SOURCE_IDENTITY_PROPERTY);
            options.remove(PaimonTableHandle.CURRENT_SCHEMA_PROPERTY);
            var currentProperties = PaimonTableHandleFactory.fromTable(
                    handle.schemaName(), handle.tableName(), table).properties();
            if (!org.apache.paimon.utils.JsonSerdeUtil.toFlatJson(table.rowType())
                    .equals(handle.rowTypeJson())
                    || !currentProperties.equals(options)) {
                throw new IllegalStateException(
                        "Selected current schema or read options changed before split planning");
            }
        }
        return fileStore;
    }

    static Snapshot selectedSnapshot(FileStoreTable table, PaimonTableHandle handle) {
        if (!table.location().toString().equals(handle.tableLocation())
                || !table.uuid().equals(handle.tableUuid())) {
            throw new IllegalStateException("Selected table identity changed");
        }
        if (handle.snapshotId().isEmpty()) return table.snapshotManager().latestSnapshot();
        long version = handle.snapshotId().orElseThrow();
        if (!table.snapshotManager().snapshotExists(version)) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_SNAPSHOT_NOT_FOUND,
                    "Selected snapshot is unavailable: " + version);
        }
        return table.snapshotManager().snapshot(version);
    }

    private static FileStoreTable load(Catalog catalog, PaimonTableHandle handle) {
        try {
            var table = PaimonTableIdentity.loadFresh(catalog,
                    Identifier.create(handle.schemaName(), handle.tableName()));
            if (!(table instanceof FileStoreTable fileStore)) {
                throw new ConnectorException(PangrovaErrorCode.CONNECTOR_UNSUPPORTED_OPERATION,
                        "Selected native read requires physical table snapshots");
            }
            return fileStore;
        } catch (Catalog.TableNotExistException e) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_TABLE_NOT_FOUND,
                    "Selected table is unavailable: " + handle.schemaName() + "." + handle.tableName(), e);
        }
    }

    private static String identity(FileStoreTable table, Snapshot snapshot) {
        // The SDK may synthesize uuid() from name and earliest schema time. It is
        // additional evidence, not a replacement for the actual manifest identity.
        return DigestUtils.sha256Hex(table.uuid() + "\0" + table.location() + "\0"
                + (snapshot == null ? "EMPTY" : snapshot.toJson()));
    }
}
