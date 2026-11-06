package org.pangrova.connector.paimon.scan;

import org.pangrova.common.api.annotation.Internal;

import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.RowType;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.connector.paimon.handle.PaimonTableHandle;
import org.pangrova.connector.paimon.handle.PaimonTableHandleFactory;
import org.pangrova.connector.api.error.ConnectorException;

import java.util.Map;

/** Reads schema evidence from the exact Paimon snapshot, never from current columns for an old pin. */
@Internal
public record PaimonReadSchema(RowType rowType, Map<String, String> options, String schemaFingerprint) {
    public PaimonReadSchema {
        options = Map.copyOf(options);
        if (schemaFingerprint == null || schemaFingerprint.isBlank()) {
            throw new IllegalArgumentException("Selected read schema requires its captured fingerprint");
        }
    }

    /** Preserves SDK field IDs and declared types from this resolved schema. */
    public Map<String, org.pangrova.connector.api.metadata.table.ColumnHandle> columnHandles() {
        var handles = new java.util.LinkedHashMap<String, org.pangrova.connector.api.metadata.table.ColumnHandle>();
        for (var field : rowType.getFields()) {
            var type = org.pangrova.connector.paimon.type.PaimonTypeMapper.toPangrovaType(
                    field.type(), field.name(), options);
            handles.put(field.name(), new org.pangrova.connector.paimon.handle.PaimonColumnHandle(
                    field.id(), field.name(), type));
        }
        return handles;
    }

    /** Resolves the schema and options belonging to the exact SDK snapshot. */
    public static PaimonReadSchema resolve(PaimonTableHandle handle, Table current) {
        if ("true".equals(handle.properties().get(PaimonTableHandle.CURRENT_SCHEMA_PROPERTY))) {
            if (!(current instanceof FileStoreTable table)
                    || !table.location().toString().equals(handle.tableLocation())) {
                throw new IllegalArgumentException("Selected current schema belongs to a different table");
            }
            var properties = new java.util.LinkedHashMap<>(handle.properties());
            properties.remove(PaimonTableHandle.CURRENT_SCHEMA_PROPERTY);
            properties.remove(PaimonTableHandle.SOURCE_IDENTITY_PROPERTY);
            return new PaimonReadSchema(PaimonTableHandleFactory.parseRowType(handle), properties,
                    handle.schemaFingerprint());
        }
        if (handle.snapshotId().isEmpty()) {
            var identity = PaimonTableHandleFactory.fromTable(handle.schemaName(), handle.tableName(), current);
            if (!identity.tableLocation().equals(handle.tableLocation())) {
                throw new IllegalArgumentException("Paimon metadata belongs to a different table");
            }
            return new PaimonReadSchema(current.rowType(), identity.properties(), identity.schemaFingerprint());
        }
        long version = handle.snapshotId().orElseThrow();
        var table = requirePinnedTable(handle, current, version);
        var snapshot = table.snapshotManager().snapshot(version);
        if (!table.schemaManager().schemaExists(snapshot.schemaId())) {
            throw missingSnapshot(table, version, "exact schema-id=" + snapshot.schemaId() + " is absent");
        }
        var schema = table.schemaManager().schema(snapshot.schemaId());
        if (schema == null || schema.id() != snapshot.schemaId()) {
            throw missingSnapshot(table, version, "exact schema-id=" + snapshot.schemaId() + " is unavailable");
        }
        return new PaimonReadSchema(schema.logicalRowType(), schema.options(),
                org.pangrova.connector.paimon.handle.PaimonTableIdentity.schemaFingerprint(schema));
    }

    private static FileStoreTable requirePinnedTable(PaimonTableHandle handle, Table current, long version) {
        if (!(current instanceof FileStoreTable table) || version <= 0
                || !table.location().toString().equals(handle.tableLocation())) {
            throw new IllegalArgumentException(
                    "Pinned Paimon metadata belongs to a different table or invalid snapshot");
        }
        if (!table.snapshotManager().snapshotExists(version)) {
            throw missingSnapshot(table, version, "does not exist");
        }
        return table;
    }

    private static ConnectorException missingSnapshot(FileStoreTable table, long version, String reason) {
        return new ConnectorException(PangrovaErrorCode.CONNECTOR_SNAPSHOT_NOT_FOUND,
                "Snapshot not found: snapshot-id=" + version + " " + reason + " in table '" + table.name() + "'");
    }
}
