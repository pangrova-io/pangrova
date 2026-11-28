package org.pangrova.connector.paimon.scan;

import org.pangrova.common.api.annotation.Internal;

import org.apache.paimon.Snapshot;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.SnapshotManager;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.connector.paimon.handle.PaimonTableHandle;
import org.pangrova.connector.paimon.handle.PaimonTableHandleFactory;
import org.pangrova.connector.api.exception.ConnectorException;

import java.io.FileNotFoundException;
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
        return resolve(handle, current, null);
    }

    /** Reuses the binding's already observed snapshot when resolving historical schema evidence. */
    public static PaimonReadSchema resolve(PaimonTableHandle handle, Table current, Snapshot selected) {
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
        Snapshot snapshot = selected;
        if (snapshot == null) {
            try {
                snapshot = readSnapshot(table, version);
            } catch (FileNotFoundException missing) {
                throw missingSnapshot(table, version, "does not exist", missing);
            }
        }
        if (snapshot.id() != version) {
            throw new IllegalStateException("Selected snapshot no longer matches the historical schema request");
        }
        long schemaId = snapshot.schemaId();
        var schemaPath = table.schemaManager().toSchemaPath(schemaId);
        org.apache.paimon.schema.TableSchema schema;
        try {
            schema = SchemaManager.tryFromPath(table.fileIO(), schemaPath);
        } catch (FileNotFoundException missing) {
            throw missingSnapshot(table, version, "exact schema-id=" + schemaId + " is absent", missing);
        }
        if (schema.id() != schemaId) {
            throw missingSnapshot(table, version, "exact schema-id=" + schemaId + " is unavailable", null);
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
        return table;
    }

    /** Reads the current object for an exact snapshot ID in one storage operation. */
    public static Snapshot readSnapshot(FileStoreTable table, long version) throws FileNotFoundException {
        var path = table.snapshotManager().snapshotPath(version);
        var snapshot = SnapshotManager.tryFromPath(table.fileIO(), path);
        if (snapshot.id() != version) {
            throw new IllegalStateException("Snapshot object does not match requested ID " + version);
        }
        return snapshot;
    }

    private static ConnectorException missingSnapshot(FileStoreTable table, long version, String reason,
            FileNotFoundException cause) {
        return new ConnectorException(PangrovaErrorCode.CONNECTOR_SNAPSHOT_NOT_FOUND,
                "Snapshot not found: snapshot-id=" + version + " " + reason + " in table '" + table.name() + "'",
                cause);
    }
}
