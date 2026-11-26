package org.pangrova.connector.iceberg;

import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.pangrova.common.util.DigestUtils;
import org.pangrova.connector.api.metadata.table.TableIdentity;
import org.pangrova.connector.iceberg.handle.IcebergTableHandle;
import org.pangrova.generation.codec.column.GeneratedColumnMetadataCodec;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;

/** Physical schema and independently proven declaration properties of one exact SDK snapshot. */
record IcebergReadSchema(Schema schema, String schemaJson, Optional<Map<String, String>> declarationProperties,
        TableIdentity tableIdentity) {
    private static final int MAX_METADATA_FILES = 1024;

    static IcebergReadSchema resolve(IcebergTableHandle handle, Table table) {
        return resolve(handle, table, currentMetadata(table), null);
    }

    static IcebergReadSchema resolve(IcebergTableHandle handle, Table table, TableMetadata captured,
            String currentSchemaJson) {
        requireLocation(handle, table);
        var snapshot = historicalSchemaSnapshot(handle, captured);
        var schema = selectedSchema(handle, captured, snapshot);
        if (usesBoundCurrentSchema(handle)) {
            var properties = new java.util.LinkedHashMap<>(handle.properties());
            properties.remove(IcebergTableHandle.SOURCE_IDENTITY_PROPERTY);
            return selected(schema, Optional.of(Map.copyOf(properties)), captured, currentSchemaJson);
        }
        if (snapshot == null) {
            return selected(schema, Optional.of(Map.copyOf(captured.properties())),
                    captured, currentSchemaJson);
        }
        var summary = snapshot.summary();
        if (summary.containsKey(GeneratedColumnMetadataCodec.PAYLOAD_PROPERTY)
                || summary.containsKey(GeneratedColumnMetadataCodec.SHA256_PROPERTY)) {
            return selected(schema, Optional.of(IcebergSnapshotDeclarations.properties(summary)),
                    captured, currentSchemaJson);
        }
        return selected(schema, creationProperties(table, snapshot, captured), captured, currentSchemaJson);
    }

    /** Selects the physical projection after split admission has checked the live snapshot. */
    static Schema scanSchema(IcebergTableHandle handle, Table table) {
        var captured = currentMetadata(table);
        requireLocation(handle, table);
        return selectedSchema(handle, captured, historicalSchemaSnapshot(handle, captured));
    }

    private static TableMetadata currentMetadata(Table table) {
        if (!(table instanceof HasTableOperations operations)) {
            throw new IllegalStateException("Read schema evidence requires actual table metadata");
        }
        return operations.operations().current();
    }

    private static void requireLocation(IcebergTableHandle handle, Table table) {
        if (!table.location().equals(handle.tableLocation())) {
            throw new IllegalArgumentException("Iceberg metadata belongs to a different table");
        }
    }

    private static boolean usesBoundCurrentSchema(IcebergTableHandle handle) {
        return IcebergTableHandle.READ_MODE_FULL.equals(handle.readMode())
                && handle.properties().containsKey(IcebergTableHandle.SOURCE_IDENTITY_PROPERTY);
    }

    private static Snapshot historicalSchemaSnapshot(IcebergTableHandle handle, TableMetadata captured) {
        if (usesBoundCurrentSchema(handle) || handle.snapshotId().isEmpty()) return null;
        var snapshot = captured.snapshot(handle.snapshotId().orElseThrow());
        if (snapshot == null || snapshot.schemaId() == null) {
            throw new IllegalStateException("Iceberg pinned snapshot or its schema identity is unavailable");
        }
        return snapshot;
    }

    private static Schema selectedSchema(IcebergTableHandle handle, TableMetadata captured, Snapshot snapshot) {
        if (snapshot == null) {
            return usesBoundCurrentSchema(handle) ? SchemaParser.fromJson(handle.schemaJson()) : captured.schema();
        }
        var schema = captured.schemasById().get(snapshot.schemaId());
        if (schema == null) {
            throw new IllegalStateException("Iceberg pinned snapshot schema is unavailable");
        }
        return schema;
    }

    private static IcebergReadSchema selected(Schema schema, Optional<Map<String, String>> properties,
            TableMetadata captured, String currentSchemaJson) {
        String schemaJson = schema == captured.schema() && currentSchemaJson != null
                ? currentSchemaJson : SchemaParser.toJson(schema);
        return new IcebergReadSchema(schema, schemaJson, properties, identity(captured, schemaJson));
    }

    static TableIdentity identity(TableMetadata metadata, String schemaJson) {
        return new TableIdentity("ICEBERG", metadata.uuid(), metadata.location(), DigestUtils.sha256Hex(schemaJson));
    }

    private static Optional<Map<String, String>> creationProperties(Table table, Snapshot snapshot,
            TableMetadata captured) {
        var current = captured;
        String uuid = current.uuid();
        var visited = new HashSet<String>();
        for (int depth = 0; depth < MAX_METADATA_FILES; depth++) {
            requireIdentity(current, table, uuid, snapshot);
            var files = current.previousFiles();
            if (files.isEmpty()) return Optional.empty();
            var previous = files.get(files.size() - 1);
            if (!visited.add(previous.file())) {
                throw new IllegalStateException("Iceberg metadata history contains a cycle");
            }
            var loaded = readMetadata(table, previous.file());
            if (loaded.isEmpty()) return Optional.empty();
            var predecessor = loaded.orElseThrow();
            requireTableIdentity(predecessor, table, uuid);
            if (predecessor.lastUpdatedMillis() != previous.timestampMillis()) {
                throw new IllegalStateException("Iceberg metadata history identity or timestamp differs");
            }
            if (predecessor.snapshot(snapshot.snapshotId()) == null) {
                // Only an adjacent persisted predecessor proves where this snapshot was introduced.
                // Choosing a file by timestamp or the earliest retained log entry is not sufficient.
                return Optional.of(IcebergSnapshotDeclarations.properties(current.properties()));
            }
            current = predecessor;
        }
        return Optional.empty();
    }

    private static Optional<TableMetadata> readMetadata(Table table, String location) {
        try {
            return Optional.of(TableMetadataParser.read(table.io(), location));
        } catch (org.apache.iceberg.exceptions.NotFoundException unavailable) {
            return Optional.empty();
        }
    }

    private static void requireTableIdentity(TableMetadata metadata, Table table, String uuid) {
        if (!uuid.equals(metadata.uuid()) || !table.location().equals(metadata.location())) {
            throw new IllegalStateException("Iceberg metadata history table identity differs");
        }
    }

    private static void requireIdentity(TableMetadata metadata, Table table, String uuid, Snapshot snapshot) {
        requireTableIdentity(metadata, table, uuid);
        var actual = metadata.snapshot(snapshot.snapshotId());
        var schema = metadata.schemasById().get(snapshot.schemaId());
        if (schema == null || !schema.sameSchema(table.schemas().get(snapshot.schemaId()))) {
            throw new IllegalStateException("Iceberg metadata history differs from the exact pinned schema");
        }
        requireSnapshotIdentity(actual, snapshot);
    }

    private static void requireSnapshotIdentity(Snapshot actual, Snapshot snapshot) {
        if (actual == null || !snapshot.schemaId().equals(actual.schemaId())
                || snapshot.sequenceNumber() != actual.sequenceNumber()
                || snapshot.timestampMillis() != actual.timestampMillis()
                || !snapshot.manifestListLocation().equals(actual.manifestListLocation())) {
            throw new IllegalStateException("Iceberg metadata history differs from the exact pinned snapshot");
        }
    }
}
