package org.pangrova.connector.iceberg;

import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.pangrova.connector.iceberg.handle.IcebergTableHandle;
import org.pangrova.generation.codec.column.GeneratedColumnMetadataCodec;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;

/** Physical schema and independently proven declaration properties of one exact SDK snapshot. */
record IcebergReadSchema(Schema schema, Optional<Map<String, String>> declarationProperties,
        org.pangrova.connector.api.metadata.table.TableIdentity tableIdentity) {
    private static final int MAX_METADATA_FILES = 1024;

    static IcebergReadSchema resolve(IcebergTableHandle handle, Table table) {
        if (!table.location().equals(handle.tableLocation())) {
            throw new IllegalArgumentException("Iceberg metadata belongs to a different table");
        }
        if (!(table instanceof HasTableOperations operations)) {
            throw new IllegalStateException("Read schema evidence requires actual table metadata");
        }
        var captured = operations.operations().current();
        if (IcebergTableHandle.READ_MODE_FULL.equals(handle.readMode())
                && handle.properties().containsKey(IcebergTableHandle.SOURCE_IDENTITY_PROPERTY)) {
            var properties = new java.util.LinkedHashMap<>(handle.properties());
            properties.remove(IcebergTableHandle.SOURCE_IDENTITY_PROPERTY);
            return selected(org.apache.iceberg.SchemaParser.fromJson(handle.schemaJson()),
                    Optional.of(Map.copyOf(properties)), captured);
        }
        if (handle.snapshotId().isEmpty()) {
            return selected(captured.schema(), Optional.of(Map.copyOf(captured.properties())), captured);
        }
        var snapshot = captured.snapshot(handle.snapshotId().orElseThrow());
        if (snapshot == null || snapshot.schemaId() == null) {
            throw new IllegalStateException("Iceberg pinned snapshot or its schema identity is unavailable");
        }
        var schema = captured.schemasById().get(snapshot.schemaId());
        if (schema == null) {
            throw new IllegalStateException("Iceberg pinned snapshot schema is unavailable");
        }
        var summary = snapshot.summary();
        if (summary.containsKey(GeneratedColumnMetadataCodec.PAYLOAD_PROPERTY)
                || summary.containsKey(GeneratedColumnMetadataCodec.SHA256_PROPERTY)) {
            return selected(schema, Optional.of(IcebergSnapshotDeclarations.properties(summary)), captured);
        }
        return selected(schema, creationProperties(table, snapshot, captured), captured);
    }

    private static IcebergReadSchema selected(Schema schema, Optional<Map<String, String>> properties,
            TableMetadata captured) {
        return new IcebergReadSchema(schema, properties,
                new org.pangrova.connector.api.metadata.table.TableIdentity("ICEBERG", captured.uuid(),
                        captured.location(), org.pangrova.common.util.DigestUtils.sha256Hex(
                                org.apache.iceberg.SchemaParser.toJson(schema))));
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
