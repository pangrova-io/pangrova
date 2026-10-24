package org.pangrova.connector.iceberg.handle;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.PartitionSpecParser;

import java.util.Map;
import java.util.Optional;

/**
 * Factory for creating immutable {@link IcebergTableHandle} and {@link IcebergOutputTableHandle}
 * instances from Iceberg SDK objects.
 *
 * <p>This factory extracts immutable metadata snapshots from live Iceberg Table objects,
 * ensuring that handles contain only serializable, immutable data.
 */
public final class IcebergTableHandleFactory {

    private IcebergTableHandleFactory() {
        // Utility class
    }

    // ========== IcebergTableHandle Factory Methods ==========

    /**
     * Creates an immutable table handle from an Iceberg SDK Table object.
     *
     * @param schemaName the schema (namespace) name
     * @param tableName the table name
     * @param table the live Iceberg SDK Table object
     * @return an immutable handle containing metadata snapshot
     */
    public static IcebergTableHandle fromTable(String schemaName, String tableName, Table table) {
        return fromTable(schemaName, tableName, table, Optional.empty());
    }

    /**
     * Creates an immutable table handle from an Iceberg SDK Table object with optional snapshot.
     *
     * @param schemaName the schema (namespace) name
     * @param tableName the table name
     * @param table the live Iceberg SDK Table object
     * @param snapshotId optional snapshot ID for time-travel queries
     * @return an immutable handle containing metadata snapshot
     */
    public static IcebergTableHandle fromTable(
            String schemaName,
            String tableName,
            Table table,
            Optional<Long> snapshotId) {
        String tableLocation = table.location();
        String schemaJson = SchemaParser.toJson(table.schema());
        String partitionSpecJson = PartitionSpecParser.toJson(table.spec());
        int formatVersion = extractFormatVersion(table);
        Map<String, String> properties = table.properties();

        return new IcebergTableHandle(
                schemaName,
                tableName,
                tableLocation,
                snapshotId,
                schemaJson,
                partitionSpecJson,
                formatVersion,
                properties,
                IcebergTableHandle.READ_MODE_FULL,
                0L,
                0L,
                IcebergTableHandle.NO_LIMIT);
    }

    /**
     * Creates a handle with a specific snapshot ID for time-travel queries.
     *
     * @param schemaName the schema (namespace) name
     * @param tableName the table name
     * @param table the live Iceberg SDK Table object
     * @param snapshotId the snapshot ID for time-travel
     * @return an immutable handle with TIME_TRAVEL read mode
     */
    public static IcebergTableHandle fromTableWithSnapshot(
            String schemaName,
            String tableName,
            Table table,
            long snapshotId) {
        return fromTable(schemaName, tableName, table, Optional.of(snapshotId))
                .withSnapshotId(snapshotId);
    }

    /**
     * Creates a handle for incremental reads between two snapshots.
     *
     * @param schemaName     the schema (namespace) name
     * @param tableName      the table name
     * @param table          the live Iceberg SDK Table object
     * @param fromSnapshotId start snapshot (exclusive)
     * @param toSnapshotId   end snapshot (inclusive)
     * @return an immutable handle with INCREMENTAL read mode
     */
    public static IcebergTableHandle fromTableWithIncrementalRead(
            String schemaName,
            String tableName,
            Table table,
            long fromSnapshotId,
            long toSnapshotId) {
        return fromTable(schemaName, tableName, table, Optional.empty())
                .withIncrementalRead(fromSnapshotId, toSnapshotId);
    }

    // ========== IcebergOutputTableHandle Factory Methods ==========

    /**
     * Creates an immutable output table handle from an Iceberg SDK Table object.
     *
     * @param schemaName the schema (namespace) name
     * @param tableName the table name
     * @param table the live Iceberg SDK Table object
     * @param writeSchema the schema to use for writing
     * @return an immutable output handle containing metadata snapshot
     */
    public static IcebergOutputTableHandle outputFromTable(
            String schemaName,
            String tableName,
            Table table,
            Schema writeSchema) {
        var metadata = ((org.apache.iceberg.HasTableOperations) table).operations().refresh();
        String schemaJson = SchemaParser.toJson(metadata.schema());
        if (!schemaJson.equals(SchemaParser.toJson(writeSchema))) {
            throw new IllegalStateException("Iceberg created write schema changed before allocation");
        }
        return new IcebergOutputTableHandle(
                schemaName,
                tableName,
                metadata.uuid(),
                metadata.location(),
                schemaJson,
                PartitionSpecParser.toJson(metadata.spec()),
                metadata.formatVersion(),
                metadata.currentSnapshot() != null
                        ? metadata.currentSnapshot().snapshotId()
                        : IcebergOutputTableHandle.NO_SNAPSHOT,
                IcebergOutputTableHandle.MODE_INSERT, IcebergOverwriteScope.NONE, java.util.Map.of(),
                IcebergDeclarationProperties.capture(metadata.properties()));
    }

    // ========== Schema/PartitionSpec Parsing ==========

    /**
     * Parses the schema JSON back to an Iceberg Schema object.
     * Useful for consumers that need the schema without loading the full table.
     *
     * @param handle the table handle containing schema JSON
     * @return the parsed Iceberg Schema
     */
    public static Schema parseSchema(IcebergTableHandle handle) {
        return SchemaParser.fromJson(handle.schemaJson());
    }

    /**
     * Parses the schema JSON from an output handle back to an Iceberg Schema object.
     *
     * @param handle the output table handle containing schema JSON
     * @return the parsed Iceberg Schema
     */
    public static Schema parseSchema(IcebergOutputTableHandle handle) {
        return SchemaParser.fromJson(handle.schemaJson());
    }

    /**
     * Parses the partition spec JSON back to an Iceberg PartitionSpec object.
     *
     * @param handle the table handle containing partition spec JSON
     * @param schema the table schema (required for partition spec parsing)
     * @return the parsed Iceberg PartitionSpec
     */
    public static PartitionSpec parsePartitionSpec(IcebergTableHandle handle, Schema schema) {
        return PartitionSpecParser.fromJson(schema, handle.partitionSpecJson());
    }

    /**
     * Parses the partition spec JSON from an output handle.
     *
     * @param handle the output table handle containing partition spec JSON
     * @param schema the table schema (required for partition spec parsing)
     * @return the parsed Iceberg PartitionSpec
     */
    public static PartitionSpec parsePartitionSpec(IcebergOutputTableHandle handle, Schema schema) {
        return PartitionSpecParser.fromJson(schema, handle.partitionSpecJson());
    }

    // ========== Helper Methods ==========

    /**
     * Extracts the format version from an Iceberg table.
     * Defaults to version 1 if not explicitly set.
     */
    private static int extractFormatVersion(Table table) {
        if (table instanceof HasTableOperations sdkTable) {
            return sdkTable.operations().current().formatVersion();
        }
        String versionStr = table.properties().get("format-version");
        if (versionStr != null) {
            try {
                return Integer.parseInt(versionStr);
            } catch (NumberFormatException e) {
                return 1;
            }
        }
        // Default to v1 for older tables
        return 1;
    }
}
