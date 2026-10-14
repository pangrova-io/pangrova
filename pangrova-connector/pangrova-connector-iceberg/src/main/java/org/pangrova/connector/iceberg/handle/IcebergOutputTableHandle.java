package org.pangrova.connector.iceberg.handle;

import org.pangrova.spi.connector.metadata.ConnectorOutputTableHandle;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;

import java.util.Map;
import java.util.TreeMap;

/**
 * Immutable handle for Iceberg table write operations.
 *
 * <p>Contains only immutable metadata fields, including the non-reusable table UUID.
 * The live Iceberg SDK Table and Schema
 * objects are NOT stored here to prevent SDK object leakage. Consumers needing
 * the SDK objects must reload them via catalog API.
 *
 * <p>The {@code schemaJson} field contains the JSON serialization of the write schema,
 * which can be parsed back using {@link IcebergTableHandleFactory#parseSchema}.
 */
public record IcebergOutputTableHandle(
        String schemaName,
        String tableName,
        String tableUuid,
        String tableLocation,
        String schemaJson,
        String partitionSpecJson,
        int formatVersion,
        long baseSnapshotId,
        String writeMode,
        Map<String, String> staticPartitions,
        Map<String, String> declarationProperties) implements ConnectorOutputTableHandle {

    /** Sentinel indicating the table had no snapshot when the write began. */
    public static final long NO_SNAPSHOT = -1L;

    /** Write mode constants. */
    public static final String MODE_INSERT = "INSERT";
    public static final String MODE_OVERWRITE = "OVERWRITE";
    public static final String MODE_DELETE = "DELETE";

    /**
     * Compact constructor with validation.
     */
    public IcebergOutputTableHandle {
        requireNonBlank(schemaName, "schemaName");
        requireNonBlank(tableName, "tableName");
        requireNonBlank(tableUuid, "tableUuid");
        requireNonBlank(tableLocation, "tableLocation");
        requireNonBlank(schemaJson, "schemaJson");
        partitionSpecJson = partitionSpecJson == null ? "{}" : partitionSpecJson;
        if (!MODE_INSERT.equals(writeMode)
                && !MODE_OVERWRITE.equals(writeMode)
                && !MODE_DELETE.equals(writeMode)) {
            throw new IllegalArgumentException("Unsupported Iceberg write mode: " + writeMode);
        }
        staticPartitions = Map.copyOf(new TreeMap<>(staticPartitions));
        if (!staticPartitions.isEmpty() && !MODE_OVERWRITE.equals(writeMode)) {
            throw new IllegalArgumentException("Static partitions require Iceberg OVERWRITE mode");
        }
        staticPartitions.forEach((key, value) -> requireNonBlank(key, "static partition column"));
        declarationProperties = IcebergDeclarationProperties.requireCanonical(declarationProperties);
    }

    @Override
    public Map<String, String> declarationProperties() {
        return Map.copyOf(declarationProperties);
    }

    private static void requireNonBlank(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
    }

    /**
     * A-COMMIT-01: structured target identity so a recovering coordinator can rehydrate this write
     * (the record's {@code writeMode()} accessor already satisfies the SPI's write-mode contract).
     */
    @Override
    public java.util.Optional<SchemaTableName> targetTable() {
        return java.util.Optional.of(new SchemaTableName(schemaName, tableName));
    }

    @Override
    public java.util.OptionalLong writeBaseSnapshotId() {
        return java.util.OptionalLong.of(baseSnapshotId);
    }
}
