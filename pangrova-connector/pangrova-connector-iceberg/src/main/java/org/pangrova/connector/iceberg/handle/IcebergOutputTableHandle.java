package org.pangrova.connector.iceberg.handle;

import org.pangrova.spi.connector.metadata.ConnectorOutputTableHandle;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;

/**
 * Immutable handle for Iceberg table write operations.
 *
 * <p>Contains only immutable metadata fields. The live Iceberg SDK Table and Schema
 * objects are NOT stored here to prevent SDK object leakage. Consumers needing
 * the SDK objects must reload them via catalog API.
 *
 * <p>The {@code schemaJson} field contains the JSON serialization of the write schema,
 * which can be parsed back using {@link IcebergTableHandleFactory#parseSchema}.
 */
public record IcebergOutputTableHandle(
        String schemaName,
        String tableName,
        String tableLocation,
        String schemaJson,
        String partitionSpecJson,
        int formatVersion,
        long baseSnapshotId,
        String writeMode) implements ConnectorOutputTableHandle {

    /** Sentinel indicating no snapshot was captured (e.g. for INSERT). */
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
        requireNonBlank(tableLocation, "tableLocation");
        requireNonBlank(schemaJson, "schemaJson");
        partitionSpecJson = partitionSpecJson == null ? "{}" : partitionSpecJson;
        writeMode = writeMode == null ? MODE_INSERT : writeMode;
    }

    private static void requireNonBlank(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
    }

    /** Backward-compatible constructor without snapshot ID and writeMode. */
    public IcebergOutputTableHandle(
            String schemaName, String tableName, String tableLocation,
            String schemaJson, String partitionSpecJson, int formatVersion) {
        this(schemaName, tableName, tableLocation, schemaJson,
                partitionSpecJson, formatVersion, NO_SNAPSHOT, MODE_INSERT);
    }

    /** Constructor with snapshot ID but no writeMode (defaults to INSERT). */
    public IcebergOutputTableHandle(
            String schemaName, String tableName, String tableLocation,
            String schemaJson, String partitionSpecJson, int formatVersion,
            long baseSnapshotId) {
        this(schemaName, tableName, tableLocation, schemaJson,
                partitionSpecJson, formatVersion, baseSnapshotId, MODE_INSERT);
    }

    /**
     * A-COMMIT-01: structured target identity so a recovering coordinator can rehydrate this write
     * (the record's {@code writeMode()} accessor already satisfies the SPI's write-mode contract).
     */
    @Override
    public java.util.Optional<SchemaTableName> targetTable() {
        return java.util.Optional.of(new SchemaTableName(schemaName, tableName));
    }
}
