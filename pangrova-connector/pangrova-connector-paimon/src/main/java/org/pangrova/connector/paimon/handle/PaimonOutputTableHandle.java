package org.pangrova.connector.paimon.handle;

import org.pangrova.spi.connector.metadata.ConnectorOutputTableHandle;

import java.util.List;
import java.util.Map;

/**
 * Immutable handle for Paimon table write operations.
 *
 * <p>This handle contains only immutable metadata fields. The live Paimon SDK Table
 * object is NOT stored here to prevent SDK object leakage across module boundaries.
 * Consumers needing the SDK Table must reload it via catalog API.
 *
 * <p>Metadata snapshot fields:
 * <ul>
 *   <li>{@code tableLocation} - Base location URI of the table</li>
 *   <li>{@code rowTypeJson} - JSON serialization of the Paimon RowType schema</li>
 *   <li>{@code primaryKeys} - List of primary key column names</li>
 *   <li>{@code partitionKeys} - List of partition key column names</li>
 * </ul>
 */
public record PaimonOutputTableHandle(
        String schemaName,
        String tableName,
        String tableLocation,
        String rowTypeJson,
        List<String> primaryKeys,
        List<String> partitionKeys,
        Map<String, String> overwritePartition) implements ConnectorOutputTableHandle {

    /**
     * Compact constructor with defensive copies and null safety.
     */
    public PaimonOutputTableHandle {
        requireNonBlank(schemaName, "schemaName");
        requireNonBlank(tableName, "tableName");
        requireNonBlank(tableLocation, "tableLocation");
        requireNonBlank(rowTypeJson, "rowTypeJson");
        primaryKeys = primaryKeys == null ? List.of() : List.copyOf(primaryKeys);
        partitionKeys = partitionKeys == null ? List.of() : List.copyOf(partitionKeys);
        overwritePartition = overwritePartition == null ? Map.of() : Map.copyOf(overwritePartition);
    }

    private static void requireNonBlank(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
    }

    /** Convenience constructor for dynamic overwrite (no static partition filter). */
    public PaimonOutputTableHandle(
            String schemaName,
            String tableName,
            String tableLocation,
            String rowTypeJson,
            List<String> primaryKeys,
            List<String> partitionKeys) {
        this(schemaName, tableName, tableLocation, rowTypeJson,
                primaryKeys, partitionKeys, Map.of());
    }

    /**
     * Returns true if this table has primary keys (LSM merge-on-read table).
     */
    public boolean hasPrimaryKeys() {
        return !primaryKeys.isEmpty();
    }

    /**
     * Returns true if this table is partitioned.
     */
    public boolean isPartitioned() {
        return !partitionKeys.isEmpty();
    }
}
