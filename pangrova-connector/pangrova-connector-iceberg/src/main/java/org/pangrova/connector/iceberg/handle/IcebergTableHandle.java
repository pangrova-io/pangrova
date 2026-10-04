package org.pangrova.connector.iceberg.handle;

import java.util.Map;
import java.util.Optional;

import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;

/**
 * Immutable handle referencing an Iceberg table by schema, name, and metadata snapshot.
 *
 * <p>This handle contains only immutable metadata fields (String, long, Optional, Map.of()).
 * The live Iceberg SDK Table object is NOT stored here to prevent SDK object leakage
 * across module boundaries. Consumers needing the SDK Table must reload it via catalog API.
 *
 * <p>Metadata snapshot fields:
 * <ul>
 *   <li>{@code tableLocation} - Base location URI of the table</li>
 *   <li>{@code snapshotId} - Optional snapshot ID for time-travel queries</li>
 *   <li>{@code schemaJson} - JSON serialization of the Iceberg schema</li>
 *   <li>{@code partitionSpecJson} - JSON serialization of the partition spec</li>
 *   <li>{@code formatVersion} - Iceberg table format version (1 or 2)</li>
 * </ul>
 *
 * <p>Read mode fields (for incremental / time-travel queries):
 * <ul>
 *   <li>{@code readMode} - "FULL" (default), "INCREMENTAL", or "TIME_TRAVEL"</li>
 *   <li>{@code fromSnapshotId} - Start snapshot (exclusive) for incremental reads</li>
 *   <li>{@code toSnapshotId} - End snapshot (inclusive) for incremental reads</li>
 * </ul>
 */
public record IcebergTableHandle(
        String schemaName,
        String tableName,
        String tableLocation,
        Optional<Long> snapshotId,
        String schemaJson,
        String partitionSpecJson,
        int formatVersion,
        Map<String, String> properties,
        String readMode,
        long fromSnapshotId,
        long toSnapshotId,
        long pushedLimit) implements ConnectorTableHandle {

    /** No limit pushed down. */
    public static final long NO_LIMIT = 0L;
    /** Connector-owned evidence for an immutable selected read, not a caller's time-travel hint. */
    public static final String SOURCE_IDENTITY_PROPERTY = "pangrova.read.source_identity";

    /** Default read mode: full snapshot scan. */
    public static final String READ_MODE_FULL = "FULL";
    /** Incremental read: only data files added between two snapshots. */
    public static final String READ_MODE_INCREMENTAL = "INCREMENTAL";
    /** Time-travel read: scan at a specific snapshot. */
    public static final String READ_MODE_TIME_TRAVEL = "TIME_TRAVEL";

    /**
     * Compact constructor with defensive copies and null safety.
     */
    public IcebergTableHandle {
        requireNonBlank(schemaName, "schemaName");
        requireNonBlank(tableName, "tableName");
        requireNonBlank(tableLocation, "tableLocation");
        requireNonBlank(schemaJson, "schemaJson");
        partitionSpecJson = partitionSpecJson == null ? "{}" : partitionSpecJson;
        snapshotId = snapshotId == null ? Optional.empty() : snapshotId;
        properties = properties == null ? Map.of() : Map.copyOf(properties);
        readMode = readMode == null ? READ_MODE_FULL : readMode;
    }

    private static void requireNonBlank(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
    }

    /**
     * Creates a handle with a specific snapshot ID for time-travel queries.
     *
     * @param targetSnapshotId the snapshot ID to query
     * @return a new handle with TIME_TRAVEL read mode
     */
    public IcebergTableHandle withSnapshotId(long targetSnapshotId) {
        return new IcebergTableHandle(
                schemaName, tableName, tableLocation,
                Optional.of(targetSnapshotId), schemaJson, partitionSpecJson,
                formatVersion, selectorProperties(),
                READ_MODE_TIME_TRAVEL, 0L, 0L, pushedLimit);
    }

    /**
     * Creates a handle without a specific snapshot (uses current snapshot).
     *
     * @return a new handle without snapshot ID
     */
    public IcebergTableHandle withoutSnapshotId() {
        return new IcebergTableHandle(
                schemaName, tableName, tableLocation,
                Optional.empty(), schemaJson, partitionSpecJson,
                formatVersion, selectorProperties(),
                READ_MODE_FULL, 0L, 0L, pushedLimit);
    }

    /**
     * Creates a handle for incremental reads between two snapshots.
     *
     * @param startSnapshotId start snapshot (exclusive)
     * @param endSnapshotId   end snapshot (inclusive)
     * @return a new handle with INCREMENTAL read mode
     */
    public IcebergTableHandle withIncrementalRead(long startSnapshotId, long endSnapshotId) {
        return new IcebergTableHandle(
                schemaName, tableName, tableLocation,
                Optional.empty(), schemaJson, partitionSpecJson,
                formatVersion, selectorProperties(),
                READ_MODE_INCREMENTAL, startSnapshotId, endSnapshotId, pushedLimit);
    }

    private Map<String, String> selectorProperties() {
        var selected = new java.util.LinkedHashMap<>(properties);
        selected.remove(SOURCE_IDENTITY_PROPERTY);
        return selected;
    }

    /**
     * Creates a handle with a pushed-down row limit.
     *
     * @param limit the maximum number of rows to return
     * @return a new handle with the limit encoded
     */
    public IcebergTableHandle withLimit(long limit) {
        return new IcebergTableHandle(
                schemaName, tableName, tableLocation,
                snapshotId, schemaJson, partitionSpecJson,
                formatVersion, properties,
                readMode, fromSnapshotId, toSnapshotId, limit);
    }

    /**
     * Returns true if a limit has been pushed down into this handle.
     */
    public boolean hasLimit() {
        return pushedLimit > NO_LIMIT;
    }

    @Override
    public ConnectorTableHandle withTimeTravelSnapshot(long targetSnapshotId) {
        return withSnapshotId(targetSnapshotId);
    }
}
