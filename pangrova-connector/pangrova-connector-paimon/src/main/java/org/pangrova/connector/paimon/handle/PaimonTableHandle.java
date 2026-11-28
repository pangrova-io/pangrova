package org.pangrova.connector.paimon.handle;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.pangrova.connector.api.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.api.metadata.table.SnapshotRange;

import static org.pangrova.common.validation.Preconditions.requireText;

/**
 * Immutable handle referencing a Paimon table by schema, name, and metadata snapshot.
 *
 * <p>This handle contains only immutable metadata fields (String, long, Optional, List.of(), Map.of()).
 * The live Paimon SDK Table object is NOT stored here to prevent SDK object leakage
 * across module boundaries. Consumers needing the SDK Table must reload it via catalog API.
 *
 * <p>Metadata snapshot fields:
 * <ul>
 *   <li>{@code tableLocation} - Base location URI of the table</li>
 *   <li>{@code tableUuid} - SDK table identity used to fence drop/recreate</li>
 *   <li>{@code schemaFingerprint} - Canonical schema identity captured with the handle</li>
 *   <li>{@code snapshotId} - Optional snapshot ID for time-travel queries</li>
 *   <li>{@code rowTypeJson} - JSON serialization of the Paimon RowType schema</li>
 *   <li>{@code primaryKeys} - List of primary key column names (empty for append-only tables)</li>
 *   <li>{@code partitionKeys} - List of partition key column names</li>
 * </ul>
 */
public record PaimonTableHandle(
        String schemaName,
        String tableName,
        String tableLocation,
        String tableUuid,
        String schemaFingerprint,
        Optional<Long> snapshotId,
        String rowTypeJson,
        List<String> primaryKeys,
        List<String> partitionKeys,
        Map<String, String> properties,
        long pushedLimit,
        Optional<SnapshotRange> incrementalRange) implements ConnectorTableHandle {

    /** No limit pushed down. */
    public static final long NO_LIMIT = 0L;

    /** Format capabilities this adapter can prove. */
    public static final org.pangrova.connector.api.metadata.table.TableCapabilities CAPABILITIES =
            new org.pangrova.connector.api.metadata.table.TableCapabilities(
                    true, true, true, true, true, true,
                    java.util.Set.of("files", "branches", "snapshots", "tags", "options",
                            "schemas"),
                    java.util.Set.of("COMPACT", "OPTIMIZE", "VACUUM", "EXPIRE_SNAPSHOTS",
                            "REMOVE_ORPHAN_FILES"),
                    org.pangrova.connector.api.enums.WriterFenceKind
                            .FORMAT_IDENTITY_AND_BASE_VERSION);

    /** Evidence captured from the selected SDK source, never a query-wide mutable pin. */
    public static final String SOURCE_IDENTITY_PROPERTY = "pangrova.read.source_identity";
    /** Ordinary reads retain their captured current schema while selecting a data snapshot. */
    public static final String CURRENT_SCHEMA_PROPERTY = "pangrova.read.current_schema";

    /**
     * Compact constructor with defensive copies and null safety.
     */
    public PaimonTableHandle {
        requireText(schemaName, "schemaName");
        requireText(tableName, "tableName");
        requireText(tableLocation, "tableLocation");
        requireText(tableUuid, "tableUuid");
        requireText(schemaFingerprint, "schemaFingerprint");
        requireText(rowTypeJson, "rowTypeJson");
        snapshotId = snapshotId == null ? Optional.empty() : snapshotId;
        primaryKeys = primaryKeys == null ? List.of() : List.copyOf(primaryKeys);
        partitionKeys = partitionKeys == null ? List.of() : List.copyOf(partitionKeys);
        incrementalRange = incrementalRange == null ? Optional.empty() : incrementalRange;
        if (properties == null) {
            properties = Map.of();
        } else {
            // RESTCatalog includes optional audit fields in table.options();
            // an unset owner is represented there by a null value. A missing
            // option has the same meaning and keeps this handle immutable.
            var presentProperties = new java.util.HashMap<>(properties);
            presentProperties.values().removeIf(value -> value == null);
            properties = Map.copyOf(presentProperties);
        }
    }

    @Override
    public org.pangrova.connector.api.metadata.table.TableCapabilities capabilities() {
        return CAPABILITIES;
    }

    @Override
    public Optional<org.pangrova.connector.api.metadata.table.TableIdentity> tableIdentity() {
        return Optional.of(new org.pangrova.connector.api.metadata.table.TableIdentity(
                "PAIMON", tableUuid, tableLocation, schemaFingerprint));
    }

    /**
     * Creates a handle with a specific snapshot ID for time-travel queries.
     *
     * @param targetSnapshotId the snapshot ID to query
     * @return a new handle with the specified snapshot ID
     */
    public PaimonTableHandle withSnapshotId(long targetSnapshotId) {
        var selectedProperties = new java.util.LinkedHashMap<>(properties);
        selectedProperties.remove(CURRENT_SCHEMA_PROPERTY);
        selectedProperties.remove(SOURCE_IDENTITY_PROPERTY);
        return new PaimonTableHandle(
                schemaName, tableName, tableLocation, tableUuid, schemaFingerprint,
                Optional.of(targetSnapshotId), rowTypeJson,
                primaryKeys, partitionKeys, selectedProperties, pushedLimit, Optional.empty());
    }

    @Override
    public PaimonTableHandle withTimeTravelSnapshot(String snapshotToken) {
        return withSnapshotId(snapshotId(snapshotToken));
    }

    private static long snapshotId(String snapshotToken) {
        try {
            long id = Long.parseLong(snapshotToken);
            if (id < 0 || !Long.toString(id).equals(snapshotToken)) {
                throw new NumberFormatException("non-canonical Paimon snapshot");
            }
            return id;
        } catch (NumberFormatException invalid) {
            throw new IllegalArgumentException("Invalid Paimon snapshot token", invalid);
        }
    }

    /**
     * Creates a handle without a specific snapshot (uses current snapshot).
     *
     * @return a new handle without snapshot ID
     */
    public PaimonTableHandle withoutSnapshotId() {
        var selectedProperties = new java.util.LinkedHashMap<>(properties);
        selectedProperties.remove(CURRENT_SCHEMA_PROPERTY);
        selectedProperties.remove(SOURCE_IDENTITY_PROPERTY);
        return new PaimonTableHandle(
                schemaName, tableName, tableLocation, tableUuid, schemaFingerprint,
                Optional.empty(), rowTypeJson,
                primaryKeys, partitionKeys, selectedProperties, pushedLimit, Optional.empty());
    }

    /**
     * Creates a handle with a pushed-down row limit.
     *
     * @param limit the maximum number of rows to return
     * @return a new handle with the limit encoded
     */
    public PaimonTableHandle withLimit(long limit) {
        return new PaimonTableHandle(
                schemaName, tableName, tableLocation, tableUuid, schemaFingerprint,
                snapshotId, rowTypeJson,
                primaryKeys, partitionKeys, properties, limit, incrementalRange);
    }

    /**
     * Creates a handle that reads only files appended inside the snapshot range.
     *
     * @param range inclusive append-only snapshot range
     * @return a new handle with the incremental range bound
     */
    public PaimonTableHandle withIncrementalRange(SnapshotRange range) {
        if (range == null) {
            throw new IllegalArgumentException("range must not be null");
        }
        var selectedProperties = new java.util.LinkedHashMap<>(properties);
        selectedProperties.remove(CURRENT_SCHEMA_PROPERTY);
        selectedProperties.remove(SOURCE_IDENTITY_PROPERTY);
        return new PaimonTableHandle(
                schemaName, tableName, tableLocation, tableUuid, schemaFingerprint,
                Optional.of(range.toInclusive()), rowTypeJson,
                primaryKeys, partitionKeys, selectedProperties, pushedLimit, Optional.of(range));
    }

    /**
     * Returns true if a limit has been pushed down into this handle.
     */
    public boolean hasLimit() {
        return pushedLimit > NO_LIMIT;
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
