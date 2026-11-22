package org.pangrova.connector.iceberg.handle;

import org.pangrova.connector.iceberg.enums.IcebergOverwriteScope;

import org.pangrova.connector.api.metadata.ConnectorOutputTableHandle;
import org.pangrova.connector.api.metadata.table.SchemaTableName;

import java.util.Map;
import java.util.TreeMap;

import static org.pangrova.common.util.Preconditions.requireText;

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
 *
 * <p>{@code overwriteScope} is mandatory for {@code OVERWRITE} mode: a commit refuses to widen
 * a static-partition replacement into a whole-table replacement, and refuses an overwrite that
 * never declared what it replaces.</p>
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
        IcebergOverwriteScope overwriteScope,
        Map<String, String> staticPartitions,
        Map<String, String> declarationProperties) implements ConnectorOutputTableHandle {

    /** Sentinel indicating the table had no snapshot when the write began. */
    public static final long NO_SNAPSHOT = -1L;

    /** Write mode constants. */
    public static final String MODE_INSERT = "INSERT";
    public static final String MODE_OVERWRITE = "OVERWRITE";
    public static final String MODE_DELETE = "DELETE";
    public static final String MODE_MERGE = "MERGE";

    /**
     * Compact constructor with validation.
     */
    public IcebergOutputTableHandle {
        requireText(schemaName, "schemaName");
        requireText(tableName, "tableName");
        requireText(tableUuid, "tableUuid");
        requireText(tableLocation, "tableLocation");
        requireText(schemaJson, "schemaJson");
        partitionSpecJson = partitionSpecJson == null ? "{}" : partitionSpecJson;
        if (!MODE_INSERT.equals(writeMode)
                && !MODE_OVERWRITE.equals(writeMode)
                && !MODE_DELETE.equals(writeMode)
                && !MODE_MERGE.equals(writeMode)) {
            throw new IllegalArgumentException("Unsupported Iceberg write mode: " + writeMode);
        }
        overwriteScope = overwriteScope == null ? IcebergOverwriteScope.NONE : overwriteScope;
        if (!MODE_OVERWRITE.equals(writeMode)) {
            if (overwriteScope != IcebergOverwriteScope.NONE) {
                throw new IllegalArgumentException(
                        "Iceberg overwrite scope requires OVERWRITE mode: " + overwriteScope);
            }
        } else if (overwriteScope == IcebergOverwriteScope.NONE) {
            throw new IllegalArgumentException(
                    "Iceberg OVERWRITE requires an explicit overwrite scope");
        } else if (overwriteScope == IcebergOverwriteScope.DYNAMIC_PARTITION) {
            throw new UnsupportedOperationException(
                    "Iceberg dynamic partition overwrite requires planner-provided affected partitions");
        }
        staticPartitions = Map.copyOf(new TreeMap<>(staticPartitions));
        if (!staticPartitions.isEmpty() && !MODE_OVERWRITE.equals(writeMode)) {
            throw new IllegalArgumentException("Static partitions require Iceberg OVERWRITE mode");
        }
        if (overwriteScope == IcebergOverwriteScope.STATIC_PARTITION && staticPartitions.isEmpty()) {
            throw new IllegalArgumentException(
                    "Iceberg STATIC_PARTITION overwrite requires at least one static partition");
        }
        if (overwriteScope == IcebergOverwriteScope.WHOLE_TABLE && !staticPartitions.isEmpty()) {
            throw new IllegalArgumentException(
                    "Iceberg WHOLE_TABLE overwrite must not carry static partitions");
        }
        staticPartitions.forEach((key, value) -> requireText(key, "static partition column"));
        declarationProperties = IcebergDeclarationProperties.requireCanonical(declarationProperties);
    }

    @Override
    public Map<String, String> declarationProperties() {
        return Map.copyOf(declarationProperties);
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

    @Override
    public boolean requiresSingleCdcRange() {
        return MODE_MERGE.equals(writeMode);
    }

    @Override
    public java.util.Optional<org.pangrova.connector.api.metadata.table.TableIdentity> tableIdentity() {
        return java.util.Optional.of(new org.pangrova.connector.api.metadata.table.TableIdentity(
                "ICEBERG", tableUuid, tableLocation,
                org.pangrova.common.util.DigestUtils.sha256Hex(schemaJson)));
    }
}
