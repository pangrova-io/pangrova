package org.pangrova.connector.paimon.handle;

import org.pangrova.spi.connector.metadata.ConnectorOutputTableHandle;
import org.pangrova.spi.connector.metadata.NativePhysicalWriteAllocation;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

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
        Layout layout,
        WriteSemantics write) implements ConnectorOutputTableHandle {

    /**
     * Compact constructor with defensive copies and null safety.
     */
    public PaimonOutputTableHandle {
        requireNonBlank(schemaName, "schemaName");
        requireNonBlank(tableName, "tableName");
        requireNonBlank(tableLocation, "tableLocation");
        requireNonBlank(rowTypeJson, "rowTypeJson");
        java.util.Objects.requireNonNull(layout, "layout");
        java.util.Objects.requireNonNull(write, "write");
        if (write.physicalAllocation().isPresent()) {
            var allocation = write.physicalAllocation().orElseThrow();
            if (allocation.baseSnapshotId() != write.baseSnapshotId()
                    || !allocation.policy().writeMode().equals(write.mode())) {
                throw new IllegalArgumentException("Physical allocation differs from the output write authority");
            }
        }
    }

    public record Layout(List<String> primaryKeys, List<String> partitionKeys) {
        public Layout {
            primaryKeys = List.copyOf(primaryKeys);
            partitionKeys = List.copyOf(partitionKeys);
        }
    }

    public record WriteSemantics(Map<String, String> overwritePartition, String mode,
                                 long baseSnapshotId, Optional<NativePhysicalWriteAllocation> physicalAllocation) {
        public WriteSemantics {
            overwritePartition = Map.copyOf(overwritePartition);
            physicalAllocation = java.util.Objects.requireNonNull(physicalAllocation, "physicalAllocation");
            if (!List.of("INSERT", "OVERWRITE", "DELETE").contains(mode) || baseSnapshotId < -1) {
                throw new IllegalArgumentException("Invalid Paimon write mode or source snapshot");
            }
        }
    }

    public List<String> primaryKeys() { return layout.primaryKeys(); }
    public List<String> partitionKeys() { return layout.partitionKeys(); }
    public Map<String, String> overwritePartition() { return write.overwritePartition(); }
    public String mode() { return write.mode(); }
    public long baseSnapshotId() { return write.baseSnapshotId(); }

    private static void requireNonBlank(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
    }

    @Override
    public Optional<SchemaTableName> targetTable() {
        return Optional.of(new SchemaTableName(schemaName, tableName));
    }

    @Override
    public String writeMode() {
        return overwritePartition().isEmpty() ? mode() : "OVERWRITE_PARTITIONED";
    }

    @Override
    public OptionalLong writeBaseSnapshotId() {
        return baseSnapshotId() < 0 ? OptionalLong.empty() : OptionalLong.of(baseSnapshotId());
    }

    @Override
    public Optional<NativePhysicalWriteAllocation> nativePhysicalWriteAllocation() {
        return write.physicalAllocation();
    }

    public PaimonOutputTableHandle withPhysicalAllocation(NativePhysicalWriteAllocation allocation) {
        if (write.physicalAllocation().isPresent()) throw new IllegalStateException("Physical output is already allocated");
        return new PaimonOutputTableHandle(schemaName, tableName, tableLocation, rowTypeJson, layout,
                new WriteSemantics(overwritePartition(), mode(), baseSnapshotId(), Optional.of(allocation)));
    }

    public PaimonOutputTableHandle withBaseSnapshotId(long originalBaseSnapshotId) {
        return new PaimonOutputTableHandle(schemaName, tableName, tableLocation,
                rowTypeJson, layout, new WriteSemantics(overwritePartition(), mode(),
                        originalBaseSnapshotId, write.physicalAllocation()));
    }

    /**
     * Returns true if this table has primary keys (LSM merge-on-read table).
     */
    public boolean hasPrimaryKeys() {
        return !primaryKeys().isEmpty();
    }

    /**
     * Returns true if this table is partitioned.
     */
    public boolean isPartitioned() {
        return !partitionKeys().isEmpty();
    }
}
