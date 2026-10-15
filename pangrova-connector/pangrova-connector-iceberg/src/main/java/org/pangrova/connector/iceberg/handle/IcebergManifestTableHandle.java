package org.pangrova.connector.iceberg.handle;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.TreeMap;

import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;

/** A read-only Iceberg $manifests selection pinned to one snapshot and manifest list. */
public record IcebergManifestTableHandle(
        IcebergTableHandle baseTable,
        String tableUuid,
        String metadataLocation,
        OptionalLong snapshotId,
        long snapshotSequenceNumber,
        String manifestListLocation,
        long manifestListLength,
        String manifestListSha256,
        Map<Integer, List<PartitionSummaryField>> partitionSummarySpecs,
        List<ConnectorTableMetadata.Column> typedColumns) implements ConnectorTableHandle {

    private static final String SHA256_HEX_PATTERN = "[0-9a-f]{64}";

    /** The result type and transform needed to render one bound as the SDK does. */
    public record PartitionSummaryField(String transform, String resultType) {
        public PartitionSummaryField {
            if (transform == null || transform.isBlank()
                    || resultType == null || resultType.isBlank()) {
                throw new IllegalArgumentException("Partition summary field requires type and transform");
            }
        }
    }

    public IcebergManifestTableHandle {
        Objects.requireNonNull(baseTable, "baseTable");
        if (tableUuid == null || tableUuid.isBlank()
                || metadataLocation == null || metadataLocation.isBlank()) {
            throw new IllegalArgumentException("Iceberg table metadata identity is required");
        }
        Objects.requireNonNull(snapshotId, "snapshotId");
        Map<Integer, List<PartitionSummaryField>> frozenSpecs = new TreeMap<>();
        Objects.requireNonNull(partitionSummarySpecs, "partitionSummarySpecs")
                .forEach((id, fields) -> frozenSpecs.put(id, List.copyOf(fields)));
        partitionSummarySpecs = Map.copyOf(frozenSpecs);
        typedColumns = List.copyOf(Objects.requireNonNull(typedColumns, "typedColumns"));
        if (typedColumns.isEmpty()) {
            throw new IllegalArgumentException("Iceberg manifests schema is required");
        }
        validateSelection(snapshotId, snapshotSequenceNumber,
                manifestListLocation, manifestListLength, manifestListSha256);
    }

    public String tableName() {
        return baseTable.tableName() + "$manifests";
    }

    public boolean empty() {
        return snapshotId.isEmpty();
    }

    private static void validateSelection(OptionalLong snapshotId, long sequenceNumber,
            String listLocation, long listLength, String listSha256) {
        if (snapshotId.isEmpty()) {
            if (sequenceNumber != 0 || listLocation != null || listLength != 0 || listSha256 != null) {
                throw new IllegalArgumentException("Empty manifests selection must have no read object");
            }
            return;
        }
        if (sequenceNumber < 0 || listLocation == null || listLocation.isBlank()
                || listLength <= 0 || listSha256 == null
                || !listSha256.matches(SHA256_HEX_PATTERN)) {
            throw new IllegalArgumentException("Selected manifests require a pinned list object");
        }
    }
}
