package org.pangrova.connector.iceberg.handle;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.TreeMap;

import org.pangrova.protocol.v2.IcebergManifestPartitionField;
import org.pangrova.protocol.v2.IcebergManifestPartitionSpec;
import org.pangrova.connector.api.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;

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

    /** Stable tuple identity and bound rendering; an empty name is outside the unified partition type. */
    public record PartitionSummaryField(String transform, String resultType, int fieldId, String name) {
        public PartitionSummaryField {
            if (transform == null || transform.isBlank()
                    || resultType == null || resultType.isBlank()
                    || fieldId <= 0 || name == null) {
                throw new IllegalArgumentException(
                        "Partition field requires type, transform, positive ID and nonnull name");
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

    /** Canonical field-ID mapping shared by split transport and source identity. */
    public List<IcebergManifestPartitionSpec> protocolPartitionSpecs() {
        return new TreeMap<>(partitionSummarySpecs).entrySet().stream().map(entry -> {
            var spec = IcebergManifestPartitionSpec.newBuilder().setSpecId(entry.getKey());
            for (var field : entry.getValue()) {
                spec.addFields(IcebergManifestPartitionField.newBuilder()
                        .setTransform(field.transform()).setResultType(field.resultType())
                        .setFieldId(field.fieldId()).setName(field.name()));
            }
            return spec.build();
        }).toList();
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
