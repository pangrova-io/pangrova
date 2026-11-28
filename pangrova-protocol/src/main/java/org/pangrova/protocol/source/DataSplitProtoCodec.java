package org.pangrova.protocol.source;

import org.pangrova.common.crypto.Sha256Digests;

import org.pangrova.common.collection.OrderedMaps;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.protocol.v2.FileSplitDetail;
import org.pangrova.protocol.v2.FileFormat;
import org.pangrova.protocol.v2.LogicalType;
import org.pangrova.protocol.v2.PartitionValue;
import org.pangrova.protocol.v2.TypedValue;

import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import static org.pangrova.common.validation.Preconditions.requireText;

/** Utilities for canonical split identity and typed file details. */
public final class DataSplitProtoCodec {

    private DataSplitProtoCodec() {}

    /** Creates a deterministic split identity that remains stable across task retries. */
    public static String stableSplitId(
            String connectorId,
            String sourceId,
            String sourceSnapshot,
            String dataObjectId,
            String opaqueLocation,
            long startOffset,
            long length) {
        String identity = safe(connectorId) + '\0' + safe(sourceId) + '\0'
                + safe(sourceSnapshot) + '\0' + safe(dataObjectId) + '\0'
                + safe(opaqueLocation) + '\0'
                + startOffset + '\0' + length;
        byte[] digest = Sha256Digests.sha256()
                .digest(identity.getBytes(StandardCharsets.UTF_8));
        return HexFormat.of().formatHex(digest);
    }

    /** Verifies the immutable physical range identity independently of scan-only detail. */
    public static String requireStableSplitId(DataSplit split) {
        Objects.requireNonNull(split, "split must not be null");
        String expected = stableSplitId(split.getConnectorId(), split.getSourceId(),
                split.getSourceSnapshot(), split.getDataObjectId(),
                split.getOpaqueLocation(), split.getStartOffset(), split.getLength());
        if (!expected.equals(split.getSplitId()) || split.getDataObjectId().isBlank()
                || split.getSourceId().isBlank() || split.getOpaqueLocation().isBlank()) {
            throw new IllegalArgumentException("DataSplit immutable physical identity is invalid");
        }
        return expected;
    }

    /**
     * Immutable input for constructing one canonical file split.
     *
     * @param connectorId connector identifier
     * @param sourceId source relation identifier
     * @param sourceSnapshot exact source snapshot
     * @param dataObjectId stable source object identifier
     * @param opaqueLocation connector-readable location
     * @param startOffset non-negative byte offset, zero for other read units
     * @param length non-negative byte length, positive for Parquet
     * @param partitionValues string partition values
     * @param detail typed canonical file detail
     */
    public record FileSplitInput(
            String connectorId,
            String sourceId,
            String sourceSnapshot,
            String dataObjectId,
            String opaqueLocation,
            long startOffset,
            long length,
            Map<String, String> partitionValues,
            FileSplitDetail detail) {

        public FileSplitInput {
            requireText(connectorId, "connectorId");
            requireText(sourceId, "sourceId");
            requireText(opaqueLocation, "opaqueLocation");
            Objects.requireNonNull(detail, "detail must not be null");
            validateRange(startOffset, length, detail.getFileFormat());
            partitionValues = partitionValues == null
                    ? Map.of()
                    : OrderedMaps.immutableCopy(partitionValues);
        }
    }

    /** Builds a DataSplit with its typed file detail. */
    public static DataSplit buildFileSplit(FileSplitInput input) {
        Objects.requireNonNull(input, "input must not be null");
        DataSplit.Builder builder = DataSplit.newBuilder()
                .setSplitId(stableSplitId(
                        input.connectorId(), input.sourceId(), input.sourceSnapshot(),
                        input.dataObjectId(), input.opaqueLocation(), input.startOffset(),
                        input.length()))
                .setConnectorId(input.connectorId())
                .setSourceId(input.sourceId())
                .setSourceSnapshot(safe(input.sourceSnapshot()))
                .setDataObjectId(safe(input.dataObjectId()))
                .setOpaqueLocation(input.opaqueLocation())
                .setStartOffset(input.startOffset())
                .setLength(input.length())
                .setFileDetail(input.detail());
        input.partitionValues().forEach((name, value) -> builder.addPartitionValues(
                PartitionValue.newBuilder()
                        .setColumnName(name)
                        .setValue(stringValue(value))));
        return builder.build();
    }

    private static void validateRange(long startOffset, long length, FileFormat format) {
        if (startOffset < 0 || length < 0) {
            throw new IllegalArgumentException("split range must be non-negative");
        }
        if (format == FileFormat.FILE_FORMAT_PARQUET) {
            if (length == 0 || startOffset > Long.MAX_VALUE - length) {
                throw new IllegalArgumentException("Parquet split requires a positive, bounded byte range");
            }
        } else if (startOffset != 0) {
            throw new IllegalArgumentException("A nonzero byte offset is supported only for Parquet");
        }
    }

    /** Requires an explicitly supplied file detail. */
    public static FileSplitDetail fileDetail(DataSplit split) {
        if (!split.hasFileDetail()) {
            throw new IllegalArgumentException("DataSplit requires file_detail");
        }
        return split.getFileDetail();
    }

    /** Returns string partition values, rejecting type drift rather than stringifying it. */
    public static Map<String, String> stringPartitionValues(DataSplit split) {
        Map<String, String> result = new LinkedHashMap<>();
        for (PartitionValue partition : split.getPartitionValuesList()) {
            if (partition.getColumnName().isBlank()
                    || !partition.getValue().hasStringValue()
                    || result.putIfAbsent(
                    partition.getColumnName(), partition.getValue().getStringValue()) != null) {
                throw new IllegalArgumentException(
                        "DataSplit has invalid or duplicate string partition value");
            }
        }
        return Map.copyOf(result);
    }

    /** Replaces the file detail while preserving the generic split identity fields. */
    public static DataSplit withFileDetail(DataSplit split, FileSplitDetail detail) {
        return split.toBuilder().setFileDetail(detail).build();
    }

    private static TypedValue stringValue(String value) {
        return TypedValue.newBuilder()
                .setType(LogicalType.newBuilder().setCanonicalSql("VARCHAR"))
                .setStringValue(safe(value))
                .build();
    }

    private static String safe(String value) {
        return value == null ? "" : value;
    }
}
