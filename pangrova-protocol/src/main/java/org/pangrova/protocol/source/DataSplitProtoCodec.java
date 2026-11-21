package org.pangrova.protocol.source;

import org.pangrova.common.util.DigestUtils;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import org.pangrova.common.util.OrderedMaps;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.protocol.v2.FileSplitDetail;
import org.pangrova.protocol.v2.LogicalType;
import org.pangrova.protocol.v2.PartitionValue;
import org.pangrova.protocol.v2.TypedValue;

import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import static org.pangrova.common.util.Preconditions.requireText;

/** Utilities for the generic v2 DataSplit envelope and typed file detail extension. */
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
        byte[] digest = DigestUtils.sha256()
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
     * @param startOffset non-negative byte or row offset
     * @param length non-negative split length
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
            validateRange(startOffset, length);
            partitionValues = partitionValues == null
                    ? Map.of()
                    : OrderedMaps.immutableCopy(partitionValues);
            detail = copyDetail(Objects.requireNonNull(detail, "detail must not be null"));
        }

        @Override
        public Map<String, String> partitionValues() {
            return OrderedMaps.immutableCopy(partitionValues);
        }

        @Override
        public FileSplitDetail detail() {
            return copyDetail(detail);
        }
    }

    /** Builds the common portion of a DataSplit and packs its FileSplitDetail. */
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
                .setConnectorDetail(Any.pack(input.detail()));
        input.partitionValues().forEach((name, value) -> builder.addPartitionValues(
                PartitionValue.newBuilder()
                        .setColumnName(name)
                        .setValue(stringValue(value))));
        return builder.build();
    }

    private static FileSplitDetail copyDetail(FileSplitDetail detail) {
        try {
            return FileSplitDetail.parseFrom(detail.toByteArray());
        } catch (InvalidProtocolBufferException impossible) {
            throw new IllegalStateException(
                    "Serialized FileSplitDetail could not be parsed", impossible);
        }
    }

    private static void validateRange(long startOffset, long length) {
        if (startOffset < 0 || length < 0) {
            throw new IllegalArgumentException("split range must be non-negative");
        }
    }

    /** Unpacks the exact file detail type; unknown connector extensions fail closed. */
    public static FileSplitDetail fileDetail(DataSplit split) {
        if (!split.hasConnectorDetail()
                || !split.getConnectorDetail().is(FileSplitDetail.class)) {
            throw new IllegalArgumentException(
                    "DataSplit connector_detail must be pangrova.protocol.v2.FileSplitDetail");
        }
        try {
            return split.getConnectorDetail().unpack(FileSplitDetail.class);
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Malformed FileSplitDetail", failure);
        }
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

    /** Replaces the packed file detail while preserving the generic split identity fields. */
    public static DataSplit withFileDetail(DataSplit split, FileSplitDetail detail) {
        return split.toBuilder().setConnectorDetail(Any.pack(detail)).build();
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
