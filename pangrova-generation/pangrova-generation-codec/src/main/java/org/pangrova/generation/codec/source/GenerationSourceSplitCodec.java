package org.pangrova.generation.codec.source;

import java.util.List;
import java.util.Base64;
import com.google.protobuf.InvalidProtocolBufferException;
import org.pangrova.protocol.v2.DataSplit;

/** Decodes exact immutable source identities for publication reads; no store or task ownership. */
public final class GenerationSourceSplitCodec {
    private GenerationSourceSplitCodec() {}

    /** Decodes Base64 protobuf splits and rejects missing or unknown canonical identity fields. */
    public static List<DataSplit> decode(List<String> serializedSplits) {
        if (serializedSplits == null || serializedSplits.isEmpty()) {
            return List.of();
        }
        return serializedSplits.stream()
                .map(GenerationSourceSplitCodec::parseSplit)
                .toList();
    }

    private static DataSplit parseSplit(String encoded) {
        try {
            DataSplit split = DataSplit.parseFrom(Base64.getDecoder().decode(encoded));
            validateSplit(split);
            return split;
        } catch (InvalidProtocolBufferException | IllegalArgumentException failure) {
            throw new IllegalArgumentException(
                    "cannot parse split for artifact resolution", failure);
        }
    }

    private static void validateSplit(DataSplit split) {
        if (!split.getUnknownFields().asMap().isEmpty()
                || split.getSplitId().isBlank()
                || split.getSourceId().isBlank()
                || split.getSourceSnapshot().isBlank()
                || split.getDataObjectId().isBlank()) {
            throw new IllegalArgumentException(
                    "artifact resolution requires a canonical split identity");
        }
    }

}
