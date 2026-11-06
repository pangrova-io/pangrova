package org.pangrova.execution.core.split;

import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.protobuf.InvalidProtocolBufferException;

import org.pangrova.protocol.source.DataSplitProtoCodec;
import org.pangrova.protocol.v2.DataSplit;

/** Parses and validates canonical split identities before task dispatch. */
public final class CanonicalSplitParser {

    private CanonicalSplitParser() {}

    /**
     * Decodes canonical split payloads and rejects duplicate or incomplete identities.
     *
     * @param serializedSplits base64-encoded canonical v2 splits
     * @param queryId query being dispatched, included in failure context
     * @param stageId stage being dispatched, included in failure context
     * @return immutable validated splits
     */
    public static List<DataSplit> parseAll(
            List<String> serializedSplits, String queryId, int stageId) {
        if (serializedSplits == null || serializedSplits.isEmpty()) {
            return List.of();
        }
        Set<String> splitIds = new HashSet<>();
        return serializedSplits.stream()
                .map(encoded -> parse(encoded, queryId, stageId))
                .peek(split -> validateIdentity(split, splitIds, queryId, stageId))
                .toList();
    }

    private static DataSplit parse(String encoded, String queryId, int stageId) {
        try {
            byte[] bytes = Base64.getDecoder().decode(encoded);
            DataSplit split = DataSplit.parseFrom(bytes);
            DataSplitProtoCodec.fileDetail(split);
            return split;
        } catch (InvalidProtocolBufferException | IllegalArgumentException failure) {
            throw invalidSplit(queryId, stageId, failure);
        }
    }

    private static void validateIdentity(
            DataSplit split, Set<String> splitIds, String queryId, int stageId) {
        if (!validIdentity(split, splitIds)) {
            throw invalidSplit(queryId, stageId, null);
        }
    }

    private static boolean validIdentity(DataSplit split, Set<String> splitIds) {
        if (split.getSplitId().isBlank() || !splitIds.add(split.getSplitId())) {
            return false;
        }
        return !split.getConnectorId().isBlank()
                && !split.getSourceId().isBlank()
                && !split.getSourceSnapshot().isBlank()
                && !split.getDataObjectId().isBlank()
                && !split.getOpaqueLocation().isBlank();
    }

    private static IllegalStateException invalidSplit(
            String queryId, int stageId, Throwable cause) {
        String message = "Invalid canonical split identity at coordinator dispatch: "
                + "queryId=" + queryId + " stageId=" + stageId;
        return cause == null
                ? new IllegalStateException(message)
                : new IllegalStateException(message, cause);
    }
}
