package org.pangrova.execution.split;

import java.util.HashSet;
import java.util.Set;

import org.pangrova.protocol.source.DataSplitProtoCodec;
import org.pangrova.protocol.v2.DataSplit;

/** Validates canonical split identities and uniqueness before task dispatch. */
public final class CanonicalSplitValidator {

    private CanonicalSplitValidator() {}

    /**
     * Rejects duplicate or incomplete identities for all splits of a stage.
     *
     * @param splits canonical v2 splits
     * @param queryId query being dispatched, included in failure context
     * @param stageId stage being dispatched, included in failure context
     */
    public static void validateAll(Iterable<DataSplit> splits, String queryId, int stageId) {
        Set<String> splitIds = new HashSet<>();
        for (DataSplit split : splits) {
            try {
                DataSplitProtoCodec.fileDetail(split);
            } catch (IllegalArgumentException failure) {
                throw invalidSplit(queryId, stageId, failure);
            }
            validateIdentity(split, splitIds, queryId, stageId);
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
