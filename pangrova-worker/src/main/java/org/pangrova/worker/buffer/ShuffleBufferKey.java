package org.pangrova.worker.buffer;

import java.util.Objects;

/**
 * Immutable composite key for shuffle buffer namespace isolation.
 *
 * <p>Ensures concurrent queries/stages on the same worker use distinct
 * partition buffer spaces, preventing cross-query data contamination
 * (P0-FPV-7).</p>
 *
 * <p>Fields: queryId identifies the query, stageId identifies the stage
 * (or taskId as discriminator when stageId is unavailable), and
 * partitionId identifies the shuffle partition within that scope.</p>
 */
public final class ShuffleBufferKey {

    private final String queryId;
    private final String stageId;
    private final int partitionId;

    /**
     * @param queryId     query identifier (must not be null)
     * @param stageId     stage or task identifier (must not be null)
     * @param partitionId partition index within the stage
     */
    public ShuffleBufferKey(String queryId, String stageId, int partitionId) {
        this.queryId = Objects.requireNonNull(queryId, "queryId must not be null");
        this.stageId = Objects.requireNonNull(stageId, "stageId must not be null");
        this.partitionId = partitionId;
    }

    public String queryId() {
        return queryId;
    }

    public String stageId() {
        return stageId;
    }

    public int partitionId() {
        return partitionId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ShuffleBufferKey that)) return false;
        return partitionId == that.partitionId
                && queryId.equals(that.queryId)
                && stageId.equals(that.stageId);
    }

    @Override
    public int hashCode() {
        int h = queryId.hashCode();
        h = 31 * h + stageId.hashCode();
        h = 31 * h + partitionId;
        return h;
    }

    @Override
    public String toString() {
        return queryId + ":" + stageId + ":" + partitionId;
    }
}
