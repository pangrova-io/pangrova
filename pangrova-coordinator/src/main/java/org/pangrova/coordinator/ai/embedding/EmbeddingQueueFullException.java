package org.pangrova.coordinator.ai.embedding;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;

/**
 * Thrown when an ASYNC INSERT is attempted but the embedding queue for the target
 * table has reached the backpressure threshold (default 100,000 rows).
 *
 * <p>The caller should block until the queue drains below the threshold,
 * rather than rejecting the INSERT outright.</p>
 */
public final class EmbeddingQueueFullException extends PangrovaException {

    private final String tableName;
    private final long currentDepth;
    private final long threshold;

    /** Reports the queue depth that tripped backpressure for the given table. */
    public EmbeddingQueueFullException(String tableName, long currentDepth, long threshold) {
        super(PangrovaErrorCode.AI_EMBEDDING_QUEUE_FULL,
                "Embedding queue backpressure active for table=" + tableName
                        + " depth=" + currentDepth + " threshold=" + threshold
                        + "; ASYNC INSERT blocked until queue drains");
        this.tableName = tableName;
        this.currentDepth = currentDepth;
        this.threshold = threshold;
    }

    public String tableName() {
        return tableName;
    }

    public long currentDepth() {
        return currentDepth;
    }

    public long threshold() {
        return threshold;
    }
}
