package org.pangrova.indexing.spi.pruning;

import org.pangrova.indexing.api.pruning.TopKBoundary;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Callback interface for receiving Top-K boundary updates from workers.
 *
 * <p>Workers report {@link TopKBoundary} as the top-k heap fills during
 * ORDER BY ... LIMIT query execution. The coordinator uses these boundaries
 * to prune remaining unassigned splits via {@code TopKSplitPruner}.</p>
 */
@Experimental
public interface TopKBoundaryListener {

    /**
     * Called when a worker reports an updated Top-K boundary.
     *
     * @param queryId  the query identifier
     * @param boundary the updated boundary from the worker's top-k heap
     */
    void onBoundaryUpdate(String queryId, TopKBoundary boundary);
}
