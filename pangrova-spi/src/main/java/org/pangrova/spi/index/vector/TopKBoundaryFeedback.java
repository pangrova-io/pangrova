package org.pangrova.spi.index.vector;

/**
 * Boundary feedback from a Worker's local top-K vector search result.
 *
 * <p>After a worker computes its local top-K, it reports the worst (largest)
 * distance in its result set back to the coordinator. The coordinator
 * aggregates these boundaries across workers to derive a global distance
 * threshold, which is then pushed to remaining workers as a dynamic
 * filter -- workers can skip files whose nearest-possible distance
 * exceeds the threshold.</p>
 *
 * <p>This implements the Worker-to-coordinator top-K boundary feedback
 * channel required for runtime pruning (G-INDEX-03).</p>
 *
 * @param workerId       the reporting worker's identifier
 * @param queryId        the query this feedback belongs to
 * @param topK           the K value used for local search
 * @param worstDistance   the largest distance in the local top-K result set
 *                        (Double.MAX_VALUE if fewer than K results found)
 * @param resultCount    actual number of results found (may be less than topK)
 */
public record TopKBoundaryFeedback(
        String workerId,
        String queryId,
        int topK,
        double worstDistance,
        int resultCount) {

    public TopKBoundaryFeedback {
        if (workerId == null || workerId.isBlank()) {
            throw new IllegalArgumentException("workerId must not be null or blank");
        }
        if (queryId == null || queryId.isBlank()) {
            throw new IllegalArgumentException("queryId must not be null or blank");
        }
        if (topK <= 0) {
            throw new IllegalArgumentException("topK must be positive, got " + topK);
        }
    }

    /**
     * Returns true if this worker found a full top-K result set.
     * When false, the boundary may not be meaningful for pruning.
     */
    public boolean isComplete() {
        return resultCount >= topK;
    }
}
