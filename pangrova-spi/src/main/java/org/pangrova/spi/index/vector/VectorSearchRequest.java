package org.pangrova.spi.index.vector;

/**
 * ANN (Approximate Nearest Neighbor) search request.
 *
 * <p>This is a standalone record, deliberately <em>not</em> a permit of the
 * {@code IndexPredicate} sealed interface. Vector search produces a scored
 * Top-K row set -- fundamentally different from the boolean SKIP/KEEP
 * filtering semantics of traditional index predicates.</p>
 *
 * <p>The {@code queryVector} array is defensively copied on construction
 * (query vectors are small, typically &le; 4 KB for 1024-dim float).</p>
 *
 * @param column                target vector column name (must not be blank)
 * @param queryVector           query embedding (defensively copied; must not be null or empty)
 * @param topK                  number of nearest neighbors to return (must be >= 1)
 * @param metric                distance metric for similarity computation (must not be null)
 * @param filteredSearchStrategy strategy for combining ANN search with row-level predicates
 *                               (nullable; defaults to {@link FilteredSearchStrategy#POST_FILTER})
 */
public record VectorSearchRequest(String column, float[] queryVector, int topK,
                                  DistanceMetric metric,
                                  FilteredSearchStrategy filteredSearchStrategy) {

    /** Backward-compatible 4-arg constructor -- defaults to POST_FILTER. */
    public VectorSearchRequest(String column, float[] queryVector, int topK, DistanceMetric metric) {
        this(column, queryVector, topK, metric, null);
    }

    public VectorSearchRequest {
        if (column == null || column.isBlank()) {
            throw new IllegalArgumentException("column required");
        }
        if (queryVector == null || queryVector.length == 0) {
            throw new IllegalArgumentException("queryVector required");
        }
        if (topK < 1) {
            throw new IllegalArgumentException("topK must be >= 1");
        }
        if (metric == null) {
            throw new IllegalArgumentException("metric required");
        }
        queryVector = queryVector.clone(); // defensive copy
    }

    /**
     * Returns the effective filtered search strategy, falling back to
     * {@link FilteredSearchStrategy#POST_FILTER} when none was specified.
     */
    public FilteredSearchStrategy effectiveFilteredSearchStrategy() {
        return filteredSearchStrategy != null ? filteredSearchStrategy : FilteredSearchStrategy.POST_FILTER;
    }
}
