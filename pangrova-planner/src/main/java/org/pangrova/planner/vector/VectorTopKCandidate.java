package org.pangrova.planner.vector;

import org.pangrova.spi.index.vector.DistanceMetric;

/**
 * Recognized VectorTopK query pattern extracted from a Calcite logical plan.
 *
 * <p>Produced by {@link VectorTopKRecognizer} when it detects the pattern
 * {@code ORDER BY distance_func(vec_col, query) LIMIT K}. Consumed by
 * {@link VectorIndexRoutingRule} to decide between ANN index scan and full scan.</p>
 *
 * @param tableName    fully qualified table name
 * @param vectorColumn name of the vector column used in the distance function
 * @param queryVector  query embedding (defensively copied)
 * @param topK         number of nearest neighbors requested
 * @param metric       distance metric for similarity computation
 */
public record VectorTopKCandidate(
        String tableName,
        String vectorColumn,
        float[] queryVector,
        int topK,
        DistanceMetric metric) {

    public VectorTopKCandidate {
        if (tableName == null || tableName.isBlank()) {
            throw new IllegalArgumentException("tableName must not be blank");
        }
        if (vectorColumn == null || vectorColumn.isBlank()) {
            throw new IllegalArgumentException("vectorColumn must not be blank");
        }
        if (queryVector == null || queryVector.length == 0) {
            throw new IllegalArgumentException("queryVector must not be null or empty");
        }
        if (topK < 1) {
            throw new IllegalArgumentException("topK must be at least 1");
        }
        if (metric == null) {
            throw new IllegalArgumentException("metric must not be null");
        }
        queryVector = queryVector.clone();
    }
}
