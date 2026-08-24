package org.pangrova.spi.index.vector;

/**
 * Distance metrics for vector similarity search.
 *
 * <p>All metrics follow the convention that <em>lower values indicate
 * greater similarity</em>, so results can be uniformly sorted in
 * ascending order regardless of the chosen metric.
 */
public enum DistanceMetric {

    /**
     * Euclidean (L2) distance: {@code sqrt(sum((a[i] - b[i])^2))}.
     *
     * <p>Lower values indicate more similar vectors. Zero means identical.
     */
    L2,

    /**
     * Cosine distance: {@code 1 - cosine_similarity(a, b)}.
     *
     * <p>Range is {@code [0, 2]} for non-zero vectors. Lower values
     * indicate more similar vectors. Zero means identical direction.
     */
    COSINE,

    /**
     * Negative inner (dot) product: {@code -dot(a, b)}.
     *
     * <p>Lower values indicate more similar vectors. Assumes vectors
     * are normalized; for unnormalized vectors prefer {@link #L2} or
     * {@link #COSINE}.
     */
    INNER_PRODUCT
}
