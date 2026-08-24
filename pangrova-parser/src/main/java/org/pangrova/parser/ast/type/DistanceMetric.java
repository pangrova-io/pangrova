package org.pangrova.parser.ast.type;

/**
 * Distance metric for vector similarity search operations.
 * Used in VECTOR type declarations to specify how distance is computed.
 */
public enum DistanceMetric {
    /** Euclidean (L2) distance. */
    L2,
    /** Cosine similarity (1 - cosine distance). */
    COSINE,
    /** Inner product (dot product). */
    DOT_PRODUCT
}
