package org.pangrova.indexing.planner.vector;

import java.util.Locale;

import org.pangrova.indexing.api.vector.RecallEstimationHint;

/** Existing empirical recall policy, owned by planning rather than the contract. */
final class EmpiricalRecallEstimator {
    private EmpiricalRecallEstimator() {}

    private static final double DEFAULT_RECALL = 0.9;
    private static final double HNSW_MAX_RECALL = 0.99;
    private static final double HNSW_BASE_RECALL = 0.95;
    private static final double HNSW_TOP_K_FACTOR = 0.01;
    private static final double IVF_PQ_MIN_RECALL = 0.7;
    private static final double IVF_PQ_BASE_RECALL = 0.85;
    private static final double IVF_PQ_DIMENSION_FACTOR = 0.02;
    private static final double DISK_ANN_MAX_RECALL = 0.98;
    private static final double DISK_ANN_BASE_RECALL = 0.90;
    private static final double DISK_ANN_TOP_K_FACTOR = 0.005;

    /**
     * Default estimation based on empirical formulas.
     * - HNSW: ~0.95 for typical parameters
     * - IVF_PQ: ~0.85 for typical parameters, degrades with high dimension
     * - DISK_ANN: ~0.90 for typical parameters
     */
    static RecallEstimationHint defaultEstimator() {
        return (topK, dimension, algorithm, vectorCount) -> {
            if (algorithm == null) {
                return DEFAULT_RECALL;
            }
            return switch (algorithm.toUpperCase(Locale.ROOT)) {
                case "HNSW" -> Math.min(
                        HNSW_MAX_RECALL, HNSW_BASE_RECALL + HNSW_TOP_K_FACTOR * Math.log10(topK));
                case "IVF_PQ" -> Math.max(
                        IVF_PQ_MIN_RECALL,
                        IVF_PQ_BASE_RECALL - IVF_PQ_DIMENSION_FACTOR * (dimension / 128.0));
                case "DISK_ANN" -> Math.min(
                        DISK_ANN_MAX_RECALL,
                        DISK_ANN_BASE_RECALL + DISK_ANN_TOP_K_FACTOR * Math.log10(topK));
                default -> DEFAULT_RECALL;
            };
        };
    }
}
