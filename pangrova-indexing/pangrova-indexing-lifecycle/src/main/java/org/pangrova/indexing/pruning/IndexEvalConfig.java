package org.pangrova.indexing.pruning;

import org.pangrova.indexing.evaluation.IndexEvalMetrics;

/**
 * Immutable configuration for index evaluation within {@link IndexSplitFilter}.
 *
 * <p>Groups the three evaluation-related parameters that were previously
 * passed individually to the {@code IndexSplitFilter} constructor, keeping
 * the constructor within the 4-parameter governance limit.</p>
 *
 * @param evalMetrics     metrics collector for evaluation operations (nullable)
 * @param evalTimeoutMs   per-split evaluation timeout in milliseconds
 * @param evalParallelism max concurrent split evaluations; 0 = serial only
 */
public record IndexEvalConfig(
        IndexEvalMetrics evalMetrics,
        long evalTimeoutMs,
        int evalParallelism) {

    public IndexEvalConfig {
        if (evalTimeoutMs < 0) {
            throw new IllegalArgumentException("evalTimeoutMs must be >= 0, got " + evalTimeoutMs);
        }
        if (evalParallelism < 0) {
            throw new IllegalArgumentException("evalParallelism must be >= 0, got " + evalParallelism);
        }
    }
}
