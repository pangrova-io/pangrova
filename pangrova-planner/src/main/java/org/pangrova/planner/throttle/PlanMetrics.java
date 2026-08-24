package org.pangrova.planner.throttle;

/**
 * Input metrics for query complexity evaluation.
 * All values must be non-negative.
 */
public record PlanMetrics(
        long estimatedRows,
        long estimatedBytes,
        int joinCount,
        int subqueryDepth,
        int aggregationCount
) {

    public PlanMetrics {
        if (estimatedRows < 0 || estimatedBytes < 0 || joinCount < 0
                || subqueryDepth < 0 || aggregationCount < 0) {
            throw new IllegalArgumentException("All metrics must be non-negative");
        }
    }
}
