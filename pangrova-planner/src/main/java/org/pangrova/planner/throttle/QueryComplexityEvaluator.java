package org.pangrova.planner.throttle;

/**
 * Evaluates query complexity from plan metrics.
 * Each metric is normalized to [0,1] via sigmoid, then combined as a weighted sum.
 * Final score is clamped to [0.0, 1.0].
 */
public class QueryComplexityEvaluator {

    private static final double WEIGHT_ROWS = 0.30;
    private static final double WEIGHT_BYTES = 0.25;
    private static final double WEIGHT_JOINS = 0.20;
    private static final double WEIGHT_SUBQUERY = 0.15;
    private static final double WEIGHT_AGGREGATIONS = 0.10;

    // Sigmoid midpoints — value at which normalized score = 0.5
    private static final double MID_ROWS = 1_000_000.0;
    private static final double MID_BYTES = 100_000_000.0;
    private static final double MID_JOINS = 4.0;
    private static final double MID_SUBQUERY = 3.0;
    private static final double MID_AGGREGATIONS = 5.0;

    /**
     * Compute complexity score from plan metrics.
     * Returns a score in [0.0, 1.0].
     */
    public ComplexityScore evaluate(PlanMetrics metrics) {
        double score = WEIGHT_ROWS * sigmoid(metrics.estimatedRows(), MID_ROWS)
                + WEIGHT_BYTES * sigmoid(metrics.estimatedBytes(), MID_BYTES)
                + WEIGHT_JOINS * sigmoid(metrics.joinCount(), MID_JOINS)
                + WEIGHT_SUBQUERY * sigmoid(metrics.subqueryDepth(), MID_SUBQUERY)
                + WEIGHT_AGGREGATIONS * sigmoid(metrics.aggregationCount(), MID_AGGREGATIONS);
        return new ComplexityScore(clamp(score));
    }

    /**
     * Sigmoid normalization: maps [0, +∞) to [0, 1).
     * sigmoid(x, mid) = x / (x + mid), where mid is the value yielding 0.5.
     */
    private static double sigmoid(double value, double midpoint) {
        if (value <= 0.0) {
            return 0.0;
        }
        return value / (value + midpoint);
    }

    private static double clamp(double v) {
        return Math.max(0.0, Math.min(1.0, v));
    }
}
