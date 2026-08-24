package org.pangrova.planner.stats;

/**
 * A learned cardinality-correction summary for one query shape, accumulated from runtime
 * feedback. The {@code correctionFactor} is an exponentially-smoothed {@code actual/estimated}
 * ratio: multiply a fresh planner estimate by it to fold in historical observations.
 *
 * @param correctionFactor smoothed actual/estimated ratio ({@code 1.0} = estimates were accurate)
 * @param observationCount number of feedback samples folded into this summary
 */
public record CardinalityFeedback(double correctionFactor, long observationCount) {
}
