package org.pangrova.planner.api.statistics;

import org.pangrova.common.api.annotation.Internal;

/**
 * A learned cardinality-correction summary for one query shape, accumulated from runtime
 * feedback. The {@code correctionFactor} is an exponentially-smoothed {@code actual/estimated}
 * ratio: multiply a fresh planner estimate by it to fold in historical observations.
 *
 * @param correctionFactor smoothed actual/estimated ratio ({@code 1.0} = estimates were accurate)
 * @param observationCount number of feedback samples folded into this summary
 */
@Internal
public record CardinalityFeedback(double correctionFactor, long observationCount) {
}
