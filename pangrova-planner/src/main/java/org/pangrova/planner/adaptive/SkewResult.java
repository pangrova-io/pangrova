package org.pangrova.planner.adaptive;

import java.util.List;

/**
 * Result of skew detection on partition statistics.
 *
 * @param skewedPartitions partition ids identified as skewed
 * @param medianBytes      median partition size in bytes
 * @param maxBytes         maximum partition size in bytes
 */
public record SkewResult(List<Integer> skewedPartitions, long medianBytes, long maxBytes) {

    /** No skew detected. */
    public static final SkewResult NO_SKEW = new SkewResult(List.of(), 0, 0);

    public SkewResult {
        skewedPartitions = List.copyOf(skewedPartitions);
    }

    /** Returns true when at least one partition is skewed. */
    public boolean hasSkew() {
        return !skewedPartitions.isEmpty();
    }
}
