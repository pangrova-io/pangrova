package org.pangrova.planner.adaptive;

import java.util.List;
import java.util.Objects;

/**
 * Result of splitting a skewed partition into sub-ranges for parallel processing.
 *
 * <p>When a partition is too large relative to the median (detected by
 * {@link SkewJoinHandler#detectSkew}), it can be split into multiple sub-ranges
 * that are dispatched as separate tasks. Each sub-range covers a portion of the
 * original partition's data.</p>
 *
 * @param partitionId   the original skewed partition ID
 * @param subRanges     the sub-ranges that the partition is split into
 * @param originalBytes the original partition size in bytes
 */
public record SkewSplitResult(
        int partitionId,
        List<SubRange> subRanges,
        long originalBytes) {

    public SkewSplitResult {
        Objects.requireNonNull(subRanges, "subRanges must not be null");
        subRanges = List.copyOf(subRanges);
        if (subRanges.isEmpty()) {
            throw new IllegalArgumentException("subRanges must not be empty");
        }
        if (originalBytes <= 0) {
            throw new IllegalArgumentException(
                    "originalBytes must be positive, got: " + originalBytes);
        }
    }

    /** Returns the number of sub-ranges this partition was split into. */
    public int splitCount() {
        return subRanges.size();
    }

    /**
     * A sub-range within a split partition. Each sub-range represents a
     * portion of the original partition that can be processed independently.
     *
     * @param subRangeIndex zero-based index of this sub-range within the split
     * @param startFraction start position as a fraction of the partition (0.0 to 1.0)
     * @param endFraction   end position as a fraction of the partition (0.0 to 1.0)
     * @param estimatedBytes estimated size of this sub-range in bytes
     */
    public record SubRange(
            int subRangeIndex,
            double startFraction,
            double endFraction,
            long estimatedBytes) {

        public SubRange {
            if (subRangeIndex < 0) {
                throw new IllegalArgumentException(
                        "subRangeIndex must be non-negative, got: " + subRangeIndex);
            }
            if (startFraction < 0.0 || startFraction > 1.0) {
                throw new IllegalArgumentException(
                        "startFraction must be in [0.0, 1.0], got: " + startFraction);
            }
            if (endFraction < 0.0 || endFraction > 1.0) {
                throw new IllegalArgumentException(
                        "endFraction must be in [0.0, 1.0], got: " + endFraction);
            }
            if (endFraction <= startFraction) {
                throw new IllegalArgumentException(
                        "endFraction must be > startFraction, got: start="
                                + startFraction + " end=" + endFraction);
            }
            if (estimatedBytes <= 0) {
                throw new IllegalArgumentException(
                        "estimatedBytes must be positive, got: " + estimatedBytes);
            }
        }
    }
}
