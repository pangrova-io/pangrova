package org.pangrova.planner.api.statistics;

import org.pangrova.common.api.annotation.Internal;

import java.util.Objects;
import java.util.Optional;

/**
 * Per-column statistics used by the CBO cost model.
 *
 * @param rowCount        total row count
 * @param nullCount       number of null values
 * @param min             minimum value
 * @param max             maximum value
 * @param ndv             number of distinct values (HyperLogLog estimate)
 * @param avgSize         average value size in bytes
 * @param histogram       optional equi-height histogram (256 buckets)
 * @param confidenceScore confidence in [0.1, 1.0] based on staleness
 */
@Internal
public record ColumnStatistics(
        long rowCount,
        long nullCount,
        Comparable<?> min,
        Comparable<?> max,
        long ndv,
        double avgSize,
        Optional<Histogram> histogram,
        double confidenceScore) {

    private static final double MIN_CONFIDENCE = 0.1;
    private static final double MAX_CONFIDENCE = 1.0;

    public ColumnStatistics {
        Objects.requireNonNull(histogram, "histogram must not be null (use Optional.empty())");
        if (confidenceScore < MIN_CONFIDENCE || confidenceScore > MAX_CONFIDENCE) {
            throw new IllegalArgumentException(
                    "confidenceScore must be in [0.1, 1.0], got " + confidenceScore);
        }
    }
}
