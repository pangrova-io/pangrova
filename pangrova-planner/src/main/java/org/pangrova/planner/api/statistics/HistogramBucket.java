package org.pangrova.planner.api.statistics;

import org.pangrova.common.api.annotation.Internal;

/**
 * A single bucket in an equi-depth histogram, carrying per-bucket statistics.
 *
 * @param lowerBound    inclusive lower bound of the bucket range
 * @param upperBound    exclusive upper bound of the bucket range
 * @param rowCount      number of rows in this bucket
 * @param distinctCount number of distinct values in this bucket
 */
@Internal
public record HistogramBucket(
        Comparable<?> lowerBound,
        Comparable<?> upperBound,
        long rowCount,
        long distinctCount) {

    public HistogramBucket {
        if (rowCount < 0) {
            throw new IllegalArgumentException("rowCount must be non-negative, got " + rowCount);
        }
        if (distinctCount < 0) {
            throw new IllegalArgumentException("distinctCount must be non-negative, got " + distinctCount);
        }
    }
}
