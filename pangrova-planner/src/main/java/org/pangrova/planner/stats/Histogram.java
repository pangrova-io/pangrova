package org.pangrova.planner.stats;

import java.util.List;
import java.util.Objects;

/**
 * Equi-depth histogram for column value distribution.
 * Each bucket carries row count and distinct count for selectivity estimation.
 *
 * @param bucketCount number of buckets (typically 256)
 * @param buckets     per-bucket statistics with lower/upper bounds, row count, distinct count
 */
public record Histogram(int bucketCount, List<HistogramBucket> buckets) {

    public Histogram {
        Objects.requireNonNull(buckets, "buckets must not be null");
        if (bucketCount <= 0) {
            throw new IllegalArgumentException("bucketCount must be positive");
        }
        buckets = List.copyOf(buckets);
    }

    /** Total row count across all buckets. */
    public long totalRows() {
        return buckets.stream().mapToLong(HistogramBucket::rowCount).sum();
    }
}
