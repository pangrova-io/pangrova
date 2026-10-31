package org.pangrova.indexing.api.statistics;

import java.util.List;
import org.pangrova.connector.spi.index.statistics.FileColumnStatistics;

/**
 * Build-side summary statistics for probe-side dynamic filtering (JOIN pruning).
 *
 * <p>During a hash-join build phase, the coordinator collects min/max ranges
 * and optional bloom filter digests from the build side. These summaries are
 * pushed to probe-side workers as dynamic filters, allowing them to skip
 * splits whose column ranges do not overlap with the build-side range.</p>
 *
 * <p>This implements the build-side min/max/bloom summary for probe-side
 * split pruning required by G-INDEX-03.</p>
 *
 * @param queryId        the query this summary belongs to
 * @param joinId         identifies the specific join within the query
 * @param columnSummaries per-column min/max/bloom summaries from the build side
 */
public record DynamicFilterSummary(
        String queryId,
        String joinId,
        List<ColumnRangeSummary> columnSummaries) {

    public DynamicFilterSummary {
        columnSummaries = List.copyOf(columnSummaries);
    }

    /**
     * Per-column range summary from the build side of a join.
     *
     * @param columnName  the join key column name
     * @param minValue    minimum value seen on the build side (null if unknown)
     * @param maxValue    maximum value seen on the build side (null if unknown)
     * @param bloomFilter serialized bloom filter bytes (empty if not computed)
     * @param ndv         approximate number of distinct values (0 if unknown)
     */
    public record ColumnRangeSummary(
            String columnName,
            Comparable<?> minValue,
            Comparable<?> maxValue,
            byte[] bloomFilter,
            long ndv) {

        public ColumnRangeSummary {
            if (columnName == null || columnName.isBlank()) {
                throw new IllegalArgumentException("columnName must not be null or blank");
            }
            if (bloomFilter == null) {
                bloomFilter = new byte[0];
            }
        }

        /**
         * Returns true if a probe-side file with the given column statistics
         * can be pruned (no rows can match the build-side range).
         */
        @SuppressWarnings("unchecked")
        public boolean canPrune(FileColumnStatistics probeStats) {
            if (minValue == null || maxValue == null
                    || probeStats.minValue() == null || probeStats.maxValue() == null) {
                return false;
            }
            Comparable<Object> buildMin = (Comparable<Object>) minValue;
            Comparable<Object> buildMax = (Comparable<Object>) maxValue;
            Comparable<Object> probeMin = (Comparable<Object>) probeStats.minValue();
            Comparable<Object> probeMax = (Comparable<Object>) probeStats.maxValue();
            return probeMax.compareTo(buildMin) < 0 || probeMin.compareTo(buildMax) > 0;
        }
    }
}
