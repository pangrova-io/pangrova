package org.pangrova.indexing.api.statistics;

import org.pangrova.connector.api.enums.ColumnComparisonOperator;
import org.pangrova.common.api.annotation.Experimental;

import java.util.Map;
import org.pangrova.connector.api.index.statistics.FileColumnStatistics;

/**
 * Per-row-group column statistics for sub-file pruning (zone-map / skip index).
 *
 * <p>Each instance describes one row group inside a columnar file (Parquet, ORC).
 * The pruner evaluates query predicates against these statistics to decide
 * whether the row group can be skipped entirely.</p>
 *
 * @param rowGroupIndex zero-based ordinal of the row group within the file
 * @param rowCount      number of rows in this row group
 * @param columnStats   per-column min/max/null statistics keyed by column name
 */
@Experimental
public record RowGroupStatistics(
        int rowGroupIndex,
        long rowCount,
        Map<String, ColumnStats> columnStats) {

    public RowGroupStatistics {
        columnStats = columnStats == null ? Map.of() : Map.copyOf(columnStats);
    }

    /**
     * Min/max statistics for a single column within a single row group.
     *
     * @param columnName column name
     * @param minValue   minimum value (null if unknown)
     * @param maxValue   maximum value (null if unknown)
     * @param nullCount  number of nulls (-1 if unknown)
     */
    public record ColumnStats(
            String columnName,
            Comparable<?> minValue,
            Comparable<?> maxValue,
            long nullCount) {

        public ColumnStats {
            nullCount = nullCount < 0 ? -1 : nullCount;
        }

        /** Returns true if min/max bounds are both available. */
        public boolean hasBounds() {
            return minValue != null && maxValue != null;
        }

        /**
         * Returns true if no rows in this row group can satisfy the given comparison.
         * Mirrors {@link FileColumnStatistics#noRowsSatisfy} at the row-group level.
         */
        @SuppressWarnings("unchecked")
        public boolean noRowsSatisfy(ColumnComparisonOperator op, Comparable<?> value) {
            if (minValue == null || maxValue == null) {
                return false;
            }
            Comparable<Object> v = (Comparable<Object>) value;
            Comparable<Object> min = (Comparable<Object>) minValue;
            Comparable<Object> max = (Comparable<Object>) maxValue;
            return switch (op) {
                case EQUAL -> v.compareTo(min) < 0 || v.compareTo(max) > 0;
                case LESS_THAN -> v.compareTo(min) <= 0;
                case LESS_THAN_OR_EQUAL -> v.compareTo(min) < 0;
                case GREATER_THAN -> v.compareTo(max) >= 0;
                case GREATER_THAN_OR_EQUAL -> v.compareTo(max) > 0;
            };
        }
    }
}
