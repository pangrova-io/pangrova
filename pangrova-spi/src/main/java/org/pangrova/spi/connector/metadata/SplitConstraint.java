package org.pangrova.spi.connector.metadata;

import java.util.Map;

import org.pangrova.spi.annotation.Experimental;

/**
 * Constraint predicates carried by a split for native-side split skipping.
 *
 * <p>Encapsulates partition values and file-level column statistics (min/max)
 * that describe the data range within a split. The native execution engine
 * uses these predicates to skip irrelevant splits without reading file data.</p>
 *
 * <p>Immutable and thread-safe.</p>
 *
 * @param partitionValues partition column name → value pairs (identity partitions)
 * @param columnStatistics per-column file-level min/max statistics for data skipping
 */
@Experimental
public record SplitConstraint(
        Map<String, String> partitionValues,
        Map<String, ColumnStatistics> columnStatistics
) {

    /** Empty constraint — no partition values or statistics available. */
    public static final SplitConstraint NONE = new SplitConstraint(Map.of(), Map.of());

    public SplitConstraint {
        partitionValues = partitionValues == null ? Map.of() : Map.copyOf(partitionValues);
        columnStatistics = columnStatistics == null ? Map.of() : Map.copyOf(columnStatistics);
    }

    /**
     * Returns true if this constraint has any predicate information.
     */
    public boolean hasPredicates() {
        return !partitionValues.isEmpty() || !columnStatistics.isEmpty();
    }

    /**
     * File-level column statistics for data skipping.
     *
     * @param min minimum value as string (null if unknown)
     * @param max maximum value as string (null if unknown)
     * @param nullCount number of null values (-1 if unknown)
     */
    public record ColumnStatistics(String min, String max, long nullCount) {

        public ColumnStatistics {
            nullCount = nullCount < 0 ? -1 : nullCount;
        }

        /** Creates statistics with only min/max bounds. */
        public static ColumnStatistics of(String min, String max) {
            return new ColumnStatistics(min, max, -1);
        }

        /** Creates statistics with min/max and null count. */
        public static ColumnStatistics of(String min, String max, long nullCount) {
            return new ColumnStatistics(min, max, nullCount);
        }

        /** Returns true if min bound is available. */
        public boolean hasMin() {
            return min != null;
        }

        /** Returns true if max bound is available. */
        public boolean hasMax() {
            return max != null;
        }
    }
}
