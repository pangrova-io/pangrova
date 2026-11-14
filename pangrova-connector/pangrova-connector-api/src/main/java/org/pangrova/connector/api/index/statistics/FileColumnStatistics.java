package org.pangrova.connector.api.index.statistics;

import org.pangrova.connector.api.enums.ColumnComparisonOperator;
import org.pangrova.common.api.annotation.Experimental;

/**
 * Column-level statistics for a single data file, used for L0 partition pruning.
 */
@Experimental
public record FileColumnStatistics(
        String columnName,
        Comparable<?> minValue,
        Comparable<?> maxValue,
        long nullCount,
        long rowCount,
        long approximateNdv,
        long totalSizeBytes) {

    /**
     * Returns true if no rows in this file can satisfy the given comparison.
     * For example, if op is GT and value >= maxValue, no rows satisfy.
     */
    @SuppressWarnings("unchecked")
    public boolean noRowsSatisfy(ColumnComparisonOperator op, Comparable<?> value) {
        if (minValue == null || maxValue == null) {
            return false;
        }
        if (!minValue.getClass().isInstance(value)) {
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

    /**
     * Returns true if all rows in this file satisfy the given comparison.
     */
    @SuppressWarnings("unchecked")
    public boolean allRowsSatisfy(ColumnComparisonOperator op, Comparable<?> value) {
        if (minValue == null || maxValue == null || nullCount > 0) {
            return false;
        }
        if (!minValue.getClass().isInstance(value)) {
            return false;
        }
        Comparable<Object> v = (Comparable<Object>) value;
        Comparable<Object> min = (Comparable<Object>) minValue;
        Comparable<Object> max = (Comparable<Object>) maxValue;
        return switch (op) {
            case EQUAL -> min.compareTo(max) == 0 && v.compareTo(min) == 0;
            case LESS_THAN -> max.compareTo(v) < 0;
            case LESS_THAN_OR_EQUAL -> max.compareTo(v) <= 0;
            case GREATER_THAN -> min.compareTo(v) > 0;
            case GREATER_THAN_OR_EQUAL -> min.compareTo(v) >= 0;
        };
    }

}
