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
    public boolean noRowsSatisfy(ColumnComparisonOperator op, Comparable<?> value) {
        var comparison = compareBounds(value);
        if (comparison == null) {
            return false;
        }
        // File bounds may omit NaN, while SQL orders NaN after finite numbers.
        // Without a NaN count, a finite maximum cannot exclude a greater value.
        if (isFloating(value) && (op == ColumnComparisonOperator.GREATER_THAN
                || op == ColumnComparisonOperator.GREATER_THAN_OR_EQUAL)) {
            return false;
        }
        return switch (op) {
            case EQUAL -> comparison.toMin() < 0 || comparison.toMax() > 0;
            case LESS_THAN -> comparison.toMin() <= 0;
            case LESS_THAN_OR_EQUAL -> comparison.toMin() < 0;
            case GREATER_THAN -> comparison.toMax() >= 0;
            case GREATER_THAN_OR_EQUAL -> comparison.toMax() > 0;
        };
    }

    /** Returns true if all rows in this file satisfy the given comparison. */
    public boolean allRowsSatisfy(ColumnComparisonOperator op, Comparable<?> value) {
        if (nullCount != 0) {
            return false;
        }
        var comparison = compareBounds(value);
        if (comparison == null) {
            return false;
        }
        // An unreported NaN invalidates equality and upper-bound proofs.
        if (isFloating(value) && op != ColumnComparisonOperator.GREATER_THAN
                && op != ColumnComparisonOperator.GREATER_THAN_OR_EQUAL) {
            return false;
        }
        return switch (op) {
            case EQUAL -> comparison.toMin() == 0 && comparison.toMax() == 0;
            case LESS_THAN -> comparison.toMax() > 0;
            case LESS_THAN_OR_EQUAL -> comparison.toMax() >= 0;
            case GREATER_THAN -> comparison.toMin() < 0;
            case GREATER_THAN_OR_EQUAL -> comparison.toMin() <= 0;
        };
    }

    private record BoundComparison(int toMin, int toMax) {}

    private BoundComparison compareBounds(Comparable<?> value) {
        if (minValue == null || maxValue == null || value == null
                || minValue.getClass() != value.getClass() || maxValue.getClass() != value.getClass()) {
            return null;
        }
        if (value instanceof String text && (!validUnicode(text)
                || !validUnicode((String) minValue) || !validUnicode((String) maxValue))) {
            return null;
        }
        Integer lower = compareSql(value, minValue);
        Integer upper = compareSql(value, maxValue);
        Integer order = compareSql(minValue, maxValue);
        return lower == null || upper == null || order == null || order > 0
                ? null : new BoundComparison(lower, upper);
    }

    private static boolean isFloating(Comparable<?> value) {
        return value instanceof Double || value instanceof Float;
    }

    @SuppressWarnings("unchecked")
    private static Integer compareSql(Comparable<?> left, Comparable<?> right) {
        if (left instanceof String a && right instanceof String b) {
            // Connector string bounds use unsigned UTF-8 ordering. UTF-16's
            // natural order differs for supplementary code points.
            return compareCodePoints(a, b);
        }
        if (left instanceof Double a && right instanceof Double b) {
            if (a.isNaN() || b.isNaN()) {
                return null;
            }
            return a.doubleValue() == b.doubleValue() ? 0 : Double.compare(a, b);
        }
        if (left instanceof Float a && right instanceof Float b) {
            if (a.isNaN() || b.isNaN()) {
                return null;
            }
            return a.floatValue() == b.floatValue() ? 0 : Float.compare(a, b);
        }
        return ((Comparable<Object>) left).compareTo(right);
    }

    private static boolean validUnicode(String value) {
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            if (Character.isHighSurrogate(ch)) {
                if (++i == value.length() || !Character.isLowSurrogate(value.charAt(i))) {
                    return false;
                }
            } else if (Character.isLowSurrogate(ch)) {
                return false;
            }
        }
        return true;
    }

    private static int compareCodePoints(String left, String right) {
        int li = 0;
        int ri = 0;
        while (li < left.length() && ri < right.length()) {
            int l = left.codePointAt(li);
            int r = right.codePointAt(ri);
            if (l != r) {
                return Integer.compare(l, r);
            }
            li += Character.charCount(l);
            ri += Character.charCount(r);
        }
        return Integer.compare(left.length() - li, right.length() - ri);
    }
}
