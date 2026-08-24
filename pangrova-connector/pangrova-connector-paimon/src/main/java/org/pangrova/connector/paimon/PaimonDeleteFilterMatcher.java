package org.pangrova.connector.paimon;

import org.pangrova.spi.connector.metadata.DeleteFilter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.DataField;

import java.util.List;
import java.util.function.IntPredicate;

/**
 * Evaluates a {@link DeleteFilter} predicate against a Paimon
 * {@link InternalRow} for the rewrite-based delete path.
 */
final class PaimonDeleteFilterMatcher {

    private PaimonDeleteFilterMatcher() {}

    /** Returns true when the row satisfies the delete filter predicate. */
    static boolean matches(InternalRow row, List<DataField> fields, DeleteFilter filter) {
        if (filter instanceof DeleteFilter.All) {
            return true;
        }
        if (filter instanceof DeleteFilter.And f) {
            return f.filters().stream().allMatch(sub -> matches(row, fields, sub));
        }
        if (filter instanceof DeleteFilter.Or f) {
            return f.filters().stream().anyMatch(sub -> matches(row, fields, sub));
        }
        if (filter instanceof DeleteFilter.Not f) {
            return !matches(row, fields, f.filter());
        }
        return matchesLeaf(row, fields, filter);
    }

    private static boolean matchesLeaf(InternalRow row, List<DataField> fields, DeleteFilter filter) {
        if (filter instanceof DeleteFilter.Equal f) {
            return isComparison(row, fields, f.column(), f.value(), c -> c == 0);
        }
        if (filter instanceof DeleteFilter.NotEqual f) {
            return isNotEqual(row, fields, f.column(), f.value());
        }
        if (filter instanceof DeleteFilter.IsNull f) {
            return getFieldByName(row, fields, f.column()) == null;
        }
        if (filter instanceof DeleteFilter.IsNotNull f) {
            return getFieldByName(row, fields, f.column()) != null;
        }
        if (filter instanceof DeleteFilter.In f) {
            return matchesIn(row, fields, f.column(), f.values());
        }
        return matchesRangeComparison(row, fields, filter);
    }

    private static boolean matchesRangeComparison(InternalRow row, List<DataField> fields,
                                                  DeleteFilter filter) {
        if (filter instanceof DeleteFilter.LessThan f) {
            return isComparison(row, fields, f.column(), f.value(), c -> c < 0);
        }
        if (filter instanceof DeleteFilter.LessThanOrEqual f) {
            return isComparison(row, fields, f.column(), f.value(), c -> c <= 0);
        }
        if (filter instanceof DeleteFilter.GreaterThan f) {
            return isComparison(row, fields, f.column(), f.value(), c -> c > 0);
        }
        if (filter instanceof DeleteFilter.GreaterThanOrEqual f) {
            return isComparison(row, fields, f.column(), f.value(), c -> c >= 0);
        }
        throw new IllegalArgumentException(
                "Unsupported delete filter: " + filter.getClass().getSimpleName());
    }

    private static boolean isComparison(InternalRow row, List<DataField> fields,
                                        String column, Object value, IntPredicate test) {
        Object val = getFieldByName(row, fields, column);
        return val != null && test.test(compareFilterValue(val, value));
    }

    private static boolean isNotEqual(InternalRow row, List<DataField> fields,
                                      String column, Object value) {
        Object val = getFieldByName(row, fields, column);
        return val == null || compareFilterValue(val, value) != 0;
    }

    private static boolean matchesIn(InternalRow row, List<DataField> fields,
                                     String column, List<Object> values) {
        Object val = getFieldByName(row, fields, column);
        if (val == null) {
            return false;
        }
        return values.stream().anyMatch(v -> compareFilterValue(val, v) == 0);
    }

    private static Object getFieldByName(InternalRow row, List<DataField> fields, String name) {
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).name().equalsIgnoreCase(name)) {
                return PaimonInternalRows.getFieldValue(row, i, fields.get(i));
            }
        }
        return null;
    }

    private static int compareFilterValue(Object fieldVal, Object filterVal) {
        if (fieldVal instanceof Number fn && filterVal instanceof Number fv) {
            return Double.compare(fn.doubleValue(), fv.doubleValue());
        }
        if (fieldVal instanceof BinaryString bs) {
            return bs.toString().compareTo(filterVal.toString());
        }
        return fieldVal.toString().compareTo(filterVal.toString());
    }
}
