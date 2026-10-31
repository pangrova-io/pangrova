package org.pangrova.streaming.api.cdc;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Strong-typed immutable row model for CDC events.
 *
 * <p>Replaces {@code Map<String, Object>} / {@code TypedRow.fromRawMap()} in the CDC path.
 * Holds column name → value mappings with explicit types, supports schema evolution
 * (unknown fields preserved, missing fields default to null).</p>
 *
 * <p>All collections are defensively copied and unmodifiable.</p>
 */
public final class CdcRow {

    private final Map<String, Object> columns;

    private CdcRow(Map<String, Object> columns) {
        this.columns = java.util.Collections.unmodifiableMap(new LinkedHashMap<>(columns));
    }

    /**
     * Creates a CdcRow from a column map. Null values are preserved as explicit entries.
     *
     * @param columns column name → value pairs (must not be null)
     * @return immutable CdcRow
     */
    public static CdcRow of(Map<String, Object> columns) {
        Objects.requireNonNull(columns, "columns must not be null");
        return new CdcRow(columns);
    }

    /** Returns the value for the given column, or null if absent. */
    public Object get(String column) {
        return columns.get(column);
    }

    /** Returns the value as a String, or null if absent or not a String. */
    public String getString(String column) {
        Object val = columns.get(column);
        return val instanceof String s ? s : (val != null ? val.toString() : null);
    }

    /** Returns the value as a long, or defaultValue if absent or not numeric. */
    public long getLong(String column, long defaultValue) {
        Object val = columns.get(column);
        if (val instanceof Number n) return n.longValue();
        return defaultValue;
    }

    /** Returns the value as a double, or defaultValue if absent or not numeric. */
    public double getDouble(String column, double defaultValue) {
        Object val = columns.get(column);
        if (val instanceof Number n) return n.doubleValue();
        return defaultValue;
    }

    /** Returns the value as a boolean, or defaultValue if absent or not boolean. */
    public boolean getBoolean(String column, boolean defaultValue) {
        Object val = columns.get(column);
        if (val instanceof Boolean b) return b;
        return defaultValue;
    }

    /** Returns true if this row contains the given column (even if value is null). */
    public boolean containsColumn(String column) {
        return columns.containsKey(column);
    }

    /** Returns the set of column names in insertion order. */
    public Set<String> columnNames() {
        return columns.keySet();
    }

    /** Returns the number of columns. */
    public int columnCount() {
        return columns.size();
    }

    /** Returns an unmodifiable view of the underlying column map. */
    public Map<String, Object> toMap() {
        return columns;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CdcRow other)) return false;
        return columns.equals(other.columns);
    }

    @Override
    public int hashCode() {
        return columns.hashCode();
    }

    @Override
    public String toString() {
        return "CdcRow{columns=" + columns + "}";
    }
}
