package org.pangrova.connector.spi.data.value;

import org.pangrova.common.api.annotation.Experimental;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Type-safe row representation for the Java-side data path.
 * Replaces raw {@code Map<String, ColumnValue>} to provide compile-time
 * safety, explicit schema, and convenient typed accessors.
 *
 * <p>This is the canonical row exchange unit between connectors, the engine,
 * and coordinator-side DML/DDL execution.</p>
 */
@Experimental
public record TypedRow(Map<String, ColumnValue> values) {

    public TypedRow {
        // LinkedHashMap preserves insertion order — required for positional
        // column mapping in the result pipeline (remapRow, columnar IPC).
        values = java.util.Collections.unmodifiableMap(new LinkedHashMap<>(values));
    }

    /** Creates a TypedRow from column name-value pairs. */
    public static TypedRow of(Map<String, ColumnValue> values) {
        return new TypedRow(values);
    }

    /** Creates a mutable builder for constructing a TypedRow. */
    public static Builder builder() {
        return new Builder();
    }

    /** Returns the value for a column, or a null value when the column is absent. */
    public ColumnValue get(String column) {
        return values.getOrDefault(column, new ColumnValue.NullValue());
    }

    /** Returns whether this row contains the named column. */
    public boolean containsColumn(String column) {
        return values.containsKey(column);
    }

    /** Extracts the raw Java value for a column (convenience for assertions/ACL). */
    public Object getRaw(String column) {
        return ColumnValue.toRaw(get(column));
    }

    /** Returns the column names in their positional order. */
    public Set<String> columnNames() {
        return values.keySet();
    }

    /** Returns the number of columns in this row. */
    public int columnCount() {
        return values.size();
    }

    // ── Typed accessors ──

    /** Returns a string column value, or null when the value is null. */
    public String getString(String column) {
        ColumnValue cv = get(column);
        if (cv instanceof ColumnValue.StringValue s) return s.value();
        if (cv instanceof ColumnValue.NullValue) return null;
        throw typeMismatch(column, "String", cv);
    }

    /** Returns an integer column value, or null when the value is null. */
    public Integer getInt(String column) {
        ColumnValue cv = get(column);
        if (cv instanceof ColumnValue.IntValue i) return i.value();
        if (cv instanceof ColumnValue.NullValue) return null;
        throw typeMismatch(column, "Int", cv);
    }

    /** Returns a numeric column value as a long, or null when the value is null. */
    public Long getLong(String column) {
        ColumnValue cv = get(column);
        if (cv instanceof ColumnValue.LongValue l) return l.value();
        if (cv instanceof ColumnValue.IntValue i) return (long) i.value();
        if (cv instanceof ColumnValue.DoubleValue d) return (long) d.value();
        if (cv instanceof ColumnValue.DecimalValue dec) return dec.value().longValue();
        if (cv instanceof ColumnValue.NullValue) return null;
        throw typeMismatch(column, "Long", cv);
    }

    /** Returns a numeric column value as a double, or null when the value is null. */
    public Double getDouble(String column) {
        ColumnValue cv = get(column);
        if (cv instanceof ColumnValue.DoubleValue d) return d.value();
        if (cv instanceof ColumnValue.FloatValue f) return (double) f.value();
        if (cv instanceof ColumnValue.LongValue l) return (double) l.value();
        if (cv instanceof ColumnValue.IntValue i) return (double) i.value();
        if (cv instanceof ColumnValue.DecimalValue dec) return dec.value().doubleValue();
        if (cv instanceof ColumnValue.NullValue) return null;
        throw typeMismatch(column, "Double", cv);
    }

    /** Returns a boolean column value, or null when the value is null. */
    public Boolean getBoolean(String column) {
        ColumnValue cv = get(column);
        if (cv instanceof ColumnValue.BooleanValue b) {
            return b.value();
        }
        if (cv instanceof ColumnValue.NullValue) {
            return null;
        }
        throw typeMismatch(column, "Boolean", cv);
    }

    /** Returns a decimal column value, or null when the value is null. */
    public BigDecimal getDecimal(String column) {
        ColumnValue cv = get(column);
        if (cv instanceof ColumnValue.DecimalValue d) return d.value();
        if (cv instanceof ColumnValue.NullValue) return null;
        throw typeMismatch(column, "Decimal", cv);
    }

    /** Returns a date column value, or null when the value is null. */
    public LocalDate getDate(String column) {
        ColumnValue cv = get(column);
        if (cv instanceof ColumnValue.DateValue d) return d.value();
        if (cv instanceof ColumnValue.NullValue) return null;
        throw typeMismatch(column, "Date", cv);
    }

    /** Returns a timestamp column value, or null when the value is null. */
    public LocalDateTime getTimestamp(String column) {
        ColumnValue cv = get(column);
        if (cv instanceof ColumnValue.TimestampValue t) return t.value();
        if (cv instanceof ColumnValue.NullValue) return null;
        throw typeMismatch(column, "Timestamp", cv);
    }

    /** Converts this row to a raw Map for framework boundaries (Jackson, Calcite). */
    public Map<String, Object> toRawMap() {
        Map<String, Object> raw = new LinkedHashMap<>();
        values.forEach((k, v) -> raw.put(k, ColumnValue.toRaw(v)));
        return raw;
    }

    /** Creates a TypedRow from a raw Map (ACL boundary from the type system). */
    public static TypedRow fromRawMap(Map<String, Object> raw) {
        Map<String, ColumnValue> typed = new LinkedHashMap<>();
        raw.forEach((k, v) -> typed.put(k, ColumnValue.of(v)));
        return new TypedRow(typed);
    }

    /** Returns a new TypedRow with the specified column updated. */
    public TypedRow withColumn(String column, ColumnValue value) {
        Map<String, ColumnValue> copy = new LinkedHashMap<>(values);
        copy.put(column, value);
        return new TypedRow(copy);
    }

    private static IllegalArgumentException typeMismatch(
            String column, String expected, ColumnValue actual) {
        return new IllegalArgumentException(
                "Column '" + column + "' expected " + expected
                        + " but was " + actual.getClass().getSimpleName());
    }

    /** Mutable builder for constructing TypedRow instances. */
    public static final class Builder {
        private final Map<String, ColumnValue> values = new LinkedHashMap<>();

        private Builder() {
        }

        /** Adds a typed column value. */
        public Builder put(String column, ColumnValue value) {
            values.put(column, value);
            return this;
        }

        /** Adds a nullable string column value. */
        public Builder putString(String column, String value) {
            values.put(column, value == null
                    ? new ColumnValue.NullValue()
                    : new ColumnValue.StringValue(value));
            return this;
        }

        /** Adds an integer column value. */
        public Builder putInt(String column, int value) {
            values.put(column, new ColumnValue.IntValue(value));
            return this;
        }

        /** Adds a long column value. */
        public Builder putLong(String column, long value) {
            values.put(column, new ColumnValue.LongValue(value));
            return this;
        }

        /** Converts and adds a raw Java column value. */
        public Builder putRaw(String column, Object value) {
            values.put(column, ColumnValue.of(value));
            return this;
        }

        /** Builds an immutable typed row. */
        public TypedRow build() {
            return new TypedRow(values);
        }
    }
}
