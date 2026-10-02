package org.pangrova.spi.connector.data;

import org.pangrova.spi.annotation.Experimental;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Type-safe representation of a column value in a row.
 * Replaces raw Object in the Page row model, providing exhaustive
 * pattern matching and eliminating unchecked casts.
 *
 * <p>Supported value types cover all PangrovaType primitives used in
 * the Java-side data path (DML evaluation, integration tests).</p>
 */
@Experimental
public sealed interface ColumnValue {

    record IntValue(int value) implements ColumnValue {
    }

    record LongValue(long value) implements ColumnValue {
    }

    record FloatValue(float value) implements ColumnValue {
    }

    record DoubleValue(double value) implements ColumnValue {
    }

    record BooleanValue(boolean value) implements ColumnValue {
    }

    record StringValue(String value) implements ColumnValue {
    }

    record DecimalValue(BigDecimal value) implements ColumnValue {
    }

    record DateValue(LocalDate value) implements ColumnValue {
    }

    record TimestampValue(LocalDateTime value) implements ColumnValue {
    }

    record BytesValue(byte[] value) implements ColumnValue {
    }

    record ArrayValue(List<?> elements) implements ColumnValue {
        public ArrayValue {
            // SQL arrays may contain NULL elements. List.copyOf rejects them.
            elements = Collections.unmodifiableList(new ArrayList<>(elements));
        }
    }

    record RowValue(List<?> fields) implements ColumnValue {
        public RowValue {
            fields = Collections.unmodifiableList(new ArrayList<>(fields));
        }
    }

    record VectorValue(float[] value) implements ColumnValue {
        public VectorValue {
            value = value.clone();
        }
    }

    record MapValue(Map<?, ?> entries) implements ColumnValue {
        public MapValue {
            // SQL map keys are non-null, but values may be NULL. Preserve
            // insertion order and detach from the caller's mutable map.
            if (entries.keySet().stream().anyMatch(java.util.Objects::isNull)) {
                throw new IllegalArgumentException("Map keys must not be null");
            }
            entries = Collections.unmodifiableMap(new LinkedHashMap<>(entries));
        }
    }

    record NullValue() implements ColumnValue {
    }

    /** Wraps a raw Java value into the appropriate ColumnValue subtype. */
    static ColumnValue of(Object raw) {
        ColumnValue scalarValue = scalarValue(raw);
        if (scalarValue != null) {
            return scalarValue;
        }
        if (raw instanceof byte[] bytes) {
            return new BytesValue(bytes);
        }
        if (raw instanceof float[] floats) {
            return new VectorValue(floats);
        }
        if (raw instanceof Map<?, ?> map) {
            return new MapValue(map);
        }
        if (raw instanceof List<?> list) {
            return new ArrayValue(list);
        }
        throw new IllegalArgumentException(
                "Unsupported column value type: " + raw.getClass().getName());
    }

    /**
     * Extracts the raw Java value from a ColumnValue for framework boundaries.
     *
     * <p>{@code null} cannot signal "not handled" here, because {@link NullValue}
     * legitimately maps to {@code null}. The scalar helpers therefore return the
     * argument <em>itself</em> as an out-of-band sentinel, and this method compares
     * by identity ({@code !=}) rather than by value. A {@code ColumnValue} is never
     * its own raw value, so the sentinel can never collide with a real result.</p>
     *
     * @param columnValue the value to unwrap
     * @return the raw Java value, or {@code null} for {@link NullValue}
     * @throws IllegalArgumentException if the subtype is not handled
     */
    static Object toRaw(ColumnValue columnValue) {
        Object scalarValue = rawScalarValue(columnValue);
        // Identity comparison: see the sentinel contract in this method's Javadoc.
        if (scalarValue != columnValue) {
            return scalarValue;
        }
        if (columnValue instanceof BytesValue value) {
            return value.value();
        }
        if (columnValue instanceof VectorValue value) {
            return value.value().clone();
        }
        if (columnValue instanceof ArrayValue value) {
            return value.elements();
        }
        if (columnValue instanceof RowValue value) {
            return value.fields();
        }
        if (columnValue instanceof MapValue value) {
            return value.entries();
        }
        throw new IllegalArgumentException(
                "Unknown ColumnValue subtype: " + columnValue.getClass().getName());
    }

    private static ColumnValue scalarValue(Object raw) {
        if (raw == null) {
            return new NullValue();
        }
        if (raw instanceof Integer value) {
            return new IntValue(value);
        }
        if (raw instanceof Long value) {
            return new LongValue(value);
        }
        if (raw instanceof Float value) {
            return new FloatValue(value);
        }
        if (raw instanceof Double value) {
            return new DoubleValue(value);
        }
        if (raw instanceof Boolean value) {
            return new BooleanValue(value);
        }
        return textScalarValue(raw);
    }

    private static ColumnValue textScalarValue(Object raw) {
        if (raw instanceof String value) {
            return new StringValue(value);
        }
        if (raw instanceof BigDecimal value) {
            return new DecimalValue(value);
        }
        if (raw instanceof LocalDate value) {
            return new DateValue(value);
        }
        if (raw instanceof LocalDateTime value) {
            return new TimestampValue(value);
        }
        return null;
    }

    /**
     * Unwraps the numeric/boolean scalars.
     *
     * @return the raw value, or {@code columnValue} itself when the subtype is not a
     *     scalar — the identity sentinel documented on {@link #toRaw(ColumnValue)}
     */
    private static Object rawScalarValue(ColumnValue columnValue) {
        if (columnValue instanceof NullValue) {
            return null;
        }
        if (columnValue instanceof IntValue value) {
            return value.value();
        }
        if (columnValue instanceof LongValue value) {
            return value.value();
        }
        if (columnValue instanceof FloatValue value) {
            return value.value();
        }
        if (columnValue instanceof DoubleValue value) {
            return value.value();
        }
        if (columnValue instanceof BooleanValue value) {
            return value.value();
        }
        return rawTextScalarValue(columnValue);
    }

    /**
     * Unwraps the text/temporal scalars.
     *
     * @return the raw value, or {@code columnValue} itself when the subtype is not a
     *     scalar — the identity sentinel documented on {@link #toRaw(ColumnValue)}
     */
    private static Object rawTextScalarValue(ColumnValue columnValue) {
        if (columnValue instanceof StringValue value) {
            return value.value();
        }
        if (columnValue instanceof DecimalValue value) {
            return value.value();
        }
        if (columnValue instanceof DateValue value) {
            return value.value();
        }
        if (columnValue instanceof TimestampValue value) {
            return value.value();
        }
        return columnValue;
    }
}
