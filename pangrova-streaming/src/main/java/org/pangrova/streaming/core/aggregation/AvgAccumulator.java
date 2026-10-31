package org.pangrova.streaming.core.aggregation;

import org.pangrova.connector.spi.data.value.ColumnValue;

/**
 * Streaming AVG accumulator with retraction semantics.
 *
 * <p>Maintains running sum and count. On INSERT, adds to both.
 * On DELETE (retraction), subtracts from both. This ensures correct
 * AVG computation even when upstream retracts previously emitted rows.</p>
 *
 * <p>Handles numeric types: IntValue, LongValue, FloatValue, DoubleValue.
 * Returns DoubleValue as the result (AVG is always floating-point).</p>
 */
public final class AvgAccumulator implements StreamingAccumulator {

    private double sum;
    private long count;

    @Override
    public void accumulate(ColumnValue value) {
        if (value instanceof ColumnValue.NullValue) {
            return;
        }
        sum += extractNumeric(value);
        count++;
    }

    @Override
    public void retract(ColumnValue value) {
        if (value instanceof ColumnValue.NullValue) {
            return;
        }
        sum -= extractNumeric(value);
        count--;
    }

    @Override
    public ColumnValue result() {
        if (count <= 0) {
            return new ColumnValue.NullValue();
        }
        return new ColumnValue.DoubleValue(sum / count);
    }

    @Override
    public void reset() {
        sum = 0.0;
        count = 0;
    }

    public long count() {
        return count;
    }

    public double sum() {
        return sum;
    }

    private static double extractNumeric(ColumnValue value) {
        if (value instanceof ColumnValue.IntValue v) return v.value();
        if (value instanceof ColumnValue.LongValue v) return v.value();
        if (value instanceof ColumnValue.FloatValue v) return v.value();
        if (value instanceof ColumnValue.DoubleValue v) return v.value();
        throw new IllegalArgumentException(
                "AVG requires numeric input, got: " + value.getClass().getSimpleName());
    }
}
