package org.pangrova.streaming.aggregation;

import org.pangrova.spi.connector.data.value.ColumnValue;

import java.util.HashMap;
import java.util.Map;

/**
 * Streaming COUNT DISTINCT accumulator with retraction support.
 *
 * <p>Exact mode: maintains a frequency map of distinct values. Each value
 * tracks how many times it has been accumulated (to handle retractions correctly).
 * A value is counted as distinct when its frequency is greater than zero.</p>
 *
 * <p>For approximate mode, use {@link HyperLogLogCountDistinctAccumulator}.</p>
 */
public final class CountDistinctAccumulator implements StreamingAccumulator {

    private final Map<ColumnValue, Integer> frequencies = new HashMap<>();

    @Override
    public void accumulate(ColumnValue value) {
        if (value instanceof ColumnValue.NullValue) {
            return;
        }
        frequencies.merge(value, 1, Integer::sum);
    }

    @Override
    public void retract(ColumnValue value) {
        if (value instanceof ColumnValue.NullValue) {
            return;
        }
        frequencies.computeIfPresent(value, (k, count) -> count <= 1 ? null : count - 1);
    }

    @Override
    public ColumnValue result() {
        return new ColumnValue.LongValue(frequencies.size());
    }

    @Override
    public void reset() {
        frequencies.clear();
    }

    /**
     * Returns the current number of distinct values (convenience accessor).
     */
    public long distinctCount() {
        return frequencies.size();
    }
}
