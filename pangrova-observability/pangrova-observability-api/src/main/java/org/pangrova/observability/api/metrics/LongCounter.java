package org.pangrova.observability.api.metrics;

import org.pangrova.common.api.annotation.Experimental;

/**
 * A monotonically increasing counter that records {@code long} values.
 *
 * <p>Counters are used to measure cumulative values that only go up,
 * such as the number of requests processed or bytes sent.
 */
@Experimental
public interface LongCounter {

    /**
     * Adds the given value to the counter.
     *
     * @param value the increment; must be non-negative
     */
    void add(long value);

    /**
     * Adds the given value to the counter with dimensional attributes.
     *
     * @param value      the increment; must be non-negative
     * @throws UnsupportedOperationException when nonempty attributes are not supported
     * @param attributes key-value pairs for dimensional classification
     */
    default void add(long value, MetricAttributes attributes) {
        if (!java.util.Objects.requireNonNull(attributes, "attributes").isEmpty()) {
            throw new UnsupportedOperationException("Metric implementation does not support dimensional attributes");
        }
        add(value);
    }
}
