package org.pangrova.observability.api;

import org.pangrova.common.api.annotation.Experimental;

/**
 * A counter that can both increase and decrease, recording {@code long} values.
 *
 * <p>Up-down counters are used to measure values that fluctuate,
 * such as the number of active connections or in-flight requests.
 */
@Experimental
public interface LongUpDownCounter {

    /**
     * Adds the given value to the counter. The value may be negative
     * to represent a decrease.
     *
     * @param value the delta to add (positive or negative)
     */
    void add(long value);

    /**
     * Adds the given value to the counter with dimensional attributes.
     *
     * @param value      the delta to add (positive or negative)
     * @param attributes key-value pairs for dimensional classification
     */
    default void add(long value, MetricAttributes attributes) {
        add(value);
    }
}
