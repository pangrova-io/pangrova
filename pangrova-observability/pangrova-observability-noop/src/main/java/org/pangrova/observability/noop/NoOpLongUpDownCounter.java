package org.pangrova.observability.noop;

import org.pangrova.common.api.annotation.Internal;

import org.pangrova.observability.api.metrics.LongUpDownCounter;

/**
 * No-operation {@link LongUpDownCounter} implementation.
 *
 * <p>Thread-safe by design as this class holds no mutable state.
 */
@Internal
public final class NoOpLongUpDownCounter implements LongUpDownCounter {

    static final NoOpLongUpDownCounter INSTANCE = new NoOpLongUpDownCounter();

    @Override
    public void add(long value) {
        // NoOp
    }
    /** Explicitly disabled telemetry discards the entire observation, including its attributes. */
    @Override
    public void add(long value, org.pangrova.observability.api.metrics.MetricAttributes attributes) {
        java.util.Objects.requireNonNull(attributes, "attributes");
    }
}
