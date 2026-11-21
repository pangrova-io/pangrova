package org.pangrova.observability.noop;

import org.pangrova.common.api.annotation.Internal;

import org.pangrova.observability.api.metrics.LongCounter;

/**
 * No-operation {@link LongCounter} implementation.
 *
 * <p>Thread-safe by design as this class holds no mutable state.
 */
@Internal
public final class NoOpLongCounter implements LongCounter {

    static final NoOpLongCounter INSTANCE = new NoOpLongCounter();

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
