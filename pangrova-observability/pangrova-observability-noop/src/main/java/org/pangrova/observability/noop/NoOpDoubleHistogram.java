package org.pangrova.observability.noop;

import org.pangrova.common.api.annotation.Internal;

import org.pangrova.observability.api.metrics.DoubleHistogram;

/**
 * No-operation {@link DoubleHistogram} implementation.
 *
 * <p>Thread-safe by design as this class holds no mutable state.
 */
@Internal
public final class NoOpDoubleHistogram implements DoubleHistogram {

    static final NoOpDoubleHistogram INSTANCE = new NoOpDoubleHistogram();

    @Override
    public void record(double value) {
        // NoOp
    }
    /** Explicitly disabled telemetry discards the entire observation, including its attributes. */
    @Override
    public void record(double value, org.pangrova.observability.api.metrics.MetricAttributes attributes) {
        java.util.Objects.requireNonNull(attributes, "attributes");
    }
}
