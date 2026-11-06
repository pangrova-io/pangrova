package org.pangrova.observability.sdk.metrics.otel;

import org.pangrova.observability.api.metrics.LongUpDownCounter;
import org.pangrova.observability.api.metrics.MetricAttributes;

/**
 * Wraps {@link io.opentelemetry.api.metrics.LongUpDownCounter} behind Pangrova's SPI.
 */
public final class OtelLongUpDownCounter implements LongUpDownCounter {

    private final io.opentelemetry.api.metrics.LongUpDownCounter delegate;

    OtelLongUpDownCounter(io.opentelemetry.api.metrics.LongUpDownCounter delegate) {
        this.delegate = delegate;
    }

    @Override
    public void add(long value) {
        delegate.add(value);
    }

    @Override
    public void add(long value, MetricAttributes attributes) {
        delegate.add(value, OtelAttributeConverter.toOtel(attributes));
    }
}
