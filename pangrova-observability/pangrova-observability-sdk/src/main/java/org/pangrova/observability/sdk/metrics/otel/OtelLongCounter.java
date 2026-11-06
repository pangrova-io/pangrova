package org.pangrova.observability.sdk.metrics.otel;

import org.pangrova.observability.api.metrics.LongCounter;
import org.pangrova.observability.api.metrics.MetricAttributes;

/**
 * Wraps {@link io.opentelemetry.api.metrics.LongCounter} behind Pangrova's SPI.
 */
public final class OtelLongCounter implements LongCounter {

    private final io.opentelemetry.api.metrics.LongCounter delegate;

    OtelLongCounter(io.opentelemetry.api.metrics.LongCounter delegate) {
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
