package org.pangrova.observability.sdk.metrics.provider.otel;

import org.pangrova.spi.observability.LongCounter;
import org.pangrova.spi.observability.MetricAttributes;

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
