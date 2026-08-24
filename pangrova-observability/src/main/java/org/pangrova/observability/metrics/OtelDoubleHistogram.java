package org.pangrova.observability.metrics;

import org.pangrova.spi.observability.DoubleHistogram;
import org.pangrova.spi.observability.MetricAttributes;

/**
 * Wraps {@link io.opentelemetry.api.metrics.DoubleHistogram} behind Pangrova's SPI.
 */
public final class OtelDoubleHistogram implements DoubleHistogram {

    private final io.opentelemetry.api.metrics.DoubleHistogram delegate;

    OtelDoubleHistogram(io.opentelemetry.api.metrics.DoubleHistogram delegate) {
        this.delegate = delegate;
    }

    @Override
    public void record(double value) {
        delegate.record(value);
    }

    @Override
    public void record(double value, MetricAttributes attributes) {
        delegate.record(value, OtelAttributeConverter.toOtel(attributes));
    }
}
