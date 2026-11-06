package org.pangrova.observability.sdk.metrics.otel;

import io.opentelemetry.api.metrics.Meter;
import org.pangrova.observability.api.metrics.DoubleHistogram;
import org.pangrova.observability.api.metrics.LongCounter;
import org.pangrova.observability.api.metrics.LongUpDownCounter;
import org.pangrova.observability.spi.metrics.MetricsProvider;
import org.pangrova.observability.api.metrics.ObservableDoubleGauge;

import java.util.List;
import java.util.function.Supplier;

/**
 * OpenTelemetry-backed {@link MetricsProvider} implementation.
 * Creates wrapped OTel metric instruments via the configured {@link Meter}.
 */
public final class OtelMetricsProvider implements MetricsProvider {

    private final Meter meter;

    public OtelMetricsProvider(Meter meter) {
        this.meter = meter;
    }

    @Override
    public LongCounter createCounter(String name, String description, String unit) {
        io.opentelemetry.api.metrics.LongCounter counter = meter.counterBuilder(name)
                .setDescription(description)
                .setUnit(unit)
                .build();
        return new OtelLongCounter(counter);
    }

    @Override
    public ObservableDoubleGauge createGauge(String name, String description,
                                              String unit, Supplier<Double> supplier) {
        io.opentelemetry.api.metrics.ObservableDoubleGauge gauge =
                meter.gaugeBuilder(name)
                        .setDescription(description)
                        .setUnit(unit)
                        .buildWithCallback(measurement -> measurement.record(supplier.get()));
        return new OtelObservableDoubleGauge(gauge);
    }

    @Override
    public DoubleHistogram createHistogram(String name, String description,
                                            String unit, List<Double> boundaries) {
        io.opentelemetry.api.metrics.DoubleHistogram histogram =
                meter.histogramBuilder(name)
                        .setDescription(description)
                        .setUnit(unit)
                        .setExplicitBucketBoundariesAdvice(boundaries)
                        .build();
        return new OtelDoubleHistogram(histogram);
    }

    @Override
    public LongUpDownCounter createUpDownCounter(String name, String description, String unit) {
        io.opentelemetry.api.metrics.LongUpDownCounter counter =
                meter.upDownCounterBuilder(name)
                        .setDescription(description)
                        .setUnit(unit)
                        .build();
        return new OtelLongUpDownCounter(counter);
    }
}
