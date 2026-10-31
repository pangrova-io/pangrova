package org.pangrova.observability.api.noop;

import java.util.List;
import java.util.function.Supplier;

import org.pangrova.observability.api.DoubleHistogram;
import org.pangrova.observability.api.LongCounter;
import org.pangrova.observability.api.LongUpDownCounter;
import org.pangrova.observability.api.MetricsProvider;
import org.pangrova.observability.api.ObservableDoubleGauge;

/**
 * No-operation {@link MetricsProvider} implementation.
 *
 * <p>All factory methods return shared singleton instances. Thread-safe by design.
 */
public class NoOpMetricsProvider implements MetricsProvider {

    private static final LongCounter NOOP_COUNTER = NoOpLongCounter.INSTANCE;
    private static final DoubleHistogram NOOP_HISTOGRAM = NoOpDoubleHistogram.INSTANCE;
    private static final LongUpDownCounter NOOP_UP_DOWN = NoOpLongUpDownCounter.INSTANCE;

    @Override
    public LongCounter createCounter(String name, String description, String unit) {
        return NOOP_COUNTER;
    }

    @Override
    public ObservableDoubleGauge createGauge(String name, String description,
                                              String unit, Supplier<Double> supplier) {
        return NoOpObservableDoubleGauge.INSTANCE;
    }

    @Override
    public DoubleHistogram createHistogram(String name, String description,
                                            String unit, List<Double> boundaries) {
        return NOOP_HISTOGRAM;
    }

    @Override
    public LongUpDownCounter createUpDownCounter(String name, String description, String unit) {
        return NOOP_UP_DOWN;
    }
}
