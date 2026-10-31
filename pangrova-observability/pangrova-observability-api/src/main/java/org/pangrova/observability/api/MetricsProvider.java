package org.pangrova.observability.api;

import org.pangrova.common.api.annotation.Stable;

import java.util.List;
import java.util.function.Supplier;

/**
 * SPI for metrics collection.
 *
 * <p>Provides factory methods for creating counter, gauge, histogram,
 * and up-down counter instruments. Implementations bridge Pangrova's
 * lightweight metrics abstraction to a concrete metrics backend
 * (e.g. Micrometer, OpenTelemetry Metrics SDK).
 *
 * <p>A NoOp implementation is provided by the {@code pangrova-observability}
 * module and serves as the default fallback when no real backend is
 * configured.
 */
@Stable(since = "1.0")
public interface MetricsProvider {

    /**
     * Creates a monotonically increasing counter.
     *
     * @param name        the metric name
     * @param description a human-readable description
     * @param unit        the unit of measurement (e.g. "requests", "bytes")
     * @return a new {@link LongCounter}
     */
    LongCounter createCounter(String name, String description, String unit);

    /**
     * Creates an observable gauge backed by the given supplier.
     *
     * <p>The supplier is invoked at collection time to obtain the
     * current gauge value.
     *
     * @param name        the metric name
     * @param description a human-readable description
     * @param unit        the unit of measurement
     * @param supplier    the value supplier called at collection time
     * @return a new {@link ObservableDoubleGauge}
     */
    ObservableDoubleGauge createGauge(String name, String description,
                                       String unit, Supplier<Double> supplier);

    /**
     * Creates a histogram with the specified bucket boundaries.
     *
     * @param name        the metric name
     * @param description a human-readable description
     * @param unit        the unit of measurement
     * @param boundaries  explicit bucket boundaries for the histogram
     * @return a new {@link DoubleHistogram}
     */
    DoubleHistogram createHistogram(String name, String description,
                                     String unit, List<Double> boundaries);

    /**
     * Creates a counter that can both increase and decrease.
     *
     * @param name        the metric name
     * @param description a human-readable description
     * @param unit        the unit of measurement
     * @return a new {@link LongUpDownCounter}
     */
    LongUpDownCounter createUpDownCounter(String name, String description, String unit);
}
