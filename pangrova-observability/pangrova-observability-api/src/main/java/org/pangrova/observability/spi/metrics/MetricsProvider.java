package org.pangrova.observability.spi.metrics;

import org.pangrova.observability.api.metrics.DoubleHistogram;
import org.pangrova.observability.api.metrics.LongCounter;
import org.pangrova.observability.api.metrics.LongUpDownCounter;
import org.pangrova.observability.api.metrics.ObservableDoubleGauge;

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
 * <p>Explicit disabled implementations live in the Observability Noop module. Process
 * assembly selects the provider according to configuration.
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
     * current gauge value. It must be thread-safe and must not block collection.
     * The caller owns the returned registration and closes it before retiring
     * its observation scope. Closing a registration does not close this provider.
     * If creation fails, the implementation must release any partial registration.
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
