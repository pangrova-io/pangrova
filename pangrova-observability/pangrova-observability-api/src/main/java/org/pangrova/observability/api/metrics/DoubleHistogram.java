package org.pangrova.observability.api.metrics;

import org.pangrova.common.api.annotation.Experimental;

/**
 * A histogram metric that records {@code double} observations and
 * aggregates them into configurable buckets.
 *
 * <p>Histograms are typically used for measuring latencies or
 * value distributions.
 */
@Experimental
public interface DoubleHistogram {

    /**
     * Records a single observation.
     *
     * @param value the observed value
     */
    void record(double value);

    /**
     * Records a single observation with dimensional attributes.
     *
     * @param value      the observed value
     * @throws UnsupportedOperationException when nonempty attributes are not supported
     * @param attributes key-value pairs for dimensional classification
     */
    default void record(double value, MetricAttributes attributes) {
        if (!java.util.Objects.requireNonNull(attributes, "attributes").isEmpty()) {
            throw new UnsupportedOperationException("Metric implementation does not support dimensional attributes");
        }
        record(value);
    }
}
