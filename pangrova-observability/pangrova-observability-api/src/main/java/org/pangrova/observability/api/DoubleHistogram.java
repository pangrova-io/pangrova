package org.pangrova.observability.api;

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
     * @param attributes key-value pairs for dimensional classification
     */
    default void record(double value, MetricAttributes attributes) {
        record(value);
    }
}
