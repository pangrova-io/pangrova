package org.pangrova.spi.observability;

import org.pangrova.spi.annotation.Experimental;

/**
 * A gauge metric that reports a {@code double} value obtained from a
 * registered callback supplier.
 *
 * <p>Unlike counters and histograms, gauges are registration-based:
 * the value is pulled from the supplier at collection time rather than
 * pushed by the application. Consequently this interface exposes no
 * active recording methods.
 */
@Experimental
public interface ObservableDoubleGauge {
}
