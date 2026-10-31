package org.pangrova.observability.sdk.metrics.provider.otel;

import org.pangrova.observability.api.ObservableDoubleGauge;

/**
 * Wraps OTel's observable gauge registration behind Pangrova's SPI.
 * The gauge value is pulled from a supplier at collection time.
 */
public final class OtelObservableDoubleGauge implements ObservableDoubleGauge {

    private final io.opentelemetry.api.metrics.ObservableDoubleGauge delegate;

    OtelObservableDoubleGauge(io.opentelemetry.api.metrics.ObservableDoubleGauge delegate) {
        this.delegate = delegate;
    }
}
