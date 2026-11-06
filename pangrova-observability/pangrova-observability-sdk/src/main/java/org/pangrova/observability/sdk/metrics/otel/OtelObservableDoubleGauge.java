package org.pangrova.observability.sdk.metrics.otel;

import org.pangrova.observability.api.metrics.ObservableDoubleGauge;

/**
 * Wraps OTel's observable gauge registration behind Pangrova's SPI.
 * The gauge value is pulled from a supplier at collection time.
 */
public final class OtelObservableDoubleGauge implements ObservableDoubleGauge {

    private final io.opentelemetry.api.metrics.ObservableDoubleGauge delegate;
    private boolean closed;

    OtelObservableDoubleGauge(io.opentelemetry.api.metrics.ObservableDoubleGauge delegate) {
        this.delegate = delegate;
    }

    @Override
    public synchronized void close() {
        if (!closed) {
            delegate.close();
            closed = true;
        }
    }
}
