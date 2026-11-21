package org.pangrova.observability.noop;

import org.pangrova.common.api.annotation.Internal;

import org.pangrova.observability.api.metrics.ObservableDoubleGauge;

/**
 * No-operation {@link ObservableDoubleGauge} implementation.
 *
 * <p>Thread-safe by design as this class holds no mutable state.
 */
@Internal
public final class NoOpObservableDoubleGauge implements ObservableDoubleGauge {

    @Override
    public void close() {
        // Explicitly disabled telemetry has no callback registration to release.
    }

    static final NoOpObservableDoubleGauge INSTANCE = new NoOpObservableDoubleGauge();

    private NoOpObservableDoubleGauge() {
    }
}
