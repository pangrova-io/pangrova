package org.pangrova.observability.api.noop;

import org.pangrova.observability.api.ObservableDoubleGauge;

/**
 * No-operation {@link ObservableDoubleGauge} implementation.
 *
 * <p>Thread-safe by design as this class holds no mutable state.
 */
public final class NoOpObservableDoubleGauge implements ObservableDoubleGauge {

    static final NoOpObservableDoubleGauge INSTANCE = new NoOpObservableDoubleGauge();

    private NoOpObservableDoubleGauge() {
    }
}
