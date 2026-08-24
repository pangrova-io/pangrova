package org.pangrova.observability.noop;

import org.pangrova.spi.observability.ObservableDoubleGauge;

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
