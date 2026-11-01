package org.pangrova.observability.sdk.noop;

import org.pangrova.common.api.annotation.Internal;

import org.pangrova.spi.observability.ObservableDoubleGauge;

/**
 * No-operation {@link ObservableDoubleGauge} implementation.
 *
 * <p>Thread-safe by design as this class holds no mutable state.
 */
@Internal
public final class NoOpObservableDoubleGauge implements ObservableDoubleGauge {

    static final NoOpObservableDoubleGauge INSTANCE = new NoOpObservableDoubleGauge();

    private NoOpObservableDoubleGauge() {
    }
}
