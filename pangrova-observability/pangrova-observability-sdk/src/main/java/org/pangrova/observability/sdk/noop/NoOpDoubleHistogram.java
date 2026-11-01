package org.pangrova.observability.sdk.noop;

import org.pangrova.common.api.annotation.Internal;

import org.pangrova.spi.observability.DoubleHistogram;

/**
 * No-operation {@link DoubleHistogram} implementation.
 *
 * <p>Thread-safe by design as this class holds no mutable state.
 */
@Internal
public final class NoOpDoubleHistogram implements DoubleHistogram {

    static final NoOpDoubleHistogram INSTANCE = new NoOpDoubleHistogram();

    @Override
    public void record(double value) {
        // NoOp
    }
}
