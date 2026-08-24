package org.pangrova.observability.noop;

import org.pangrova.spi.observability.DoubleHistogram;

/**
 * No-operation {@link DoubleHistogram} implementation.
 *
 * <p>Thread-safe by design as this class holds no mutable state.
 */
public final class NoOpDoubleHistogram implements DoubleHistogram {

    static final NoOpDoubleHistogram INSTANCE = new NoOpDoubleHistogram();

    @Override
    public void record(double value) {
        // NoOp
    }
}
