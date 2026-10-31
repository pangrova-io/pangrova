package org.pangrova.observability.api.noop;

import org.pangrova.observability.api.LongUpDownCounter;

/**
 * No-operation {@link LongUpDownCounter} implementation.
 *
 * <p>Thread-safe by design as this class holds no mutable state.
 */
public final class NoOpLongUpDownCounter implements LongUpDownCounter {

    static final NoOpLongUpDownCounter INSTANCE = new NoOpLongUpDownCounter();

    @Override
    public void add(long value) {
        // NoOp
    }
}
