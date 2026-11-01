package org.pangrova.observability.sdk.noop;

import org.pangrova.common.api.annotation.Internal;

import org.pangrova.spi.observability.LongUpDownCounter;

/**
 * No-operation {@link LongUpDownCounter} implementation.
 *
 * <p>Thread-safe by design as this class holds no mutable state.
 */
@Internal
public final class NoOpLongUpDownCounter implements LongUpDownCounter {

    static final NoOpLongUpDownCounter INSTANCE = new NoOpLongUpDownCounter();

    @Override
    public void add(long value) {
        // NoOp
    }
}
