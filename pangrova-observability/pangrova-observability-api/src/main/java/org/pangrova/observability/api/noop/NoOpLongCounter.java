package org.pangrova.observability.api.noop;

import org.pangrova.observability.api.LongCounter;

/**
 * No-operation {@link LongCounter} implementation.
 *
 * <p>Thread-safe by design as this class holds no mutable state.
 */
public final class NoOpLongCounter implements LongCounter {

    static final NoOpLongCounter INSTANCE = new NoOpLongCounter();

    @Override
    public void add(long value) {
        // NoOp
    }
}
