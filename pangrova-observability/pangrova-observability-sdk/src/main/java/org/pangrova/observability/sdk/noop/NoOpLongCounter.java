package org.pangrova.observability.sdk.noop;

import org.pangrova.common.api.annotation.Internal;

import org.pangrova.spi.observability.LongCounter;

/**
 * No-operation {@link LongCounter} implementation.
 *
 * <p>Thread-safe by design as this class holds no mutable state.
 */
@Internal
public final class NoOpLongCounter implements LongCounter {

    static final NoOpLongCounter INSTANCE = new NoOpLongCounter();

    @Override
    public void add(long value) {
        // NoOp
    }
}
