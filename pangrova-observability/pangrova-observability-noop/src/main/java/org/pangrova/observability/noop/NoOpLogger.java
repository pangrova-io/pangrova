package org.pangrova.observability.noop;

import org.pangrova.common.api.annotation.Internal;

import org.pangrova.observability.api.logging.Logger;

/**
 * No-operation {@link Logger} implementation.
 *
 * <p>All log messages are silently discarded. Thread-safe by design
 * as this class holds no mutable state.
 */
@Internal
public final class NoOpLogger implements Logger {

    static final NoOpLogger INSTANCE = new NoOpLogger();

    @Override
    public void info(String message) {
        // NoOp
    }

    @Override
    public void warn(String message) {
        // NoOp
    }

    @Override
    public void error(String message, Throwable throwable) {
        // NoOp
    }

    @Override
    public void debug(String message) {
        // NoOp
    }
}
