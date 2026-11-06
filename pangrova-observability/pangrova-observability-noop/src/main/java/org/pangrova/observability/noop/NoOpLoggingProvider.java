package org.pangrova.observability.noop;

import org.pangrova.common.api.annotation.Internal;

import org.pangrova.observability.api.tracing.Context;
import org.pangrova.observability.api.logging.Logger;
import org.pangrova.observability.spi.logging.LoggingProvider;

/**
 * No-operation {@link LoggingProvider} implementation.
 *
 * <p>Returns a shared {@link NoOpLogger} singleton that discards all messages.
 * Thread-safe by design.
 */
@Internal
public class NoOpLoggingProvider implements LoggingProvider {

    private static final Logger NOOP_LOGGER = NoOpLogger.INSTANCE;

    @Override
    public Logger getLogger(String name) {
        return NOOP_LOGGER;
    }

    @Override
    public Logger getLogger(String name, Context context) {
        return NOOP_LOGGER;
    }
}
