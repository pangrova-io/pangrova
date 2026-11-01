package org.pangrova.observability.sdk.noop;

import org.pangrova.common.api.annotation.Internal;

import org.pangrova.spi.observability.Context;
import org.pangrova.spi.observability.Logger;
import org.pangrova.spi.observability.LoggingProvider;

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
