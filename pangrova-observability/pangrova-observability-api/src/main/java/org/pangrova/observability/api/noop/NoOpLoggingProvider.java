package org.pangrova.observability.api.noop;

import org.pangrova.observability.api.Context;
import org.pangrova.observability.api.Logger;
import org.pangrova.observability.api.LoggingProvider;

/**
 * No-operation {@link LoggingProvider} implementation.
 *
 * <p>Returns a shared {@link NoOpLogger} singleton that discards all messages.
 * Thread-safe by design.
 */
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
