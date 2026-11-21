package org.pangrova.observability.api.logging;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Structured logger abstraction for Pangrova modules.
 *
 * <p>Provides a minimal logging contract that can be backed by SLF4J,
 * java.util.logging, or any other logging framework via the
 * {@link LoggingProvider} SPI.
 */
@Experimental
public interface Logger {

    /**
     * Logs an informational message.
     *
     * @param message the log message
     */
    void info(String message);

    /**
     * Logs a warning message.
     *
     * @param message the log message
     */
    void warn(String message);

    /**
     * Logs an error message with an associated throwable.
     *
     * @param message   the log message
     * @param throwable the exception to log
     */
    void error(String message, Throwable throwable);

    /**
     * Logs a debug message.
     *
     * @param message the log message
     */
    void debug(String message);
}
