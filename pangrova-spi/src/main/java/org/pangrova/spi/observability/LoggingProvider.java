package org.pangrova.spi.observability;

import org.pangrova.common.api.annotation.Stable;

/**
 * SPI for structured logging.
 *
 * <p>Provides logger instances optionally enriched with trace context.
 * Implementations bridge Pangrova's lightweight logging abstraction to a
 * concrete logging backend (e.g. SLF4J + Logback).
 *
 * <p>Explicit disabled implementations live in Observability SDK. Process
 * assembly selects the provider according to configuration.
 */
@Stable(since = "1.0")
public interface LoggingProvider {

    /**
     * Returns a logger for the given name.
     *
     * @param name the logger name, typically a fully-qualified class name
     * @return a {@link Logger} instance
     */
    Logger getLogger(String name);

    /**
     * Returns a logger for the given name, enriched with trace context
     * so that log entries can be correlated with distributed traces.
     *
     * @param name    the logger name
     * @param context the trace context to attach to log entries
     * @return a context-aware {@link Logger} instance
     */
    Logger getLogger(String name, Context context);
}
