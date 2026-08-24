package org.pangrova.spi.observability;

import org.pangrova.spi.annotation.Stable;

/**
 * An activated-span scope: the window during which a {@link Span} is the current span on the calling
 * thread. Closing the scope restores the previously-current span.
 *
 * <p>Pangrova-owned SPI type with no external tracing SDK on its surface (the tracing-backed
 * implementation wraps the SDK's own scope internally only), so the SPI module stays free of
 * tracing-SDK dependencies. Use with try-with-resources:
 *
 * <pre>{@code
 * Span span = tracer.startSpan("work");
 * try (Scope ignored = span.makeCurrent()) {
 *     // downstream calls observe `span` as current; context propagates across hops
 * } finally {
 *     span.end();
 * }
 * }</pre>
 *
 * <p>{@link #close()} does not throw, so it is safe in try-with-resources without extra handling.
 */
@Stable(since = "1.0")
public interface Scope extends AutoCloseable {

    /** A no-op scope used when tracing is disabled; closing it does nothing. */
    Scope NOOP = () -> { };

    @Override
    void close();
}
