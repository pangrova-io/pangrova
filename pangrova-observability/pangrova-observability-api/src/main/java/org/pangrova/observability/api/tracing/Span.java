package org.pangrova.observability.api.tracing;

import org.pangrova.observability.api.enums.StatusCode;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Lightweight span abstraction representing a single unit of work in a trace.
 *
 * <p>Mirrors the core contract of an OpenTelemetry Span but belongs to
 * Pangrova's own SPI layer, keeping the SPI module free of external
 * tracing SDK dependencies.
 *
 * <p>All mutating methods return {@code this} to allow fluent chaining.
 */
@Experimental
public interface Span {

    /**
     * Sets a string attribute on this span.
     *
     * @param key   the attribute key
     * @param value the attribute value
     * @return this span for fluent chaining
     */
    Span setAttribute(String key, String value);

    /**
     * Sets a long attribute on this span.
     *
     * @param key   the attribute key
     * @param value the attribute value
     * @return this span for fluent chaining
     */
    Span setAttribute(String key, long value);

    /**
     * Sets the status of this span.
     *
     * @param statusCode  the status code
     * @param description a human-readable description of the status
     * @return this span for fluent chaining
     */
    Span setStatus(StatusCode statusCode, String description);

    /**
     * Records an exception as an event on this span.
     *
     * @param exception the exception to record
     * @return this span for fluent chaining
     */
    Span recordException(Throwable exception);

    /**
     * Makes this span the current span on the calling thread.
     * Returns a {@link Scope} that, when closed, restores the previously-current span.
     *
     * <p>Usage:
     * <pre>{@code
     * Span span = tracer.startSpan("work");
     * try (Scope ignored = span.makeCurrent()) {
     *     // downstream calls observe  as current
     * } finally {
     *     span.end();
     * }
     * }</pre>
     *
     * @return a scope that must be closed to deactivate this span
     */
    Scope makeCurrent();

    /**
     * Marks the end of this span's execution, recording its duration.
     */
    void end();
}
