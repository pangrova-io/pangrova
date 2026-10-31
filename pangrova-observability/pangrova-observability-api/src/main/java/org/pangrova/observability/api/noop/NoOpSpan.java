package org.pangrova.observability.api.noop;

import org.pangrova.observability.api.Span;
import org.pangrova.observability.api.StatusCode;

/**
 * No-operation {@link Span} implementation.
 *
 * <p>All methods are no-ops or return {@code this}. Thread-safe by design
 * as this class holds no mutable state.
 */
public final class NoOpSpan implements Span {

    static final NoOpSpan INSTANCE = new NoOpSpan();

    @Override
    public Span setAttribute(String key, String value) {
        return this;
    }

    @Override
    public Span setAttribute(String key, long value) {
        return this;
    }

    @Override
    public Span setStatus(StatusCode statusCode, String description) {
        return this;
    }

    @Override
    public Span recordException(Throwable exception) {
        return this;
    }

    @Override
    public void end() {
        // NoOp
    }
}
