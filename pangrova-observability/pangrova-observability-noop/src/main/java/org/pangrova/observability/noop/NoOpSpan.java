package org.pangrova.observability.noop;

import org.pangrova.common.api.annotation.Internal;

import org.pangrova.observability.api.tracing.Span;
import org.pangrova.observability.api.tracing.StatusCode;

/**
 * No-operation {@link Span} implementation.
 *
 * <p>All methods are no-ops or return {@code this}. Thread-safe by design
 * as this class holds no mutable state.
 */
@Internal
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
    @Override
    public org.pangrova.observability.api.tracing.Scope makeCurrent() {
        return org.pangrova.observability.api.tracing.Scope.NOOP;
    }
}
