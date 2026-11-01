package org.pangrova.observability.sdk.tracing.provider.otel;

import org.pangrova.spi.observability.Scope;
import org.pangrova.spi.observability.Span;
import org.pangrova.spi.observability.StatusCode;

/**
 * Wraps {@link io.opentelemetry.api.trace.Span} behind Pangrova's SPI {@link Span}.
 * Delegates all operations to the underlying OTel span.
 */
public final class OtelSpanWrapper implements Span {

    private final io.opentelemetry.api.trace.Span delegate;

    OtelSpanWrapper(io.opentelemetry.api.trace.Span delegate) {
        this.delegate = delegate;
    }

    @Override
    public Span setAttribute(String key, String value) {
        delegate.setAttribute(key, value);
        return this;
    }

    @Override
    public Span setAttribute(String key, long value) {
        delegate.setAttribute(key, value);
        return this;
    }

    @Override
    public Span setStatus(StatusCode statusCode, String description) {
        delegate.setStatus(mapStatusCode(statusCode), description);
        return this;
    }

    @Override
    public Span recordException(Throwable exception) {
        delegate.recordException(exception);
        return this;
    }

    @Override
    public Scope makeCurrent() {
        io.opentelemetry.context.Scope otelScope = delegate.makeCurrent();
        return otelScope::close;
    }

    @Override
    public void end() {
        delegate.end();
    }

    public io.opentelemetry.api.trace.Span unwrap() {
        return delegate;
    }

    private static io.opentelemetry.api.trace.StatusCode mapStatusCode(StatusCode code) {
        return switch (code) {
            case OK -> io.opentelemetry.api.trace.StatusCode.OK;
            case ERROR -> io.opentelemetry.api.trace.StatusCode.ERROR;
            case UNSET -> io.opentelemetry.api.trace.StatusCode.UNSET;
        };
    }
}
