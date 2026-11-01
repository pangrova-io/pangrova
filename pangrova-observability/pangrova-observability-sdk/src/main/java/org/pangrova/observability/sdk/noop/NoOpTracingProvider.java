package org.pangrova.observability.sdk.noop;

import org.pangrova.common.api.annotation.Internal;

import org.pangrova.spi.observability.Context;
import org.pangrova.spi.observability.Span;
import org.pangrova.spi.observability.SpanKind;
import org.pangrova.spi.observability.TextMapGetter;
import org.pangrova.spi.observability.TextMapSetter;
import org.pangrova.spi.observability.TracingProvider;

/**
 * No-operation {@link TracingProvider} implementation.
 *
 * <p>All span operations return a shared {@link NoOpSpan} singleton.
 * Context injection and extraction are no-ops. Thread-safe by design.
 */
@Internal
public class NoOpTracingProvider implements TracingProvider {

    private static final Span NOOP_SPAN = NoOpSpan.INSTANCE;

    @Override
    public Span startSpan(String name, SpanKind kind) {
        return NOOP_SPAN;
    }

    @Override
    public Span startSpan(String name, SpanKind kind, Context parent) {
        return NOOP_SPAN;
    }

    @Override
    public Span currentSpan() {
        return NOOP_SPAN;
    }

    @Override
    public <C> void inject(Context context, C carrier, TextMapSetter<C> setter) {
        // NoOp
    }

    @Override
    public <C> Context extract(Context context, C carrier, TextMapGetter<C> getter) {
        return context;
    }
}
