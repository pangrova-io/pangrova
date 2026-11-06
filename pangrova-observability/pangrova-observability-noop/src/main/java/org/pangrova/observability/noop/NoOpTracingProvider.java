package org.pangrova.observability.noop;

import org.pangrova.common.api.annotation.Internal;

import org.pangrova.observability.api.tracing.Context;
import org.pangrova.observability.api.tracing.Span;
import org.pangrova.observability.api.tracing.SpanKind;
import org.pangrova.observability.api.tracing.TextMapGetter;
import org.pangrova.observability.api.tracing.TextMapSetter;
import org.pangrova.observability.spi.tracing.TracingProvider;

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
