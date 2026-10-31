package org.pangrova.observability.api.noop;

import org.pangrova.observability.api.Context;
import org.pangrova.observability.api.Span;
import org.pangrova.observability.api.SpanKind;
import org.pangrova.observability.api.TextMapGetter;
import org.pangrova.observability.api.TextMapSetter;
import org.pangrova.observability.api.TracingProvider;

/**
 * No-operation {@link TracingProvider} implementation.
 *
 * <p>All span operations return a shared {@link NoOpSpan} singleton.
 * Context injection and extraction are no-ops. Thread-safe by design.
 */
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
