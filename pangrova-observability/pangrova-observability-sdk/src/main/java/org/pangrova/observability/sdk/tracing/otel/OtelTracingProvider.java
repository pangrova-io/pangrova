package org.pangrova.observability.sdk.tracing.otel;

import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.TextMapPropagator;
import org.pangrova.spi.observability.Context;
import org.pangrova.spi.observability.Span;
import org.pangrova.spi.observability.SpanKind;
import org.pangrova.spi.observability.TextMapGetter;
import org.pangrova.spi.observability.TextMapSetter;
import org.pangrova.spi.observability.TracingProvider;

/**
 * OpenTelemetry-backed {@link TracingProvider} implementation.
 * Uses W3C TraceContext propagator for cross-process context propagation.
 */
public final class OtelTracingProvider implements TracingProvider {

    private final Tracer tracer;
    private final TextMapPropagator propagator;

    public OtelTracingProvider(Tracer tracer) {
        this.tracer = tracer;
        this.propagator = W3CTraceContextPropagator.getInstance();
    }

    @Override
    public Span startSpan(String name, SpanKind kind) {
        io.opentelemetry.api.trace.Span otelSpan = tracer.spanBuilder(name)
                .setSpanKind(mapSpanKind(kind))
                .startSpan();
        return new OtelSpanWrapper(otelSpan);
    }

    @Override
    public Span startSpan(String name, SpanKind kind, Context parent) {
        io.opentelemetry.context.Context otelCtx = OtelContextWrapper.unwrapOrCurrent(parent);
        io.opentelemetry.api.trace.Span otelSpan = tracer.spanBuilder(name)
                .setSpanKind(mapSpanKind(kind))
                .setParent(otelCtx)
                .startSpan();
        return new OtelSpanWrapper(otelSpan);
    }

    @Override
    public Span currentSpan() {
        return new OtelSpanWrapper(io.opentelemetry.api.trace.Span.current());
    }

    @Override
    public <C> void inject(Context context, C carrier, TextMapSetter<C> setter) {
        io.opentelemetry.context.Context otelCtx = OtelContextWrapper.unwrapOrCurrent(context);
        propagator.inject(otelCtx, carrier, setter::set);
    }

    @Override
    public <C> Context extract(Context context, C carrier, TextMapGetter<C> getter) {
        io.opentelemetry.context.Context otelCtx = OtelContextWrapper.unwrapOrCurrent(context);
        io.opentelemetry.context.Context extracted = propagator.extract(otelCtx, carrier,
                new OtelTextMapGetter<>(getter));
        return new OtelContextWrapper(extracted);
    }

    private static io.opentelemetry.api.trace.SpanKind mapSpanKind(SpanKind kind) {
        return switch (kind) {
            case SERVER -> io.opentelemetry.api.trace.SpanKind.SERVER;
            case CLIENT -> io.opentelemetry.api.trace.SpanKind.CLIENT;
            case PRODUCER -> io.opentelemetry.api.trace.SpanKind.PRODUCER;
            case CONSUMER -> io.opentelemetry.api.trace.SpanKind.CONSUMER;
            case INTERNAL -> io.opentelemetry.api.trace.SpanKind.INTERNAL;
        };
    }
}
