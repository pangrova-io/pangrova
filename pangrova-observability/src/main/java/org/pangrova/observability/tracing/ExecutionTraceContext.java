package org.pangrova.observability.tracing;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.trace.IdGenerator;
import org.pangrova.spi.observability.Scope;

import java.util.Objects;
import java.util.HashMap;

/** Immutable execution correlation, independent of whether span telemetry is exported. */
public final class ExecutionTraceContext {
    private final SpanContext context;

    private ExecutionTraceContext(SpanContext context) {
        this.context = Objects.requireNonNull(context, "context");
        if (!context.isValid()) {
            throw new IllegalArgumentException("Execution trace requires valid W3C trace and span identities");
        }
    }

    /** Captures the actual parent, or starts an unsampled local root when no parent exists. */
    public static ExecutionTraceContext capture() {
        SpanContext context = Span.current().getSpanContext();
        if (!context.isValid()) {
            IdGenerator ids = IdGenerator.random();
            context = SpanContext.create(ids.generateTraceId(), ids.generateSpanId(),
                    TraceFlags.getDefault(), TraceState.getDefault());
        }
        return new ExecutionTraceContext(context);
    }

    /** Returns the captured W3C trace identity. */
    public String traceId() { return context.getTraceId(); }
    /** Returns the captured W3C parent span identity. */
    public String spanId() { return context.getSpanId(); }
    /** Returns the captured W3C trace flags as an unsigned byte value. */
    public int traceFlags() { return Byte.toUnsignedInt(context.getTraceFlags().asByte()); }

    /** Serializes the captured vendor trace state with the W3C propagator. */
    /** Encodes the captured vendor state using the standard W3C propagation contract. */
    public String traceState() {
        var carrier = new HashMap<String, String>();
        W3CTraceContextPropagator.getInstance().inject(
                Context.root().with(Span.wrap(context)), carrier, HashMap::put);
        return carrier.getOrDefault("tracestate", "");
    }

    /** Activates the captured parent without retaining any originating thread-local scope. */
    public Scope makeCurrent() {
        var scope = Span.wrap(context).makeCurrent();
        return scope::close;
    }
}
