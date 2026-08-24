package org.pangrova.observability.tracing;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;

/**
 * Converts current trace context to hex traceId/spanId for passing to native (C++) via JNI.
 * Uses OTel SDK directly — this is an internal implementation class.
 */
public final class NativeTraceBridge {

    private static final String INVALID_TRACE_ID = "00000000000000000000000000000000";
    private static final String INVALID_SPAN_ID = "0000000000000000";

    private NativeTraceBridge() {}

    /**
     * Returns the current 32-char hex trace ID, or all-zeros if no active span.
     */
    public static String currentTraceId() {
        SpanContext ctx = Span.current().getSpanContext();
        return ctx.isValid() ? ctx.getTraceId() : INVALID_TRACE_ID;
    }

    /**
     * Returns the current 16-char hex span ID, or all-zeros if no active span.
     */
    public static String currentSpanId() {
        SpanContext ctx = Span.current().getSpanContext();
        return ctx.isValid() ? ctx.getSpanId() : INVALID_SPAN_ID;
    }
}
