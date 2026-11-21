package org.pangrova.observability.sdk.tracing.otel;

import io.opentelemetry.api.trace.Span;

/** W3C correlation IDs of the active span. OpenTelemetry supplies zero IDs outside a span. */
public final class CurrentSpanIds {
    private CurrentSpanIds() {}

    public static String currentTraceId() {
        return Span.current().getSpanContext().getTraceId();
    }

    public static String currentSpanId() {
        return Span.current().getSpanContext().getSpanId();
    }
}
