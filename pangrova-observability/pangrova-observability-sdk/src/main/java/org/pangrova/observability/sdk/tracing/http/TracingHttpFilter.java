package org.pangrova.observability.sdk.tracing.http;

import io.opentelemetry.api.trace.SpanContext;
import org.pangrova.observability.sdk.logging.mdc.LogContext;
import org.pangrova.spi.observability.Context;
import org.pangrova.spi.observability.Span;
import org.pangrova.spi.observability.SpanKind;
import org.pangrova.spi.observability.StatusCode;
import org.pangrova.spi.observability.TextMapGetter;
import org.pangrova.spi.observability.TextMapSetter;
import org.pangrova.spi.observability.TracingProvider;
import org.pangrova.observability.sdk.tracing.otel.OtelContextWrapper;
import org.pangrova.observability.sdk.tracing.otel.OtelSpanWrapper;

/**
 * Extracts W3C traceparent/tracestate from request headers, creates a server span,
 * injects trace context into response headers, and populates MDC for log correlation.
 */
public final class TracingHttpFilter {

    /** First HTTP status code considered an error (client errors start at 4xx). */
    private static final int HTTP_ERROR_STATUS_THRESHOLD = 400;

    private final TracingProvider tracingProvider;

    public TracingHttpFilter(TracingProvider tracingProvider) {
        this.tracingProvider = tracingProvider;
    }

    /**
     * Extracts parent context from request headers, starts a server span,
     * and populates MDC with trace_id/span_id for log correlation.
     */
    public <C> Span startServerSpan(String operationName, C requestCarrier,
                                     TextMapGetter<C> getter) {
        Context parent = tracingProvider.extract(
                new OtelContextWrapper(io.opentelemetry.context.Context.current()),
                requestCarrier, getter);
        Span span = tracingProvider.startSpan(operationName, SpanKind.SERVER, parent);
        populateMdc(span);
        return span;
    }

    /**
     * Injects current trace context into response headers.
     */
    public <C> void injectResponseHeaders(Context context, C responseCarrier,
                                           TextMapSetter<C> setter) {
        tracingProvider.inject(context, responseCarrier, setter);
    }

    /**
     * Ends the span with appropriate status based on the HTTP status code,
     * and clears MDC trace context.
     */
    public void endSpan(Span span, int httpStatusCode) {
        if (httpStatusCode >= HTTP_ERROR_STATUS_THRESHOLD) {
            span.setStatus(StatusCode.ERROR, "HTTP " + httpStatusCode);
        } else {
            span.setStatus(StatusCode.OK, "");
        }
        span.end();
        LogContext.clear();
    }

    private static void populateMdc(Span span) {
        if (span instanceof OtelSpanWrapper wrapper) {
            SpanContext spanCtx = wrapper.unwrap().getSpanContext();
            if (spanCtx.isValid()) {
                LogContext.set(spanCtx.getTraceId(), spanCtx.getSpanId());
            }
        }
    }
}
