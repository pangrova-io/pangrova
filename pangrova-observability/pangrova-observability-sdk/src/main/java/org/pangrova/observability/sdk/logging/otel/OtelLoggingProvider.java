package org.pangrova.observability.sdk.logging.otel;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import org.pangrova.observability.sdk.tracing.otel.OtelContextWrapper;
import org.pangrova.observability.api.tracing.Context;
import org.pangrova.observability.api.logging.Logger;
import org.pangrova.observability.api.logging.LoggingProvider;
import org.slf4j.LoggerFactory;
import org.pangrova.observability.logging.StructuredLogger;
import org.pangrova.observability.logging.LogContext;

/**
 * OpenTelemetry-aware {@link LoggingProvider} that returns {@link StructuredLogger} instances.
 * When a trace context is provided, injects trace_id/span_id into MDC before returning the logger.
 */
public final class OtelLoggingProvider implements LoggingProvider {

    @Override
    public Logger getLogger(String name) {
        return new StructuredLogger(LoggerFactory.getLogger(name));
    }

    @Override
    public Logger getLogger(String name, Context context) {
        enrichMdc(context);
        return new StructuredLogger(LoggerFactory.getLogger(name));
    }

    private static void enrichMdc(Context context) {
        io.opentelemetry.context.Context otelCtx = OtelContextWrapper.unwrapOrCurrent(context);
        SpanContext spanCtx = Span.fromContext(otelCtx).getSpanContext();
        if (spanCtx.isValid()) {
            LogContext.set(spanCtx.getTraceId(), spanCtx.getSpanId());
        }
    }
}
