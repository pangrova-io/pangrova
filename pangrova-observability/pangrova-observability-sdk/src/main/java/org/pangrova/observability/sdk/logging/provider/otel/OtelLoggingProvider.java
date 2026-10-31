package org.pangrova.observability.sdk.logging.provider.otel;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import org.pangrova.observability.sdk.tracing.provider.otel.OtelContextWrapper;
import org.pangrova.observability.api.Context;
import org.pangrova.observability.api.Logger;
import org.pangrova.observability.api.LoggingProvider;
import org.slf4j.LoggerFactory;
import org.pangrova.observability.sdk.logging.provider.mdc.StructuredLogger;
import org.pangrova.observability.sdk.logging.provider.mdc.LogContext;

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
