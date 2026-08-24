package org.pangrova.observability.logging;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import org.pangrova.observability.tracing.OtelContextWrapper;
import org.pangrova.spi.observability.Context;
import org.pangrova.spi.observability.Logger;
import org.pangrova.spi.observability.LoggingProvider;
import org.slf4j.LoggerFactory;

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
