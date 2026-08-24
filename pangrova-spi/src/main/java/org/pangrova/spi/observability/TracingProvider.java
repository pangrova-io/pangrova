package org.pangrova.spi.observability;

import org.pangrova.spi.annotation.Stable;

/**
 * SPI for distributed tracing.
 *
 * <p>Provides span creation and cross-process context propagation.
 * Implementations bridge Pangrova's lightweight tracing abstraction to a
 * concrete tracing backend (e.g. OpenTelemetry SDK).
 *
 * <p>A NoOp implementation is provided by the {@code pangrova-observability}
 * module and serves as the default fallback when no real backend is
 * configured.
 */
@Stable(since = "1.0")
public interface TracingProvider {

    /**
     * Starts a new span with the given name and kind.
     *
     * @param name the span name describing the operation
     * @param kind the span kind indicating its role in the trace
     * @return a new active {@link Span}
     */
    Span startSpan(String name, SpanKind kind);

    /**
     * Starts a new span as a child of the given parent context.
     *
     * @param name   the span name describing the operation
     * @param kind   the span kind indicating its role in the trace
     * @param parent the parent context to link this span to
     * @return a new active {@link Span}
     */
    Span startSpan(String name, SpanKind kind, Context parent);

    /**
     * Returns the currently active span, or a no-op span if none is active.
     *
     * @return the current {@link Span}
     */
    Span currentSpan();

    /**
     * Injects the trace context into the given carrier for outbound propagation.
     *
     * @param context the context to propagate
     * @param carrier the carrier object to inject into
     * @param setter  the setter used to write key-value pairs into the carrier
     * @param <C>     the carrier type
     */
    <C> void inject(Context context, C carrier, TextMapSetter<C> setter);

    /**
     * Extracts trace context from the given carrier for inbound propagation.
     *
     * @param context the current context (used as fallback)
     * @param carrier the carrier object to extract from
     * @param getter  the getter used to read key-value pairs from the carrier
     * @param <C>     the carrier type
     * @return the extracted {@link Context}
     */
    <C> Context extract(Context context, C carrier, TextMapGetter<C> getter);
}
