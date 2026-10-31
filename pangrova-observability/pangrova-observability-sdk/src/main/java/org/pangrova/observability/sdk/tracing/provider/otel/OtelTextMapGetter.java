package org.pangrova.observability.sdk.tracing.provider.otel;

import org.pangrova.observability.api.TextMapGetter;

/**
 * Adapts Pangrova's {@link TextMapGetter} to OTel's {@link io.opentelemetry.context.propagation.TextMapGetter}.
 * Anti-corruption layer preventing OTel types from leaking into SPI.
 */
final class OtelTextMapGetter<C>
        implements io.opentelemetry.context.propagation.TextMapGetter<C> {

    private final TextMapGetter<C> delegate;

    OtelTextMapGetter(TextMapGetter<C> delegate) {
        this.delegate = delegate;
    }

    @Override
    public Iterable<String> keys(C carrier) {
        return delegate.keys(carrier);
    }

    @Override
    public String get(C carrier, String key) {
        return delegate.get(carrier, key);
    }
}
