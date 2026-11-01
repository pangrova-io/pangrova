package org.pangrova.observability.sdk.tracing.otel;

import org.pangrova.spi.observability.Context;

/**
 * Wraps {@link io.opentelemetry.context.Context} behind Pangrova's SPI {@link Context}.
 * Prevents OTel SDK types from leaking into the public API.
 */
public final class OtelContextWrapper implements Context {

    private final io.opentelemetry.context.Context delegate;

    public OtelContextWrapper(io.opentelemetry.context.Context delegate) {
        this.delegate = delegate;
    }

    public io.opentelemetry.context.Context unwrap() {
        return delegate;
    }

    /**
     * Extracts the OTel context from an Pangrova context, falling back to current if not an OTel wrapper.
     */
    public static io.opentelemetry.context.Context unwrapOrCurrent(Context context) {
        if (context instanceof OtelContextWrapper wrapper) {
            return wrapper.unwrap();
        }
        return io.opentelemetry.context.Context.current();
    }
}
