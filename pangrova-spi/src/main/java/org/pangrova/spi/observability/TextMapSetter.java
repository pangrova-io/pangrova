package org.pangrova.spi.observability;

import org.pangrova.spi.annotation.Experimental;

/**
 * Functional interface for injecting trace context into a carrier object.
 *
 * <p>Used by {@link TracingProvider#inject} to propagate context across
 * process boundaries (e.g. HTTP headers, gRPC metadata).
 *
 * @param <C> the type of the carrier object
 */
@Experimental
@FunctionalInterface
public interface TextMapSetter<C> {

    /**
     * Sets a key-value pair on the given carrier.
     *
     * @param carrier the carrier to inject into
     * @param key     the propagation key
     * @param value   the propagation value
     */
    void set(C carrier, String key, String value);
}
