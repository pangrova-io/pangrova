package org.pangrova.spi.observability;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Interface for extracting trace context from a carrier object.
 *
 * <p>Used by {@link TracingProvider#extract} to retrieve propagated context
 * from incoming requests (e.g. HTTP headers, gRPC metadata).
 *
 * <p>This interface has two methods and is therefore <em>not</em> a
 * {@code @FunctionalInterface}.
 *
 * @param <C> the type of the carrier object
 */
@Experimental
public interface TextMapGetter<C> {

    /**
     * Returns the value associated with the given key from the carrier,
     * or {@code null} if the key is not present.
     *
     * @param carrier the carrier to read from
     * @param key     the propagation key
     * @return the value, or {@code null}
     */
    String get(C carrier, String key);

    /**
     * Returns all keys available in the carrier.
     *
     * @param carrier the carrier to read from
     * @return an iterable of all keys
     */
    Iterable<String> keys(C carrier);
}
