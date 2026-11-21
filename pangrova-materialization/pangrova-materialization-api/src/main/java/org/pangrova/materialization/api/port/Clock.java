package org.pangrova.materialization.api.port;

import org.pangrova.common.api.annotation.Experimental;

import java.time.Instant;

/**
 * Time source for the materialization kernel.
 *
 * <p>All domain and use-case timestamps come from this port so freshness,
 * fencing, and GC decisions are testable without sleeping.
 */
@Experimental
public interface Clock {

    /**
     * Returns the current instant.
     *
     * @return current instant
     */
    Instant now();
}
